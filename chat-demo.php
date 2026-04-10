<?php
declare(strict_types=1);

use King\Autoscaling;
use King\Config;
use King\IIBIN;
use King\MCP;
use King\ObjectStore;
use King\WebSocket\Server;

require_once __DIR__ . '/vendor/autoload.php';

/**
 * =====================================================================================
 * This is how you build that with King — the enterprise way.
 * =====================================================================================
 *
 * One file.
 * One composition root.
 * One clean runtime model.
 *
 * What this file gives you
 * ------------------------
 * - WebSocket chat entrypoint
 * - binary IIBIN on the wire from frontend to backend and back
 * - separated services behind one service locator
 * - durable original documents in the King Object Store
 * - PostgreSQL for metadata, versions, extracted text and chunks
 * - pgvector for semantic retrieval
 * - retrieval responses as snippets plus document references
 * - document-view path for a frontend viewer
 * - worker lifecycle through King Autoscaling
 * - MCP-ready contracts for future remote service execution
 *
 * Why this architecture is the clean one
 * --------------------------------------
 * Great systems separate concerns aggressively:
 *
 * - Chat talks to a locator.
 * - The locator talks to services.
 * - Ingest stores originals.
 * - Parse extracts and scores text.
 * - Embedding indexes chunks.
 * - Retrieval searches semantic space.
 * - Document service serves viewer-ready document state.
 * - Worker control owns infrastructure concerns.
 *
 * That means:
 * - originals are durable
 * - retrieval is grounded
 * - chat is not coupled to storage details
 * - the frontend can show snippets and full documents
 * - any service can later move out of process without breaking the contracts
 *
 * Important transport rule
 * ------------------------
 * The frontend speaks IIBIN.
 * The backend speaks IIBIN.
 * No JSON payloads are embedded in transport frames.
 *
 * Service map
 * -----------
 * locator
 * chat-service
 * document-ingest-service
 * parse-service
 * embedding-service
 * retrieval-service
 * document-service
 * worker-control-service
 *
 * Product capabilities this backend enables
 * -----------------------------------------
 * - ask a question
 * - upload a document
 * - persist the original document durably
 * - extract and index it
 * - ask again and retrieve grounded evidence
 * - receive snippet hits
 * - open the full document in a viewer
 */

/**
 * =====================================================================================
 * 1) Global configuration
 * =====================================================================================
 */

const APP_NAME             = 'AbrechnungsNavigator';
const WS_HOST              = '0.0.0.0';
const WS_PORT              = 9501;
const DEFAULT_ROOM         = 'abrechnung';
const VECTOR_DIM           = 128;
const MAX_RETRIEVAL_HITS   = 8;

/**
 * Runtime and infrastructure configuration.
 *
 * In a real project this moves to config files or env loading.
 * In a one-file composition root, keeping it visible is a strength.
 */
$config = [
    'postgres' => [
        'dsn'      => getenv('APP_PG_DSN') ?: 'pgsql:host=127.0.0.1;port=5432;dbname=abrechnungsnavigator',
        'user'     => getenv('APP_PG_USER') ?: 'postgres',
        'password' => getenv('APP_PG_PASSWORD') ?: 'postgres',
    ],

    'king' => [
        'websocket.max_payload_size'     => 16 * 1024 * 1024,
        'websocket.handshake_timeout_ms' => 5000,
        'websocket.ping_interval_ms'     => 25000,
    ],

    /**
     * Document durability strategy.
     *
     * - local_fs is the primary store
     * - cloud_s3 is the replica / backup target
     *
     * Hetzner Object Storage is S3-compatible, so this is the right shape.
     */
    'object_store' => [
        'primary_backend'        => 'local_fs',
        'storage_root_path'      => __DIR__ . '/storage',
        'max_storage_size_bytes' => 10 * 1024 * 1024 * 1024,
        'chunk_size_kb'          => 1024,

        'backup_backend' => 'cloud_s3',
        'backup_config' => [
            'bucket'     => getenv('KING_S3_BUCKET') ?: 'abrechnungsnavigator',
            'region'     => getenv('KING_S3_REGION') ?: 'fsn1',
            'endpoint'   => getenv('KING_S3_ENDPOINT') ?: 'https://example.hetzner-objectstorage.com',
            'access_key' => getenv('KING_S3_ACCESS_KEY') ?: 'replace-me',
            'secret_key' => getenv('KING_S3_SECRET_KEY') ?: 'replace-me',
            'path_style' => false,
        ],

        'cdn_config' => [
            'enabled' => false,
        ],
    ],

    /**
     * Worker control configuration.
     *
     * The idea is simple:
     * the core stays clean and persistent,
     * the heavy lifting can move to ephemeral workers.
     */
    'autoscaling' => [
        'autoscale.provider'                         => 'hetzner',
        'autoscale.region'                           => getenv('KING_HCLOUD_REGION') ?: 'nbg1',
        'autoscale.api_endpoint'                     => getenv('KING_HCLOUD_API_ENDPOINT') ?: 'https://api.hetzner.cloud/v1',
        'autoscale.state_path'                       => __DIR__ . '/autoscaling-state.bin',
        'autoscale.server_name_prefix'               => getenv('KING_AUTOSCALE_PREFIX') ?: 'abrechnung-worker',
        'autoscale.prepared_release_url'             => getenv('KING_RELEASE_URL') ?: 'https://artifacts.example.internal/abrechnung/current.tar.gz',
        'autoscale.join_endpoint'                    => getenv('KING_JOIN_ENDPOINT') ?: 'https://core.example.internal/internal/join',
        'autoscale.min_nodes'                        => 0,
        'autoscale.max_nodes'                        => 8,
        'autoscale.max_scale_step'                   => 1,
        'autoscale.scale_up_cpu_threshold_percent'   => 70,
        'autoscale.scale_down_cpu_threshold_percent' => 25,
        'autoscale.cooldown_period_sec'              => 60,
        'autoscale.idle_node_timeout_sec'            => 300,
    ],

    /**
     * MCP peers.
     *
     * Local by default.
     * Remote once a service deserves its own process or machine.
     * The service contract stays identical.
     */
    'mcp_peers' => [
        // 'retrieval-remote' => ['host' => '127.0.0.1', 'port' => 7702],
        // 'embedding-remote' => ['host' => '127.0.0.1', 'port' => 7703],
        // 'document-remote'  => ['host' => '127.0.0.1', 'port' => 7704],
    ],
];

/**
 * =====================================================================================
 * 2) King runtime bootstrap
 * =====================================================================================
 *
 * This is where platform concerns come online:
 * transport, storage, worker lifecycle, remote peers.
 */

$kingConfig = new Config($config['king']);

/**
 * The King Object Store is the durable home for original documents.
 * This is where user uploads and crawled originals belong.
 */
ObjectStore::init($config['object_store']);

/**
 * Autoscaling owns worker lifecycle and capacity operations.
 */
Autoscaling::init($config['autoscaling']);
Autoscaling::startMonitoring();

/**
 * MCP peer pool.
 *
 * Each MCP object represents a remote peer.
 * The service locator can route calls to those peers later.
 */
$mcpPeers = [];
foreach ($config['mcp_peers'] as $peerName => $peerConfig) {
    $mcpPeers[$peerName] = new MCP(
        $peerConfig['host'],
        (int) $peerConfig['port'],
        $kingConfig
    );
}

/**
 * =====================================================================================
 * 3) PostgreSQL + pgvector bootstrap
 * =====================================================================================
 *
 * Clean responsibility split:
 * - Object Store: original file payloads
 * - PostgreSQL : metadata, versions, extracted text, chunks
 * - pgvector   : semantic similarity
 */

$pdo = new PDO(
    $config['postgres']['dsn'],
    $config['postgres']['user'],
    $config['postgres']['password'],
    [
        PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
    ]
);

/**
 * For a one-file system, schema bootstrapping here keeps the whole model visible.
 * In production this becomes migrations without changing the model.
 */
$pdo->exec('CREATE EXTENSION IF NOT EXISTS vector');

$pdo->exec(
    'CREATE TABLE IF NOT EXISTS documents (
        id UUID PRIMARY KEY,
        source TEXT NOT NULL,
        title TEXT NOT NULL,
        mime_type TEXT NOT NULL,
        object_store_key TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )'
);

$pdo->exec(
    'CREATE TABLE IF NOT EXISTS document_versions (
        id UUID PRIMARY KEY,
        document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
        sha256 TEXT NOT NULL,
        text_sha256 TEXT,
        extracted_text TEXT,
        parse_quality_score NUMERIC(10,4) NOT NULL DEFAULT 0,
        suspect BOOLEAN NOT NULL DEFAULT FALSE,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )'
);

$pdo->exec(
    'CREATE TABLE IF NOT EXISTS document_chunks (
        id UUID PRIMARY KEY,
        document_id UUID NOT NULL REFERENCES documents(id) ON DELETE CASCADE,
        version_id UUID NOT NULL REFERENCES document_versions(id) ON DELETE CASCADE,
        chunk_index INTEGER NOT NULL,
        chunk_text TEXT NOT NULL,
        embedding vector(' . VECTOR_DIM . ') NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )'
);

/**
 * =====================================================================================
 * 4) IIBIN transport model
 * =====================================================================================
 *
 * The transport is binary from end to end.
 * The frontend sends IIBIN.
 * The backend responds with IIBIN.
 *
 * The clean wire model is:
 * - one envelope
 * - typed binary payloads inside the envelope
 *
 * No JSON on the wire.
 * No text blobs pretending to be structure.
 * No format confusion.
 */

/**
 * Envelope frame types.
 *
 * Each type has exactly one typed payload schema.
 */
if (!IIBIN::isEnumDefined('FrameType')) {
    IIBIN::defineEnum('FrameType', [
        'CHAT_QUERY'      => 1,
        'CHAT_ANSWER'     => 2,
        'DOCUMENT_UPLOAD' => 3,
        'DOCUMENT_STORED' => 4,
        'RETRIEVAL_HIT'   => 5,
        'DOCUMENT_VIEW_REQUEST' => 6,
        'DOCUMENT_VIEW'   => 7,
        'ERROR'           => 8,
        'STATUS'          => 9,
    ]);
}

/**
 * The outer envelope.
 *
 * payload_bin is always another IIBIN message.
 * That gives us typed payloads without transport-layer chaos.
 */
if (!IIBIN::isSchemaDefined('Envelope')) {
    IIBIN::defineSchema('Envelope', [
        'type'        => ['type' => 'enum',   'enum' => 'FrameType', 'field_number' => 1],
        'request_id'  => ['type' => 'string', 'field_number' => 2],
        'user_id'     => ['type' => 'string', 'field_number' => 3],
        'room'        => ['type' => 'string', 'field_number' => 4],
        'payload_bin' => ['type' => 'bytes',  'field_number' => 5],
        'sent_at_ms'  => ['type' => 'int64',  'field_number' => 6],
    ]);
}

/**
 * Payload schemas.
 *
 * Each transport action gets its own schema.
 * That keeps the frontend and backend contracts strict and obvious.
 */
if (!IIBIN::isSchemaDefined('ChatQueryPayload')) {
    IIBIN::defineSchema('ChatQueryPayload', [
        'question' => ['type' => 'string', 'field_number' => 1],
    ]);
}

if (!IIBIN::isSchemaDefined('ChatAnswerPayload')) {
    IIBIN::defineSchema('ChatAnswerPayload', [
        'answer' => ['type' => 'string', 'field_number' => 1],
    ]);
}

if (!IIBIN::isSchemaDefined('DocumentUploadPayload')) {
    IIBIN::defineSchema('DocumentUploadPayload', [
        'title'     => ['type' => 'string', 'field_number' => 1],
        'file_name' => ['type' => 'string', 'field_number' => 2],
        'mime_type' => ['type' => 'string', 'field_number' => 3],
        'content'   => ['type' => 'bytes',  'field_number' => 4],
    ]);
}

if (!IIBIN::isSchemaDefined('DocumentStoredPayload')) {
    IIBIN::defineSchema('DocumentStoredPayload', [
        'document_id'      => ['type' => 'string', 'field_number' => 1],
        'version_id'       => ['type' => 'string', 'field_number' => 2],
        'title'            => ['type' => 'string', 'field_number' => 3],
        'mime_type'        => ['type' => 'string', 'field_number' => 4],
        'object_store_key' => ['type' => 'string', 'field_number' => 5],
    ]);
}

if (!IIBIN::isSchemaDefined('RetrievalHitPayload')) {
    IIBIN::defineSchema('RetrievalHitPayload', [
        'document_id'      => ['type' => 'string', 'field_number' => 1],
        'version_id'       => ['type' => 'string', 'field_number' => 2],
        'title'            => ['type' => 'string', 'field_number' => 3],
        'mime_type'        => ['type' => 'string', 'field_number' => 4],
        'snippet'          => ['type' => 'string', 'field_number' => 5],
        'chunk_index'      => ['type' => 'int32',  'field_number' => 6],
        'distance_text'    => ['type' => 'string', 'field_number' => 7],
        'object_store_key' => ['type' => 'string', 'field_number' => 8],
    ]);
}

if (!IIBIN::isSchemaDefined('DocumentViewRequestPayload')) {
    IIBIN::defineSchema('DocumentViewRequestPayload', [
        'document_id' => ['type' => 'string', 'field_number' => 1],
    ]);
}

if (!IIBIN::isSchemaDefined('DocumentViewPayload')) {
    IIBIN::defineSchema('DocumentViewPayload', [
        'document_id'      => ['type' => 'string', 'field_number' => 1],
        'version_id'       => ['type' => 'string', 'field_number' => 2],
        'title'            => ['type' => 'string', 'field_number' => 3],
        'mime_type'        => ['type' => 'string', 'field_number' => 4],
        'object_store_key' => ['type' => 'string', 'field_number' => 5],
        'extracted_text'   => ['type' => 'string', 'field_number' => 6],
        'quality_text'     => ['type' => 'string', 'field_number' => 7],
    ]);
}

if (!IIBIN::isSchemaDefined('ErrorPayload')) {
    IIBIN::defineSchema('ErrorPayload', [
        'message' => ['type' => 'string', 'field_number' => 1],
    ]);
}

if (!IIBIN::isSchemaDefined('StatusPayload')) {
    IIBIN::defineSchema('StatusPayload', [
        'message' => ['type' => 'string', 'field_number' => 1],
    ]);
}

/**
 * =====================================================================================
 * 5) Small low-level primitives
 * =====================================================================================
 *
 * A composition root still needs a few primitives.
 * Keeping them here keeps the rest of the file elegant.
 */

$uuid = static function (): string {
    $data = random_bytes(16);
    $data[6] = chr((ord($data[6]) & 0x0f) | 0x40);
    $data[8] = chr((ord($data[8]) & 0x3f) | 0x80);
    return vsprintf('%s%s-%s-%s-%s-%s%s%s', str_split(bin2hex($data), 4));
};

$nowMs = static function (): int {
    return (int) floor(microtime(true) * 1000);
};

$sanitizeFileName = static function (string $name): string {
    return preg_replace('/[^a-zA-Z0-9._-]/', '_', $name) ?: 'document.bin';
};

$vectorLiteral = static function (array $vector): string {
    return '[' . implode(',', array_map(
        static fn (float $v): string => rtrim(rtrim(number_format($v, 8, '.', ''), '0'), '.'),
        $vector
    )) . ']';
};

$encodeEnvelope = static function (
    int $type,
    string $requestId,
    string $userId,
    string $room,
    string $payloadBinary
) use ($nowMs): string {
    return IIBIN::encode('Envelope', [
        'type'        => $type,
        'request_id'  => $requestId,
        'user_id'     => $userId,
        'room'        => $room,
        'payload_bin' => $payloadBinary,
        'sent_at_ms'  => $nowMs(),
    ]);
};

$sendTypedFrame = static function (
    Server $server,
    string $connectionId,
    int $frameType,
    string $requestId,
    string $userId,
    string $room,
    string $payloadSchema,
    array $payloadData
) use ($encodeEnvelope): void {
    $payloadBinary = IIBIN::encode($payloadSchema, $payloadData);
    $envelope = $encodeEnvelope($frameType, $requestId, $userId, $room, $payloadBinary);
    $server->sendBinary($connectionId, $envelope);
};

/**
 * =====================================================================================
 * 6) Services
 * =====================================================================================
 *
 * This is the heart of the design.
 * Each service owns a single job.
 * Each service can later move out of process.
 * The locator is the only place that needs to know how resolution works.
 */

$services = [];

/**
 * -------------------------------------------------------------------------------------
 * document-ingest-service
 * -------------------------------------------------------------------------------------
 *
 * This service is where documents enter the platform.
 *
 * Why it exists
 * -------------
 * The original document must be durably stored first.
 * Everything else depends on that truth existing.
 *
 * What it owns
 * ------------
 * - writes original file bytes into the King Object Store
 * - creates the document row
 * - creates the first version row
 *
 * Why this shape is the right one
 * -------------------------------
 * Parsing can fail.
 * Indexing can fail.
 * Retrieval can fail.
 * The original must remain durable and addressable regardless.
 */
$services['document-ingest-service'] = [
    'store' => static function (array $input) use ($pdo, $uuid, $sanitizeFileName): array {
        $documentId = $uuid();
        $versionId  = $uuid();
        $title      = trim((string) ($input['title'] ?? 'Untitled document'));
        $source     = trim((string) ($input['source'] ?? 'chat-upload'));
        $mimeType   = trim((string) ($input['mime_type'] ?? 'application/octet-stream'));
        $binary     = (string) ($input['binary'] ?? '');
        $fileName   = $sanitizeFileName((string) ($input['file_name'] ?? 'document.bin'));

        $sha256     = hash('sha256', $binary);
        $objectKey  = 'documents/' . gmdate('Y/m/d') . '/' . $documentId . '/' . $fileName;

        ObjectStore::put($objectKey, $binary, [
            'content_type'     => $mimeType,
            'object_type'      => 'document',
            'cache_policy'     => 'etag',
            'integrity_sha256' => $sha256,
        ]);

        $stmt = $pdo->prepare(
            'INSERT INTO documents (id, source, title, mime_type, object_store_key)
             VALUES (:id, :source, :title, :mime_type, :object_store_key)'
        );
        $stmt->execute([
            ':id'               => $documentId,
            ':source'           => $source,
            ':title'            => $title,
            ':mime_type'        => $mimeType,
            ':object_store_key' => $objectKey,
        ]);

        $stmt = $pdo->prepare(
            'INSERT INTO document_versions (id, document_id, sha256)
             VALUES (:id, :document_id, :sha256)'
        );
        $stmt->execute([
            ':id'          => $versionId,
            ':document_id' => $documentId,
            ':sha256'      => $sha256,
        ]);

        return [
            'document_id'      => $documentId,
            'version_id'       => $versionId,
            'title'            => $title,
            'mime_type'        => $mimeType,
            'object_store_key' => $objectKey,
        ];
    },
];

/**
 * -------------------------------------------------------------------------------------
 * parse-service
 * -------------------------------------------------------------------------------------
 *
 * This service turns a stored document into extracted text plus chunkable content.
 *
 * Why it exists
 * -------------
 * Retrieval quality begins with parse quality.
 * Parsing and quality scoring must not be smeared into chat or retrieval.
 *
 * What it owns
 * ------------
 * - fetches the original from the Object Store
 * - extracts text
 * - scores extraction quality
 * - splits text into chunks
 * - writes extracted text and quality into the version row
 *
 * Why great systems do this
 * -------------------------
 * Retrieval wants good chunks.
 * Chat wants good evidence.
 * The parse layer is what gives both of them a foundation.
 */
$services['parse-service'] = [
    'run' => static function (array $input) use ($pdo): array {
        $documentId = (string) $input['document_id'];
        $versionId  = (string) $input['version_id'];

        $stmt = $pdo->prepare(
            'SELECT d.title, d.mime_type, d.object_store_key
             FROM documents d
             WHERE d.id = :document_id'
        );
        $stmt->execute([':document_id' => $documentId]);
        $doc = $stmt->fetch();

        if (!$doc) {
            throw new RuntimeException('Document not found.');
        }

        $binary = ObjectStore::get((string) $doc['object_store_key']);
        if ($binary === false) {
            throw new RuntimeException('Document payload not found in Object Store.');
        }

        /**
         * For a real rollout this is where:
         * - pdftotext
         * - XML parsers
         * - KE parsers
         * - ZIP readers
         * plug in.
         *
         * The contract does not change.
         *
         * This single-file demo keeps text/plain and application/json directly readable.
         */
        $mimeType = (string) $doc['mime_type'];
        $text = '';

        if ($mimeType === 'text/plain' || $mimeType === 'application/json') {
            $text = (string) $binary;
        }

        $text = trim($text);

        /**
         * Parse quality.
         *
         * No OCR.
         * Bad extraction stays visible instead of being hidden.
         */
        $length = mb_strlen($text);
        preg_match_all('/[\p{L}\p{N}]/u', $text, $matches);
        $useful = count($matches[0]);
        $ratio  = $length > 0 ? ($useful / $length) : 0.0;
        $score  = min(1.0, ($length / 5000.0)) * 0.5 + min(1.0, $ratio) * 0.5;
        $suspect = ($length < 200) || ($ratio < 0.25);

        $stmt = $pdo->prepare(
            'UPDATE document_versions
             SET extracted_text = :extracted_text,
                 text_sha256 = :text_sha256,
                 parse_quality_score = :score,
                 suspect = :suspect
             WHERE id = :version_id'
        );
        $stmt->execute([
            ':extracted_text' => $text,
            ':text_sha256'    => hash('sha256', $text),
            ':score'          => $score,
            ':suspect'        => $suspect,
            ':version_id'     => $versionId,
        ]);

        /**
         * Chunking strategy.
         *
         * Fixed-size with overlap is simple, deterministic and strong enough
         * for a serious first version.
         */
        $chunks = [];
        $targetChars = 1200;
        $overlap = 240;
        $offset = 0;

        while ($offset < mb_strlen($text)) {
            $chunk = trim(mb_substr($text, $offset, $targetChars));
            if ($chunk !== '') {
                $chunks[] = $chunk;
            }

            if (($offset + $targetChars) >= mb_strlen($text)) {
                break;
            }

            $offset += max(1, $targetChars - $overlap);
        }

        return [
            'document_id' => $documentId,
            'version_id'  => $versionId,
            'title'       => (string) $doc['title'],
            'chunks'      => $chunks,
            'quality'     => [
                'score'   => $score,
                'suspect' => $suspect,
                'length'  => $length,
                'ratio'   => $ratio,
            ],
        ];
    },
];

/**
 * -------------------------------------------------------------------------------------
 * embedding-service
 * -------------------------------------------------------------------------------------
 *
 * This service owns vector indexing.
 *
 * Why it exists
 * -------------
 * Embeddings are compute, batching and model territory.
 * That is not chat logic and not retrieval logic.
 *
 * What it owns
 * ------------
 * - receives chunks
 * - turns chunks into vectors
 * - writes vectors to pgvector
 *
 * Why this is the clean cut
 * -------------------------
 * The retrieval layer should never care how embeddings were created.
 * It should only query them.
 */
$services['embedding-service'] = [
    'index' => static function (array $input) use ($pdo, $uuid, $vectorLiteral): array {
        $documentId = (string) $input['document_id'];
        $versionId  = (string) $input['version_id'];
        $chunks     = (array)  $input['chunks'];

        $stmt = $pdo->prepare(
            'INSERT INTO document_chunks (id, document_id, version_id, chunk_index, chunk_text, embedding)
             VALUES (:id, :document_id, :version_id, :chunk_index, :chunk_text, :embedding)'
        );

        /**
         * The vector projection below is the seam where your real local model
         * or worker execution plugs in.
         *
         * The service contract is already correct.
         * The computation engine can be swapped in without breaking the rest.
         */
        foreach ($chunks as $index => $chunkText) {
            $hash = hash('sha256', (string) $chunkText, true);
            $vector = [];

            for ($i = 0; $i < VECTOR_DIM; $i++) {
                $byte = ord($hash[$i % strlen($hash)]);
                $vector[] = $byte / 255.0;
            }

            $stmt->execute([
                ':id'          => $uuid(),
                ':document_id' => $documentId,
                ':version_id'  => $versionId,
                ':chunk_index' => $index,
                ':chunk_text'  => (string) $chunkText,
                ':embedding'   => $vectorLiteral($vector),
            ]);
        }

        return [
            'document_id' => $documentId,
            'version_id'  => $versionId,
            'indexed'     => count($chunks),
        ];
    },
];

/**
 * -------------------------------------------------------------------------------------
 * retrieval-service
 * -------------------------------------------------------------------------------------
 *
 * This service owns semantic retrieval.
 *
 * Why it exists
 * -------------
 * Retrieval is a first-class problem:
 * ranking, grounding, snippet quality and reference assembly belong together.
 *
 * What it owns
 * ------------
 * - embeds the query into the same vector space
 * - runs pgvector similarity
 * - returns ranked snippet hits with document references
 *
 * Why top systems separate this
 * -----------------------------
 * Chat should ask for evidence.
 * Retrieval should produce evidence.
 * Storage should store.
 * That is the clean line.
 */
$services['retrieval-service'] = [
    'search' => static function (array $input) use ($pdo, $vectorLiteral): array {
        $query = trim((string) ($input['query'] ?? ''));
        $limit = max(1, (int) ($input['limit'] ?? MAX_RETRIEVAL_HITS));

        $hash = hash('sha256', $query, true);
        $vector = [];

        for ($i = 0; $i < VECTOR_DIM; $i++) {
            $byte = ord($hash[$i % strlen($hash)]);
            $vector[] = $byte / 255.0;
        }

        $stmt = $pdo->prepare(
            'SELECT
                 dc.document_id,
                 dc.version_id,
                 dc.chunk_index,
                 dc.chunk_text,
                 d.title,
                 d.mime_type,
                 d.object_store_key,
                 (dc.embedding <-> :query_embedding::vector) AS distance
             FROM document_chunks dc
             JOIN documents d ON d.id = dc.document_id
             ORDER BY dc.embedding <-> :query_embedding::vector
             LIMIT ' . $limit
        );

        $stmt->execute([
            ':query_embedding' => $vectorLiteral($vector),
        ]);

        $rows = $stmt->fetchAll();
        $hits = [];

        foreach ($rows as $row) {
            $hits[] = [
                'document_id'      => (string) $row['document_id'],
                'version_id'       => (string) $row['version_id'],
                'title'            => (string) $row['title'],
                'mime_type'        => (string) $row['mime_type'],
                'snippet'          => (string) $row['chunk_text'],
                'chunk_index'      => (int)    $row['chunk_index'],
                'distance_text'    => (string) number_format((float) $row['distance'], 6, '.', ''),
                'object_store_key' => (string) $row['object_store_key'],
            ];
        }

        return [
            'query' => $query,
            'hits'  => $hits,
        ];
    },
];

/**
 * -------------------------------------------------------------------------------------
 * document-service
 * -------------------------------------------------------------------------------------
 *
 * This service owns full document views.
 *
 * Why it exists
 * -------------
 * Snippets are not enough.
 * A serious frontend needs to open the actual document and render a proper viewer.
 *
 * What it owns
 * ------------
 * - fetches document metadata
 * - fetches latest extracted text
 * - prepares a frontend-ready document view contract
 *
 * Why this is the right split
 * ---------------------------
 * Retrieval answers "what is relevant?"
 * Document service answers "show me the actual thing."
 */
$services['document-service'] = [
    'view' => static function (array $input) use ($pdo): array {
        $documentId = (string) $input['document_id'];

        $stmt = $pdo->prepare(
            'SELECT
                 d.id,
                 d.title,
                 d.mime_type,
                 d.object_store_key,
                 dv.id AS version_id,
                 dv.extracted_text,
                 dv.parse_quality_score,
                 dv.suspect
             FROM documents d
             LEFT JOIN LATERAL (
                 SELECT *
                 FROM document_versions
                 WHERE document_id = d.id
                 ORDER BY created_at DESC
                 LIMIT 1
             ) dv ON TRUE
             WHERE d.id = :document_id'
        );
        $stmt->execute([':document_id' => $documentId]);
        $row = $stmt->fetch();

        if (!$row) {
            throw new RuntimeException('Document not found.');
        }

        return [
            'document_id'      => (string) $row['id'],
            'version_id'       => (string) ($row['version_id'] ?? ''),
            'title'            => (string) $row['title'],
            'mime_type'        => (string) $row['mime_type'],
            'object_store_key' => (string) $row['object_store_key'],
            'extracted_text'   => (string) ($row['extracted_text'] ?? ''),
            'quality_text'     => 'score=' . number_format((float) ($row['parse_quality_score'] ?? 0), 4, '.', '')
                                . ';suspect=' . (((bool) ($row['suspect'] ?? false)) ? 'true' : 'false'),
        ];
    },
];

/**
 * -------------------------------------------------------------------------------------
 * worker-control-service
 * -------------------------------------------------------------------------------------
 *
 * This service owns infrastructure-facing worker concerns.
 *
 * Why it exists
 * -------------
 * The application should ask for capacity.
 * It should not know how infrastructure APIs work.
 *
 * What it owns
 * ------------
 * - status inspection
 * - scale-up requests
 * - future worker readiness / drain / retirement flows
 *
 * Why this is the elite split
 * ---------------------------
 * Domain services stay domain-focused.
 * Infrastructure stays behind one contract.
 */
$services['worker-control-service'] = [
    'status' => static function (): array {
        return [
            'autoscaling_status'  => Autoscaling::getStatus(),
            'autoscaling_metrics' => Autoscaling::getMetrics(),
            'nodes'               => Autoscaling::getNodes(),
        ];
    },

    'ensure-capacity' => static function (array $input): array {
        $needed = max(0, (int) ($input['nodes'] ?? 1));
        if ($needed > 0) {
            Autoscaling::scaleUp($needed);
        }

        return [
            'requested_nodes' => $needed,
            'status'          => Autoscaling::getStatus(),
            'nodes'           => Autoscaling::getNodes(),
        ];
    },
];

/**
 * -------------------------------------------------------------------------------------
 * chat-service
 * -------------------------------------------------------------------------------------
 *
 * This service owns user-facing answer assembly.
 *
 * Why it exists
 * -------------
 * Chat is an orchestrator of evidence, not a dumping ground for storage logic.
 *
 * What it owns
 * ------------
 * - accepts user questions
 * - asks retrieval for ranked evidence
 * - assembles grounded answer text
 *
 * Why the strongest systems do this
 * ---------------------------------
 * You can change:
 * - answer style
 * - LLM provider
 * - prompt strategy
 * without changing indexing, storage or retrieval contracts.
 */
$services['chat-service'] = [
    'answer' => static function (array $input) use (&$services): array {
        $question = trim((string) ($input['question'] ?? ''));
        $userId   = trim((string) ($input['user_id'] ?? 'anonymous'));

        $retrieval = $services['retrieval-service']['search']([
            'query' => $question,
            'limit' => MAX_RETRIEVAL_HITS,
        ]);

        $hits = $retrieval['hits'];

        $topTitles = array_values(array_unique(array_map(
            static fn (array $hit): string => $hit['title'],
            $hits
        )));

        /**
         * This is the answer seam.
         * A real LLM slot can replace this line without moving any boundaries.
         */
        $answerText =
            "Grounded answer for {$userId}. " .
            "The system found " . count($hits) . " relevant knowledge fragments. " .
            "Top source documents: " . implode(', ', array_slice($topTitles, 0, 3)) . ".";

        return [
            'answer' => $answerText,
            'hits'   => $hits,
        ];
    },
];

/**
 * =====================================================================================
 * 7) Service locator
 * =====================================================================================
 *
 * This is the front door for all service invocations.
 *
 * Why it exists
 * -------------
 * The caller must not care whether a service is local or remote.
 * The caller asks for a capability.
 * The locator resolves execution.
 *
 * This is the exact place where a local service can later become:
 * - a remote MCP peer
 * - a separately scaled process
 * - a discovered service instance
 *
 * without changing the caller contract.
 */

$locator = static function (string $service, string $method, array $payload = []) use (&$services, $mcpPeers): mixed {
    /**
     * Remote override map.
     *
     * Uncomment any line when a service moves out of process.
     * The rest of the code stays untouched.
     */
    $remoteMap = [
        // 'retrieval-service' => 'retrieval-remote',
        // 'embedding-service' => 'embedding-remote',
        // 'document-service'  => 'document-remote',
    ];

    if (isset($remoteMap[$service], $mcpPeers[$remoteMap[$service]])) {
        $peer = $mcpPeers[$remoteMap[$service]];

        /**
         * MCP payloads are binary-safe strings.
         * The transport between internal services can stay IIBIN as well.
         */
        $servicePayload = IIBIN::encode('StatusPayload', [
            'message' => base64_encode(serialize($payload)),
        ]);

        return $peer->request($service, $method, $servicePayload);
    }

    if (!isset($services[$service])) {
        throw new RuntimeException("Unknown service: {$service}");
    }

    if (!isset($services[$service][$method]) || !is_callable($services[$service][$method])) {
        throw new RuntimeException("Unknown method {$method} on service {$service}");
    }

    return $services[$service][$method]($payload);
};

/**
 * =====================================================================================
 * 8) WebSocket entrypoint
 * =====================================================================================
 *
 * This is the external door.
 * The frontend connects here and speaks pure IIBIN.
 *
 * Supported frame flows
 * ---------------------
 * CHAT_QUERY
 *   payload: ChatQueryPayload
 *
 * DOCUMENT_UPLOAD
 *   payload: DocumentUploadPayload
 *
 * DOCUMENT_VIEW_REQUEST
 *   payload: DocumentViewRequestPayload
 */

$server = new Server(WS_HOST, WS_PORT, $kingConfig);

/**
 * Simple room registry.
 * This keeps fanout and future room-specific behavior straightforward.
 */
$rooms = [];

while (true) {
    $peer = $server->accept();
    $info = $peer->getInfo();
    $connectionId = (string) ($info['connection_id'] ?? '');

    if ($connectionId === '') {
        continue;
    }

    $rooms[DEFAULT_ROOM][$connectionId] = true;

    $sendTypedFrame(
        $server,
        $connectionId,
        9,
        'boot',
        'system',
        DEFAULT_ROOM,
        'StatusPayload',
        [
            'message' => 'Connection established.',
        ]
    );

    while (true) {
        /**
         * The current documented receive path is procedural.
         * The application remains procedural.
         * The platform remains King-based.
         */
        $binaryEnvelope = king_client_websocket_receive($peer, 1000);

        if ($binaryEnvelope === false) {
            unset($rooms[DEFAULT_ROOM][$connectionId]);
            if (($rooms[DEFAULT_ROOM] ?? []) === []) {
                unset($rooms[DEFAULT_ROOM]);
            }
            break;
        }

        try {
            $envelope = IIBIN::decode('Envelope', $binaryEnvelope);
            if (!is_array($envelope)) {
                throw new RuntimeException('Envelope decode failed.');
            }

            $type       = (int)    ($envelope['type'] ?? 0);
            $requestId  = (string) ($envelope['request_id'] ?? 'req-' . bin2hex(random_bytes(6)));
            $userId     = (string) ($envelope['user_id'] ?? 'anonymous');
            $room       = (string) ($envelope['room'] ?? DEFAULT_ROOM);
            $payloadBin = (string) ($envelope['payload_bin'] ?? '');

            /**
             * ----------------------------------------------------------------
             * CHAT_QUERY
             * ----------------------------------------------------------------
             *
             * Flow:
             * frontend -> chat-service -> retrieval-service -> response frames
             *
             * Response:
             * - one CHAT_ANSWER frame
             * - multiple RETRIEVAL_HIT frames
             */
            if ($type === 1) {
                $payload = IIBIN::decode('ChatQueryPayload', $payloadBin);
                if (!is_array($payload)) {
                    throw new RuntimeException('Chat query payload decode failed.');
                }

                $result = $locator('chat-service', 'answer', [
                    'question' => (string) ($payload['question'] ?? ''),
                    'user_id'  => $userId,
                ]);

                $sendTypedFrame(
                    $server,
                    $connectionId,
                    2,
                    $requestId,
                    $userId,
                    $room,
                    'ChatAnswerPayload',
                    [
                        'answer' => (string) $result['answer'],
                    ]
                );

                foreach ((array) $result['hits'] as $hit) {
                    $sendTypedFrame(
                        $server,
                        $connectionId,
                        5,
                        $requestId,
                        $userId,
                        $room,
                        'RetrievalHitPayload',
                        [
                            'document_id'      => (string) $hit['document_id'],
                            'version_id'       => (string) $hit['version_id'],
                            'title'            => (string) $hit['title'],
                            'mime_type'        => (string) $hit['mime_type'],
                            'snippet'          => (string) $hit['snippet'],
                            'chunk_index'      => (int)    $hit['chunk_index'],
                            'distance_text'    => (string) $hit['distance_text'],
                            'object_store_key' => (string) $hit['object_store_key'],
                        ]
                    );
                }

                continue;
            }

            /**
             * ----------------------------------------------------------------
             * DOCUMENT_UPLOAD
             * ----------------------------------------------------------------
             *
             * Flow:
             * frontend -> document-ingest-service -> parse-service -> embedding-service
             *
             * Response:
             * - one DOCUMENT_STORED frame
             * - one STATUS frame confirming indexing
             *
             * Why this is the right product move
             * ----------------------------------
             * A user can enrich the system directly from the chat surface.
             * The file becomes durable, searchable and retrievable in one pass.
             */
            if ($type === 3) {
                $payload = IIBIN::decode('DocumentUploadPayload', $payloadBin);
                if (!is_array($payload)) {
                    throw new RuntimeException('Document upload payload decode failed.');
                }

                $stored = $locator('document-ingest-service', 'store', [
                    'source'    => 'chat-upload',
                    'title'     => (string) ($payload['title'] ?? 'Untitled document'),
                    'file_name' => (string) ($payload['file_name'] ?? 'document.bin'),
                    'mime_type' => (string) ($payload['mime_type'] ?? 'application/octet-stream'),
                    'binary'    => (string) ($payload['content'] ?? ''),
                ]);

                $parsed = $locator('parse-service', 'run', [
                    'document_id' => (string) $stored['document_id'],
                    'version_id'  => (string) $stored['version_id'],
                ]);

                $indexed = $locator('embedding-service', 'index', [
                    'document_id' => (string) $stored['document_id'],
                    'version_id'  => (string) $stored['version_id'],
                    'chunks'      => (array)  $parsed['chunks'],
                ]);

                $sendTypedFrame(
                    $server,
                    $connectionId,
                    4,
                    $requestId,
                    $userId,
                    $room,
                    'DocumentStoredPayload',
                    [
                        'document_id'      => (string) $stored['document_id'],
                        'version_id'       => (string) $stored['version_id'],
                        'title'            => (string) $stored['title'],
                        'mime_type'        => (string) $stored['mime_type'],
                        'object_store_key' => (string) $stored['object_store_key'],
                    ]
                );

                $sendTypedFrame(
                    $server,
                    $connectionId,
                    9,
                    $requestId,
                    $userId,
                    $room,
                    'StatusPayload',
                    [
                        'message' => 'Document stored, parsed and indexed. Chunks indexed: ' . (string) $indexed['indexed'],
                    ]
                );

                continue;
            }

            /**
             * ----------------------------------------------------------------
             * DOCUMENT_VIEW_REQUEST
             * ----------------------------------------------------------------
             *
             * Flow:
             * frontend -> document-service -> DOCUMENT_VIEW frame
             *
             * This is what enables a proper document viewer in the frontend.
             */
            if ($type === 6) {
                $payload = IIBIN::decode('DocumentViewRequestPayload', $payloadBin);
                if (!is_array($payload)) {
                    throw new RuntimeException('Document view request decode failed.');
                }

                $view = $locator('document-service', 'view', [
                    'document_id' => (string) ($payload['document_id'] ?? ''),
                ]);

                $sendTypedFrame(
                    $server,
                    $connectionId,
                    7,
                    $requestId,
                    $userId,
                    $room,
                    'DocumentViewPayload',
                    [
                        'document_id'      => (string) $view['document_id'],
                        'version_id'       => (string) $view['version_id'],
                        'title'            => (string) $view['title'],
                        'mime_type'        => (string) $view['mime_type'],
                        'object_store_key' => (string) $view['object_store_key'],
                        'extracted_text'   => (string) $view['extracted_text'],
                        'quality_text'     => (string) $view['quality_text'],
                    ]
                );

                continue;
            }

            throw new RuntimeException('Unknown frame type.');
        } catch (Throwable $e) {
            $sendTypedFrame(
                $server,
                $connectionId,
                8,
                'error',
                'system',
                DEFAULT_ROOM,
                'ErrorPayload',
                [
                    'message' => $e->getMessage(),
                ]
            );
        }
    }
}

/**
 * =====================================================================================
 * What still needs to be extended or hardened in King for this to become first-class
 * =====================================================================================
 *
 * 1) A documented OO receive path for accepted WebSocket peers
 *    ---------------------------------------------------------
 *    The server accept path is OO, but the receive path in this composition still
 *    uses the procedural receive function. A fully symmetrical OO receive API would
 *    make the transport layer cleaner.
 *
 * 2) A first-class MCP server-side host surface
 *    ------------------------------------------
 *    The client-side MCP peer story is clear. For this architecture, an equally
 *    polished documented MCP server hosting surface would let retrieval, embedding
 *    and document services move out of process without custom glue.
 *
 * 3) Typed IIBIN support for repeated / nested payloads in docs and examples
 *    -----------------------------------------------------------------------
 *    The transport above avoids JSON completely and stays elegant by sending
 *    multiple typed frames. Stronger repeated/nested message examples would make
 *    richer frontend result sets even more natural.
 *
 * 4) First-class pgvector / SQL bridge is outside King today
 *    -------------------------------------------------------
 *    That is acceptable, but if King ever grows a stronger database or vector
 *    integration surface, systems like this become even cleaner.
 *
 * 5) Stronger service discovery examples for MCP-backed application services
 *    -----------------------------------------------------------------------
 *    The locator pattern is correct. The next level is a polished discovery and
 *    resolution story for multiple retrieval / embedding / document service nodes.
 *
 * 6) A more explicit file / stream ingest pattern in OO docs
 *    -------------------------------------------------------
 *    Document-heavy systems benefit from highly visible best-practice examples for:
 *    - Object Store originals
 *    - extracted artifacts
 *    - streamed uploads
 *    - viewer delivery
 *
 * 7) Higher-level orchestration examples for multi-service RAG systems
 *    ----------------------------------------------------------------
 *    King already has the right building blocks. What would help most is a canonical
 *    “chat + ingest + parse + embed + retrieve + worker lifecycle” reference flow.
 */
