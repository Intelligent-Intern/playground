<?php
declare(strict_types=1);

use King\Autoscaling;
use King\Config;
use King\IIBIN;
use King\MCP;
use King\ObjectStore;
use King\WebSocket\Server;

require_once __DIR__ . '/vendor/autoload.php';

const APP_NAME = 'AbrechnungsNavigator';
const WS_HOST = '0.0.0.0';
const WS_PORT = 9501;
const DEFAULT_ROOM = 'abrechnung';
const VECTOR_DIM = 128;
const MAX_RETRIEVAL_HITS = 8;

$config = [
    'postgres' => [
        'dsn' => getenv('APP_PG_DSN') ?: 'pgsql:host=127.0.0.1;port=5432;dbname=abrechnungsnavigator',
        'user' => getenv('APP_PG_USER') ?: 'postgres',
        'password' => getenv('APP_PG_PASSWORD') ?: 'postgres',
    ],
    'king' => [
        'websocket.max_payload_size' => 16 * 1024 * 1024,
        'websocket.handshake_timeout_ms' => 5000,
        'websocket.ping_interval_ms' => 25000,
    ],
    'object_store' => [
        'primary_backend' => 'local_fs',
        'storage_root_path' => __DIR__ . '/storage',
        'max_storage_size_bytes' => 10 * 1024 * 1024 * 1024,
        'chunk_size_kb' => 1024,
        'backup_backend' => 'cloud_s3',
        'backup_config' => [
            'bucket' => getenv('KING_S3_BUCKET') ?: 'abrechnungsnavigator',
            'region' => getenv('KING_S3_REGION') ?: 'fsn1',
            'endpoint' => getenv('KING_S3_ENDPOINT') ?: 'https://example.hetzner-objectstorage.com',
            'access_key' => getenv('KING_S3_ACCESS_KEY') ?: 'replace-me',
            'secret_key' => getenv('KING_S3_SECRET_KEY') ?: 'replace-me',
            'path_style' => false,
        ],
        'cdn_config' => [
            'enabled' => false,
        ],
    ],
    'autoscaling' => [
        'autoscale.provider' => 'hetzner',
        'autoscale.region' => getenv('KING_HCLOUD_REGION') ?: 'nbg1',
        'autoscale.api_endpoint' => getenv('KING_HCLOUD_API_ENDPOINT') ?: 'https://api.hetzner.cloud/v1',
        'autoscale.state_path' => __DIR__ . '/autoscaling-state.bin',
        'autoscale.server_name_prefix' => getenv('KING_AUTOSCALE_PREFIX') ?: 'abrechnung-worker',
        'autoscale.prepared_release_url' => getenv('KING_RELEASE_URL')
            ?: 'https://artifacts.example.internal/abrechnung/current.tar.gz',
        'autoscale.join_endpoint' => getenv('KING_JOIN_ENDPOINT')
            ?: 'https://core.example.internal/internal/join',
        'autoscale.min_nodes' => 0,
        'autoscale.max_nodes' => 8,
        'autoscale.max_scale_step' => 1,
        'autoscale.scale_up_cpu_threshold_percent' => 70,
        'autoscale.scale_down_cpu_threshold_percent' => 25,
        'autoscale.cooldown_period_sec' => 60,
        'autoscale.idle_node_timeout_sec' => 300,
    ],
    'mcp_peers' => [
        // 'retrieval-remote' => ['host' => '127.0.0.1', 'port' => 7702],
        // 'embedding-remote' => ['host' => '127.0.0.1', 'port' => 7703],
        // 'document-remote' => ['host' => '127.0.0.1', 'port' => 7704],
    ],
];

$kingConfig = new Config($config['king']);
ObjectStore::init($config['object_store']);
Autoscaling::init($config['autoscaling']);
Autoscaling::startMonitoring();

$mcpPeers = [];
foreach ($config['mcp_peers'] as $peerName => $peerConfig) {
    $mcpPeers[$peerName] = new MCP($peerConfig['host'], (int) $peerConfig['port'], $kingConfig);
}

$pdo = new PDO(
    $config['postgres']['dsn'],
    $config['postgres']['user'],
    $config['postgres']['password'],
    [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC]
);

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

if (!IIBIN::isEnumDefined('FrameType')) {
    IIBIN::defineEnum('FrameType', [
        'CHAT_QUERY' => 1,
        'CHAT_ANSWER' => 2,
        'DOCUMENT_UPLOAD' => 3,
        'DOCUMENT_STORED' => 4,
        'RETRIEVAL_HIT' => 5,
        'DOCUMENT_VIEW_REQUEST' => 6,
        'DOCUMENT_VIEW' => 7,
        'ERROR' => 8,
        'STATUS' => 9,
    ]);
}

if (!IIBIN::isSchemaDefined('Envelope')) {
    IIBIN::defineSchema('Envelope', [
        'type' => ['type' => 'enum', 'enum' => 'FrameType', 'field_number' => 1],
        'request_id' => ['type' => 'string', 'field_number' => 2],
        'user_id' => ['type' => 'string', 'field_number' => 3],
        'room' => ['type' => 'string', 'field_number' => 4],
        'payload_bin' => ['type' => 'bytes', 'field_number' => 5],
        'sent_at_ms' => ['type' => 'int64', 'field_number' => 6],
    ]);
}

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
        'title' => ['type' => 'string', 'field_number' => 1],
        'file_name' => ['type' => 'string', 'field_number' => 2],
        'mime_type' => ['type' => 'string', 'field_number' => 3],
        'content' => ['type' => 'bytes', 'field_number' => 4],
    ]);
}

if (!IIBIN::isSchemaDefined('DocumentStoredPayload')) {
    IIBIN::defineSchema('DocumentStoredPayload', [
        'document_id' => ['type' => 'string', 'field_number' => 1],
        'version_id' => ['type' => 'string', 'field_number' => 2],
        'title' => ['type' => 'string', 'field_number' => 3],
        'mime_type' => ['type' => 'string', 'field_number' => 4],
        'object_store_key' => ['type' => 'string', 'field_number' => 5],
    ]);
}

if (!IIBIN::isSchemaDefined('RetrievalHitPayload')) {
    IIBIN::defineSchema('RetrievalHitPayload', [
        'document_id' => ['type' => 'string', 'field_number' => 1],
        'version_id' => ['type' => 'string', 'field_number' => 2],
        'title' => ['type' => 'string', 'field_number' => 3],
        'mime_type' => ['type' => 'string', 'field_number' => 4],
        'snippet' => ['type' => 'string', 'field_number' => 5],
        'chunk_index' => ['type' => 'int32', 'field_number' => 6],
        'distance_text' => ['type' => 'string', 'field_number' => 7],
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
        'document_id' => ['type' => 'string', 'field_number' => 1],
        'version_id' => ['type' => 'string', 'field_number' => 2],
        'title' => ['type' => 'string', 'field_number' => 3],
        'mime_type' => ['type' => 'string', 'field_number' => 4],
        'object_store_key' => ['type' => 'string', 'field_number' => 5],
        'extracted_text' => ['type' => 'string', 'field_number' => 6],
        'quality_text' => ['type' => 'string', 'field_number' => 7],
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
        'type' => $type,
        'request_id' => $requestId,
        'user_id' => $userId,
        'room' => $room,
        'payload_bin' => $payloadBinary,
        'sent_at_ms' => $nowMs(),
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
    $server->sendBinary($connectionId, $encodeEnvelope($frameType, $requestId, $userId, $room, $payloadBinary));
};

$services = [];

$services['document-ingest-service'] = [
    'store' => static function (array $input) use ($pdo, $uuid, $sanitizeFileName): array {
        $documentId = $uuid();
        $versionId = $uuid();
        $title = trim((string) ($input['title'] ?? 'Untitled document'));
        $source = trim((string) ($input['source'] ?? 'chat-upload'));
        $mimeType = trim((string) ($input['mime_type'] ?? 'application/octet-stream'));
        $binary = (string) ($input['binary'] ?? '');
        $fileName = $sanitizeFileName((string) ($input['file_name'] ?? 'document.bin'));
        $sha256 = hash('sha256', $binary);
        $objectKey = 'documents/' . gmdate('Y/m/d') . '/' . $documentId . '/' . $fileName;

        ObjectStore::put($objectKey, $binary, [
            'content_type' => $mimeType,
            'object_type' => 'document',
            'cache_policy' => 'etag',
            'integrity_sha256' => $sha256,
        ]);

        $stmt = $pdo->prepare(
            'INSERT INTO documents (id, source, title, mime_type, object_store_key)
             VALUES (:id, :source, :title, :mime_type, :object_store_key)'
        );
        $stmt->execute([
            ':id' => $documentId,
            ':source' => $source,
            ':title' => $title,
            ':mime_type' => $mimeType,
            ':object_store_key' => $objectKey,
        ]);

        $stmt = $pdo->prepare(
            'INSERT INTO document_versions (id, document_id, sha256)
             VALUES (:id, :document_id, :sha256)'
        );
        $stmt->execute([
            ':id' => $versionId,
            ':document_id' => $documentId,
            ':sha256' => $sha256,
        ]);

        return [
            'document_id' => $documentId,
            'version_id' => $versionId,
            'title' => $title,
            'mime_type' => $mimeType,
            'object_store_key' => $objectKey,
        ];
    },
];

$services['parse-service'] = [
    'run' => static function (array $input) use ($pdo): array {
        $documentId = (string) $input['document_id'];
        $versionId = (string) $input['version_id'];

        $stmt = $pdo->prepare(
            'SELECT d.title, d.mime_type, d.object_store_key FROM documents d WHERE d.id = :document_id'
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

        $mimeType = (string) $doc['mime_type'];
        $text = '';

        if ($mimeType === 'text/plain' || $mimeType === 'application/json') {
            $text = (string) $binary;
        }

        $text = trim($text);
        $length = mb_strlen($text);
        preg_match_all('/[\p{L}\p{N}]/u', $text, $matches);
        $useful = count($matches[0]);
        $ratio = $length > 0 ? ($useful / $length) : 0.0;
        $score = min(1.0, ($length / 5000.0)) * 0.5 + min(1.0, $ratio) * 0.5;
        $suspect = ($length < 200) || ($ratio < 0.25);

        $stmt = $pdo->prepare(
            'UPDATE document_versions
             SET extracted_text = :extracted_text, text_sha256 = :text_sha256,
                 parse_quality_score = :score, suspect = :suspect
             WHERE id = :version_id'
        );
        $stmt->execute([
            ':extracted_text' => $text,
            ':text_sha256' => hash('sha256', $text),
            ':score' => $score,
            ':suspect' => $suspect,
            ':version_id' => $versionId,
        ]);

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
            'version_id' => $versionId,
            'title' => (string) $doc['title'],
            'chunks' => $chunks,
            'quality' => [
                'score' => $score,
                'suspect' => $suspect,
                'length' => $length,
                'ratio' => $ratio,
            ],
        ];
    },
];

$services['embedding-service'] = [
    'index' => static function (array $input) use ($pdo, $uuid, $vectorLiteral): array {
        $documentId = (string) $input['document_id'];
        $versionId = (string) $input['version_id'];
        $chunks = (array) $input['chunks'];

        $stmt = $pdo->prepare(
            'INSERT INTO document_chunks (id, document_id, version_id, chunk_index, chunk_text, embedding)
             VALUES (:id, :document_id, :version_id, :chunk_index, :chunk_text, :embedding)'
        );

        foreach ($chunks as $index => $chunkText) {
            $hash = hash('sha256', (string) $chunkText, true);
            $vector = [];

            for ($i = 0; $i < VECTOR_DIM; $i++) {
                $byte = ord($hash[$i % strlen($hash)]);
                $vector[] = $byte / 255.0;
            }

            $stmt->execute([
                ':id' => $uuid(),
                ':document_id' => $documentId,
                ':version_id' => $versionId,
                ':chunk_index' => $index,
                ':chunk_text' => (string) $chunkText,
                ':embedding' => $vectorLiteral($vector),
            ]);
        }

        return [
            'document_id' => $documentId,
            'version_id' => $versionId,
            'indexed' => count($chunks),
        ];
    },
];

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
            'SELECT dc.document_id, dc.version_id, dc.chunk_index, dc.chunk_text, d.title, d.mime_type,
                    d.object_store_key, (dc.embedding <-> :query_embedding::vector) AS distance
             FROM document_chunks dc
             JOIN documents d ON d.id = dc.document_id
             ORDER BY dc.embedding <-> :query_embedding::vector
             LIMIT ' . $limit
        );
        $stmt->execute([':query_embedding' => $vectorLiteral($vector)]);
        $rows = $stmt->fetchAll();

        $hits = [];
        foreach ($rows as $row) {
            $hits[] = [
                'document_id' => (string) $row['document_id'],
                'version_id' => (string) $row['version_id'],
                'title' => (string) $row['title'],
                'mime_type' => (string) $row['mime_type'],
                'snippet' => (string) $row['chunk_text'],
                'chunk_index' => (int) $row['chunk_index'],
                'distance_text' => (string) number_format((float) $row['distance'], 6, '.', ''),
                'object_store_key' => (string) $row['object_store_key'],
            ];
        }

        return [
            'query' => $query,
            'hits' => $hits,
        ];
    },
];

$services['document-service'] = [
    'view' => static function (array $input) use ($pdo): array {
        $documentId = (string) $input['document_id'];

        $stmt = $pdo->prepare(
            'SELECT d.id, d.title, d.mime_type, d.object_store_key, dv.id AS version_id, dv.extracted_text,
                    dv.parse_quality_score, dv.suspect
             FROM documents d
             LEFT JOIN LATERAL (
                 SELECT * FROM document_versions
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
            'document_id' => (string) $row['id'],
            'version_id' => (string) ($row['version_id'] ?? ''),
            'title' => (string) $row['title'],
            'mime_type' => (string) $row['mime_type'],
            'object_store_key' => (string) $row['object_store_key'],
            'extracted_text' => (string) ($row['extracted_text'] ?? ''),
            'quality_text' => 'score=' . number_format((float) ($row['parse_quality_score'] ?? 0), 4, '.', '')
                . ';suspect=' . (((bool) ($row['suspect'] ?? false)) ? 'true' : 'false'),
        ];
    },
];

$services['worker-control-service'] = [
    'status' => static function (): array {
        return [
            'autoscaling_status' => Autoscaling::getStatus(),
            'autoscaling_metrics' => Autoscaling::getMetrics(),
            'nodes' => Autoscaling::getNodes(),
        ];
    },
    'ensure-capacity' => static function (array $input): array {
        $needed = max(0, (int) ($input['nodes'] ?? 1));
        if ($needed > 0) {
            Autoscaling::scaleUp($needed);
        }
        return [
            'requested_nodes' => $needed,
            'status' => Autoscaling::getStatus(),
            'nodes' => Autoscaling::getNodes(),
        ];
    },
];

$services['chat-service'] = [
    'answer' => static function (array $input) use (&$services): array {
        $question = trim((string) ($input['question'] ?? ''));
        $userId = trim((string) ($input['user_id'] ?? 'anonymous'));

        $retrieval = $services['retrieval-service']['search']([
            'query' => $question,
            'limit' => MAX_RETRIEVAL_HITS,
        ]);

        $hits = $retrieval['hits'];
        $topTitles = array_values(array_unique(array_map(
            static fn (array $hit): string => $hit['title'],
            $hits
        )));

        $answerText = 'Grounded answer for ' . $userId . '. The system found ' . count($hits)
            . ' relevant knowledge fragments. Top source documents: '
            . implode(', ', array_slice($topTitles, 0, 3)) . '.';

        return [
            'answer' => $answerText,
            'hits' => $hits,
        ];
    },
];

$locator = static function (string $service, string $method, array $payload = []) use (&$services, $mcpPeers): mixed {
    $remoteMap = [
        // 'retrieval-service' => 'retrieval-remote',
        // 'embedding-service' => 'embedding-remote',
        // 'document-service' => 'document-remote',
    ];

    if (isset($remoteMap[$service], $mcpPeers[$remoteMap[$service]])) {
        $peer = $mcpPeers[$remoteMap[$service]];
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

$server = new Server(WS_HOST, WS_PORT, $kingConfig);
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
        ['message' => 'Connection established.']
    );

    while (true) {
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

            $type = (int) ($envelope['type'] ?? 0);
            $requestId = (string) ($envelope['request_id'] ?? 'req-' . bin2hex(random_bytes(6)));
            $userId = (string) ($envelope['user_id'] ?? 'anonymous');
            $room = (string) ($envelope['room'] ?? DEFAULT_ROOM);
            $payloadBin = (string) ($envelope['payload_bin'] ?? '');

            if ($type === 1) {
                $payload = IIBIN::decode('ChatQueryPayload', $payloadBin);
                if (!is_array($payload)) {
                    throw new RuntimeException('Chat query payload decode failed.');
                }

                $result = $locator('chat-service', 'answer', [
                    'question' => (string) ($payload['question'] ?? ''),
                    'user_id' => $userId,
                ]);

                $sendTypedFrame(
                    $server,
                    $connectionId,
                    2,
                    $requestId,
                    $userId,
                    $room,
                    'ChatAnswerPayload',
                    ['answer' => (string) $result['answer']]
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
                            'document_id' => (string) $hit['document_id'],
                            'version_id' => (string) $hit['version_id'],
                            'title' => (string) $hit['title'],
                            'mime_type' => (string) $hit['mime_type'],
                            'snippet' => (string) $hit['snippet'],
                            'chunk_index' => (int) $hit['chunk_index'],
                            'distance_text' => (string) $hit['distance_text'],
                            'object_store_key' => (string) $hit['object_store_key'],
                        ]
                    );
                }

                continue;
            }

            if ($type === 3) {
                $payload = IIBIN::decode('DocumentUploadPayload', $payloadBin);
                if (!is_array($payload)) {
                    throw new RuntimeException('Document upload payload decode failed.');
                }

                $stored = $locator('document-ingest-service', 'store', [
                    'source' => 'chat-upload',
                    'title' => (string) ($payload['title'] ?? 'Untitled document'),
                    'file_name' => (string) ($payload['file_name'] ?? 'document.bin'),
                    'mime_type' => (string) ($payload['mime_type'] ?? 'application/octet-stream'),
                    'binary' => (string) ($payload['content'] ?? ''),
                ]);

                $parsed = $locator('parse-service', 'run', [
                    'document_id' => (string) $stored['document_id'],
                    'version_id' => (string) $stored['version_id'],
                ]);

                $indexed = $locator('embedding-service', 'index', [
                    'document_id' => (string) $stored['document_id'],
                    'version_id' => (string) $stored['version_id'],
                    'chunks' => (array) $parsed['chunks'],
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
                        'document_id' => (string) $stored['document_id'],
                        'version_id' => (string) $stored['version_id'],
                        'title' => (string) $stored['title'],
                        'mime_type' => (string) $stored['mime_type'],
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
                    ['message' => 'Document stored, parsed and indexed. Chunks indexed: ' . (string) $indexed['indexed']]
                );

                continue;
            }

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
                        'document_id' => (string) $view['document_id'],
                        'version_id' => (string) $view['version_id'],
                        'title' => (string) $view['title'],
                        'mime_type' => (string) $view['mime_type'],
                        'object_store_key' => (string) $view['object_store_key'],
                        'extracted_text' => (string) $view['extracted_text'],
                        'quality_text' => (string) $view['quality_text'],
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
                ['message' => $e->getMessage()]
            );
        }
    }
}
