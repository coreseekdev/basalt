//! API key 与错误码常量（apache/kafka 4.5 协议）。

pub mod key {
    pub const PRODUCE: i16 = 0;
    pub const FETCH: i16 = 1;
    pub const LIST_OFFSETS: i16 = 2;
    pub const METADATA: i16 = 3;
    pub const OFFSET_COMMIT: i16 = 8;
    pub const OFFSET_FETCH: i16 = 9;
    pub const FIND_COORDINATOR: i16 = 10;
    pub const JOIN_GROUP: i16 = 11;
    pub const HEARTBEAT: i16 = 12;
    pub const LEAVE_GROUP: i16 = 13;
    pub const SYNC_GROUP: i16 = 14;
    pub const DESCRIBE_GROUPS: i16 = 15;
    pub const LIST_GROUPS: i16 = 16;
    pub const SASL_HANDSHAKE: i16 = 17;
    pub const SASL_AUTHENTICATE: i16 = 36;
    pub const API_VERSIONS: i16 = 18;
    pub const CREATE_TOPICS: i16 = 19;
    pub const DELETE_TOPICS: i16 = 20;
    pub const INIT_PRODUCER_ID: i16 = 22;
    pub const ADD_PARTITIONS_TO_TXN: i16 = 24;
    pub const ADD_OFFSETS_TO_TXN: i16 = 25;
    pub const END_TXN: i16 = 26;
    pub const WRITE_TXN_MARKERS: i16 = 27;
    pub const TXN_OFFSET_COMMIT: i16 = 28;
    pub const DESCRIBE_TRANSACTIONS: i16 = 65;
    pub const LIST_TRANSACTIONS: i16 = 66;
    pub const OFFSET_FOR_LEADER_EPOCH: i16 = 23;
    pub const DESCRIBE_CLUSTER: i16 = 60;
    pub const DESCRIBE_CONFIGS: i16 = 32;
    pub const ALTER_CONFIGS: i16 = 33;
    pub const DELETE_RECORDS: i16 = 21;
    pub const CONSUMER_GROUP_HEARTBEAT: i16 = 68;
    pub const CONSUMER_GROUP_DESCRIBE: i16 = 69;
}

/// 错误码（Kafka ErrorCodes）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ErrorCode {
    None = 0,
    OffsetOutOfRange = 1,
    CorruptMessage = 2,
    UnknownTopicOrPartition = 3,
    InvalidFetchSize = 4,
    LeaderNotAvailable = 5,
    NotLeaderOrFollower = 6,
    RequestTimedOut = 7,
    BrokerNotAvailable = 8,
    ReplicaNotAvailable = 9,
    MessageTooLarge = 10,
    StaleControllerEpoch = 11,
    OffsetMetadataTooLarge = 12,
    NetworkException = 13,
    CoordinatorLoadInProgress = 14,
    CoordinatorNotAvailable = 15,
    NotCoordinator = 16,
    InvalidTopicException = 17,
    RecordListTooLarge = 18,
    NotEnoughReplicas = 19,
    NotEnoughReplicasAfterAppend = 20,
    InvalidRequiredAcks = 21,
    IllegalGeneration = 22,
    InconsistentGroupProtocol = 23,
    InvalidGroupId = 24,
    UnknownMemberId = 25,
    InvalidSessionTimeout = 26,
    RebalanceInProgress = 27,
    InvalidCommitOffsetSize = 28,
    TopicAuthorizationFailed = 29,
    GroupAuthorizationFailed = 30,
    ClusterAuthorizationFailed = 31,
    InvalidTimestamp = 32,
    UnsupportedSaslMechanism = 33,
    IllegalSaslState = 34,
    UnsupportedVersion = 35,
    TopicAlreadyExists = 36,
    InvalidPartitions = 37,
    InvalidReplicationFactor = 38,
    InvalidReplicaAssignment = 39,
    InvalidConfig = 40,
    NotController = 41,
    InvalidRequest = 42,
    UnsupportedForMessageFormat = 43,
    PolicyViolation = 44,
    OutOfOrderSequence = 45, // Kafka 官方：45=OUT_OF_ORDER_SEQUENCE（此前误标 UnknownLeaderEpoch 且与 74 撞号）
    FencedLeaderEpoch = 74,
    UnknownLeaderEpoch = 75,
    UnknownTopicId = 100,
    DuplicateSequenceNumber = 46,
    InvalidProducerEpoch = 47,
    InvalidProducerIdMapping = 48,
    InvalidTxnState = 49,
    InvalidProducerId = 50,
    InvalidOffset = 51,
    TransactionalIdAuthorizationFailed = 53,
    FetchSessionIdNotFound = 70,
    IneligibleReplica = 71,
    NewLeaderElected = 72,
    OffsetMovedToTieredStorage = 73,
    // 83 官方语义 = ELIGIBLE_LEADERS_NOT_AVAILABLE（可重试；Errors.java
    // 核对）——此前误名 UnknownServer（官方 UNKNOWN_SERVER = -1），事务面
    // 曾误用作兜底给客户端错误的重试语义（review P1 实证后改兜底为 15）
    EligibleLeadersNotAvailable = 83,
    // 82 = FENCED_MEMBER_EPOCH（KIP-848：member-epoch 落后的心跳，客户端
    // 重新加入语义；ADR-19 块 b 消费组协议用）
    FencedMemberEpoch = 82,
    // 69 = GROUP_ID_NOT_FOUND（KIP-848 ConsumerGroupDescribe：组不存在；
    // Errors.java 核对，不可重试——ADR-19 块 b describe 面）
    GroupIdNotFound = 69,
    // 112 = UNSUPPORTED_ASSIGNOR（KIP-848 ServerAssignor 协商：请求的分配
    // 器不受支持；kerr/Errors.java 双源核对 112 非 57，不可重试——
    // ADR-20 §3 uniform/range 协商面）
    UnsupportedAssignor = 112,
    // 81 = GROUP_MAX_SIZE_REACHED（KIP-848 组容量上限；Errors.java:364 双源
    // 核对 81 非 68——不可重试。研究对照行动清单 P2）
    GroupMaxSizeReached = 81,
    // 58 = SASL_AUTHENTICATION_FAILED（SASL 面认证失败：proof 不匹配/未知
    // 用户；可重试性由客户端决定——Kafka broker 回该错误后关闭连接）
    SaslAuthenticationFailed = 58,
}

impl From<ErrorCode> for i16 {
    fn from(c: ErrorCode) -> i16 {
        c as i16
    }
}

/// ApiVersions 响应里宣告的「本 broker 支持的 API 版本区间」。
/// min/max 都是闭区间；未列出 = 不支持。
/// 纪律：只宣告 dispatch 真正实现了的 API（conn.rs 的 match 白名单），
/// 宣告未实现的 API 会让客户端把请求发进来然后吃 UNSUPPORTED_VERSION。
pub fn supported_versions() -> &'static [(i16, i16, i16)] {
    &[
        (key::PRODUCE, 3, 13),
        (key::FETCH, 4, 18),
        (key::LIST_OFFSETS, 1, 11),
        (key::METADATA, 0, 13),
        (key::OFFSET_COMMIT, 0, 9),
        (key::OFFSET_FETCH, 0, 9),
        (key::FIND_COORDINATOR, 0, 6),
        (key::JOIN_GROUP, 0, 9),
        (key::HEARTBEAT, 0, 4),
        (key::LEAVE_GROUP, 0, 5),
        (key::SYNC_GROUP, 0, 5),
        (key::API_VERSIONS, 0, 5),
        (key::CREATE_TOPICS, 0, 7),
        (key::DELETE_TOPICS, 0, 6),
        (key::DELETE_RECORDS, 0, 2),
        (key::INIT_PRODUCER_ID, 0, 5),
        // 事务面（ADR-18 §8）：只宣告客户端形态（24 的 v4+ 是 broker 互信
        // 批量形状，不对客户端宣告；27 WriteTxnMarkers 内部 RPC 不宣告）
        (key::ADD_PARTITIONS_TO_TXN, 0, 3),
        (key::ADD_OFFSETS_TO_TXN, 0, 3),
        (key::END_TXN, 0, 3),
        (key::TXN_OFFSET_COMMIT, 0, 3),
        (key::DESCRIBE_TRANSACTIONS, 0, 0),
        (key::LIST_TRANSACTIONS, 0, 0),
        (key::DESCRIBE_GROUPS, 0, 5),
        (key::LIST_GROUPS, 0, 4),
        (key::OFFSET_FOR_LEADER_EPOCH, 0, 5),
        // SASL 面（T-S1）：无鉴权模式下也宣告——客户端仅在配置了 SASL 时才走
        // 17/36；authenticate 宣告 0-2（v2 flexible，客户端取公共最大）
        (key::SASL_HANDSHAKE, 0, 1),
        (key::SASL_AUTHENTICATE, 0, 2),
        // 管理面对齐（T-S4，有限兼容）：DescribeConfigs schema 已删 v0，
        // 基线 v1；DescribeCluster 全区间（v1 EndpointType / v2 IsFenced）
        (key::DESCRIBE_CLUSTER, 0, 2),
        (key::DESCRIBE_CONFIGS, 1, 4),
        // KIP-848 新消费组协议（ADR-19 块 b/c）：heartbeat 宣告 0-1——
        // franz-go v1.21 的 should848 硬性 supportsKIP848v1()（只认 broker
        // 宣告 max≥1，v0 宣告 = 客户端静默回退 classic 路径）。v1 差异：
        // SubscribedTopicRegex（服务端不支持正则订阅 → 拒收 INVALID_REQUEST）
        // + KIP-1082 客户端自生成 member id（服务端按原样注册，已支持）。
        // describe 保持 v0（消费路径不依赖）。
        (key::CONSUMER_GROUP_HEARTBEAT, 0, 1),
        (key::CONSUMER_GROUP_DESCRIBE, 0, 0),
    ]
}
