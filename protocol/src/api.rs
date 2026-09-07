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
    pub const API_VERSIONS: i16 = 18;
    pub const CREATE_TOPICS: i16 = 19;
    pub const DELETE_TOPICS: i16 = 20;
    pub const INIT_PRODUCER_ID: i16 = 22;
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
    UnknownLeaderEpoch = 45, // 4.5: FENCED_LEADER_EPOCH=74? 顺序按 Kafka：45=UnknownLeaderEpoch? 见下注
    FencedLeaderEpoch = 74,
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
    UnknownServer = 83,
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
    ]
}
