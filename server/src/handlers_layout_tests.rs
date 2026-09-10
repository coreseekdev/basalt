// ===================== 协议 handler 布局回归（永久机制） =====================
// 来源：五轮 review 探针（缺陷⑩⑪/FindCoordinator/管理面修复的字节级锁定）。
// 方法：请求按 registry plan@version 编码+解码（复现 dispatch 解码产物形状），
// 调真实 handler，响应按 plan@version 编码后**独立解码**（codec::decode 内
// r.finish() 对尾随/缺失字节报错 → 字节级布局证明），逐字段断言。
// 注意：探针驱动禁止阻塞式自旋（noop-waker 会让单线程 runtime 的后台任务
// 饿死）——一律在 async 测试内 await 让出。
#[cfg(test)]
pub mod handlers_layout_tests {

    use crate::meta::{MetaCmd, Route, RoutingTable};
    use crate::handlers::Ctx;
    use crate::handlers_groups;
    use basalt_coordinator::{CommittedOffset, GroupCmd, JoinSpec, SyncSpec};
    use basalt_protocol::codec;
    use basalt_protocol::frame;
    use basalt_protocol::registry::Registry;
    use basalt_protocol::value::{s, Struct, Value};
    use bytes::{BufMut, Bytes, BytesMut};
    use std::sync::Arc;

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!(
            "basalt-probe5-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// 构造带路由 + 真实 MetaService（应答 Lookup）+ 真实 GroupManager 的 Ctx。
    async fn make_ctx(topics: &[(&str, i32)]) -> Ctx {
        let dir = tmpdir("groups");
        let group_tx = basalt_coordinator::GroupManager::spawn(&dir);
        let cfg = crate::config::Config {
            node_id: 0,
            host: "localhost".into(),
            port: 9092,
            data_dir: dir.display().to_string(),
            num_partitions: 1,
            default_rf: 1,
            segment_max_bytes: 1 << 30,
            log_level: "warn".into(),
            nodes: vec![],
            min_isr: 1,
            isr_lag_ms: 500,
        };
        let pool: Arc<basalt_storage::pool::BufferPool> = Arc::new(basalt_storage::pool::BufferPool::new());
        let (meta_tx, routes_rx) = crate::meta::MetaService::spawn(cfg, None, None, pool.clone());
        // 填充集群态：t0..tN 各 1 分区，leader=0 —— Lookup 由此应答 found
        let mut state = basalt_metadata::cluster::ClusterState::default();
        state.brokers.insert(0, basalt_metadata::cluster::BrokerInfo { node_id: 0, host: "localhost".into(), port: 9092 });
        for (name, _) in topics {
            state.assignments.push(basalt_metadata::cluster::ReplicaAssignment {
                topic: name.to_string(),
                partition: 0,
                replicas: vec![0],
                leader: 0,
                epoch: 1,
            });
        }
        // 集群态进 MetaService（原探针漏发此条 → Lookup 恒空，delete_topics 误报红）
        let _ = meta_tx.send(MetaCmd::ApplyCluster(Box::new(state))).await;
        // await 让出使 MetaService 任务得到轮询
        // （单线程 runtime：阻塞式自旋会让后台任务饿死——原探针死锁根因）
        for _ in 0..20 {
            if meta_tx.send(MetaCmd::Lookup { names: Some(vec!["__probe__".into()]), allow_create: false, reply: tokio::sync::oneshot::channel().0 }).await.is_err() {
                break;
            }
        }
        // 本地路由表：供 offset_fetch 全集展开（与 ApplyCluster 生成的路由同形）
        let mut rt = RoutingTable::default();
        for (name, parts) in topics {
            for p in 0..*parts {
                let (tx, _rx) = tokio::sync::mpsc::channel::<crate::partition::PartitionCmd>(1);
                rt.by_name.insert((name.to_string(), p), Route { tx, leader: 0, epoch: 0 });
            }
        }
        let (_tw, rx2) = tokio::sync::watch::channel(rt);
        Ctx {
            node_id: 0,
            all_brokers: vec![],
            host: "localhost".into(),
            port: 9092,
            meta_tx,
            group_tx,
            routes_rx: rx2,
            brokers_cache: std::sync::Mutex::new(None),
            pool,
        }
    }

    /// 请求 Struct 经 plan@V 编码再解码 —— 与 dispatch 对真实客户端字节的解码产物同形。
    fn as_struct_owned(v: Value) -> Struct {
        match v {
            Value::Struct(s) => s,
            o => panic!("not struct: {o:?}"),
        }
    }

    fn req_at(api_key: i16, v: i16, st: &Struct) -> Struct {
        let reg = Registry::global();
        let e = reg.api(api_key).unwrap();
        let flex = frame::is_flexible(v, e.flexible_from);
        let mut buf = BytesMut::new();
        codec::encode_struct_fields(&e.request.fields, v, flex, st, &mut buf).unwrap();
        let b = buf.freeze();
        codec::decode(&e.request.fields, v, flex, &b).unwrap()
    }

    fn resp_bytes(api_key: i16, v: i16, resp: &Value) -> Vec<u8> {
        let reg = Registry::global();
        let e = reg.api(api_key).unwrap();
        let flex = frame::is_flexible(v, e.flexible_from);
        let st = match resp {
            Value::Struct(s) => s,
            _ => unreachable!(),
        };
        let mut out = BytesMut::new();
        codec::encode_struct_fields(&e.response.fields, v, flex, st, &mut out).unwrap();
        out.to_vec()
    }

    /// 独立解码响应字节；r.finish() 保证无尾随/缺失字节（字节级布局证明）。
    fn resp_decode(api_key: i16, v: i16, bytes: &[u8]) -> Struct {
        let reg = Registry::global();
        let e = reg.api(api_key).unwrap();
        let flex = frame::is_flexible(v, e.flexible_from);
        codec::decode(&e.response.fields, v, flex, &Bytes::copy_from_slice(bytes)).unwrap()
    }

    // ---- 断言辅助 ----
    fn as_struct(v: &Value) -> &Struct {
        match v {
            Value::Struct(s) => s,
            o => panic!("not struct: {o:?}"),
        }
    }
    fn arr(st: &Struct, k: &str) -> Vec<Value> {
        match st.get(k) {
            Some(Value::Array(a)) => a.clone(),
            o => panic!("field {k} not an array: {o:?}"),
        }
    }
    fn fld<'a>(st: &'a Struct, k: &str) -> &'a Value {
        st.get(k).unwrap_or_else(|| panic!("missing field {k}"))
    }
    fn sfield<'a>(v: &'a Value, k: &str) -> &'a Value {
        as_struct(v).get(k).unwrap_or_else(|| panic!("missing {k} in {v:?}"))
    }
    fn missing(st: &Struct, k: &str) -> bool {
        st.get(k).is_none()
    }

    #[tokio::test]
    async fn probe_find_coordinator_layouts() {
        let k = basalt_protocol::api::key::FIND_COORDINATOR;
        let ctx = make_ctx(&[]).await;
    
        // v0：仅 ErrorCode/NodeId/Host/Port（无 Throttle/ErrorMessage/Coordinators）
        let st = s([("Key", Value::str("g1"))]);
        let r = fc(&ctx, 0, st).await;
        assert_eq!(fld(&r, "ErrorCode").as_i16(), 0);
        assert_eq!(fld(&r, "NodeId").as_i32(), 0);
        assert_eq!(fld(&r, "Host").as_str(), "localhost");
        assert_eq!(fld(&r, "Port").as_i32(), 9092);
        assert!(missing(&r, "ThrottleTimeMs"), "v0 不得有 ThrottleTimeMs");
        assert!(missing(&r, "ErrorMessage"), "v0 不得有 ErrorMessage");
        assert!(missing(&r, "Coordinators"), "v0 不得有 Coordinators");
        // v1：+Throttle+ErrorMessage(null)，仍无 Coordinators
        let st = s([("Key", Value::str("g1")), ("KeyType", Value::I8(0))]);
        let r = fc(&ctx, 1, st.clone()).await;
        assert_eq!(fld(&r, "ThrottleTimeMs").as_i32(), 0);
        assert!(matches!(fld(&r, "ErrorMessage"), Value::Null));
        assert!(missing(&r, "Coordinators"), "v1 不得有 Coordinators");
        // v3（0-3 布局的 flexible 上界）
        let r = fc(&ctx, 3, st.clone()).await;
        assert!(missing(&r, "Coordinators"));
        assert_eq!(fld(&r, "Host").as_str(), "localhost");
        // v4+：CoordinatorKeys 批量 + 逐条回显 Key（franz-go 按 Key 匹配）
        let st = s([("CoordinatorKeys", Value::Array(vec![Value::str("alpha"), Value::str("beta")]))]);
        let r = fc(&ctx, 4, st.clone()).await;
        assert!(missing(&r, "ErrorCode"), "v4+ 顶层无 ErrorCode（0-3）");
        assert!(missing(&r, "NodeId"), "v4+ 顶层无 NodeId");
        let cos = arr(&r, "Coordinators");
        assert_eq!(cos.len(), 2, "v4+ 必须逐 key 回显");
        assert_eq!(sfield(&cos[0], "Key").as_str(), "alpha");
        assert_eq!(sfield(&cos[1], "Key").as_str(), "beta");
        assert_eq!(sfield(&cos[0], "ErrorCode").as_i16(), 0);
        assert!(matches!(sfield(&cos[0], "ErrorMessage"), Value::Null));
        // v6（宣告上界）
        let r = fc(&ctx, 6, st.clone()).await;
        let cos = arr(&r, "Coordinators");
        assert_eq!(cos.len(), 2);
        assert_eq!(sfield(&cos[0], "Key").as_str(), "alpha");
        // 空 keys 数组 → 空 Coordinators（Kafka 同义）
        let st = s([("CoordinatorKeys", Value::Array(vec![]))]);
        let r = fc(&ctx, 4, st.clone()).await;
        assert!(arr(&r, "Coordinators").is_empty());
    }

    async fn commit_g1(ctx: &Ctx) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        ctx.group_tx
            .send(GroupCmd::CommitOffsets {
                group: "g1".into(),
                generation: -1,
                member_id: "".into(),
                offsets: vec![
                    CommittedOffset { topic: "t1".into(), partition: 0, offset: 15, metadata: "m0".into(), commit_ts: -1 },
                    CommittedOffset { topic: "t1".into(), partition: 1, offset: 16, metadata: "m1".into(), commit_ts: -1 },
                ],
                reply: tx,
            })
            .await
            .unwrap();
        assert_eq!(rx.await.unwrap().code(), 0, "commit must succeed");
    }

    #[tokio::test]
    async fn probe_offset_fetch_layouts_and_filter() {
        let k = basalt_protocol::api::key::OFFSET_FETCH;
        let ctx = make_ctx(&[("t1", 2), ("t2", 1)]).await;
        commit_g1(&ctx).await;
    

        // v0：无 Throttle（3+）、无顶层 ErrorCode（2-7）、分区无 CommittedLeaderEpoch（5-7）
        let st = s([
            ("GroupId", Value::str("g1")),
            ("Topics", Value::Array(vec![s([
                ("Name", Value::str("t1")),
                ("PartitionIndexes", Value::Array(vec![Value::I32(0)])),
            ])])),
        ]);
        let r = of(&ctx, 0, st).await;
        assert!(missing(&r, "ThrottleTimeMs"), "v0 无 Throttle");
        assert!(missing(&r, "ErrorCode"), "v0 无顶层 ErrorCode");
        let topics = arr(&r, "Topics");
        assert_eq!(topics.len(), 1);
        assert_eq!(sfield(&topics[0], "Name").as_str(), "t1");
        let parts = arr(as_struct(&topics[0]), "Partitions");
        assert_eq!(parts.len(), 1);
        assert_eq!(sfield(&parts[0], "PartitionIndex").as_i32(), 0);
        assert_eq!(sfield(&parts[0], "CommittedOffset").as_i64(), 15);
        assert_eq!(sfield(&parts[0], "Metadata").as_str(), "m0");
        assert_eq!(sfield(&parts[0], "ErrorCode").as_i16(), 0);
        assert!(missing(as_struct(&parts[0]), "CommittedLeaderEpoch"), "v0-v4 无 CLE");

        // v7（flexible）：分区过滤 + 未提交回 -1/null + CLE=-1
        let st = s([
            ("GroupId", Value::str("g1")),
            ("Topics", Value::Array(vec![s([
                ("Name", Value::str("t1")),
                ("PartitionIndexes", Value::Array(vec![Value::I32(1), Value::I32(9)])),
            ])])),
            ("RequireStable", Value::Bool(false)),
        ]);
        let r = of(&ctx, 7, st).await;
        assert_eq!(fld(&r, "ThrottleTimeMs").as_i32(), 0);
        assert_eq!(fld(&r, "ErrorCode").as_i16(), 0);
        let topics = arr(&r, "Topics");
        let parts = arr(as_struct(&topics[0]), "Partitions");
        assert_eq!(parts.len(), 2, "显式分区清单逐条回显");
        assert_eq!(sfield(&parts[0], "PartitionIndex").as_i32(), 1);
        assert_eq!(sfield(&parts[0], "CommittedOffset").as_i64(), 16);
        assert_eq!(sfield(&parts[0], "CommittedLeaderEpoch").as_i32(), -1);
        assert_eq!(sfield(&parts[1], "PartitionIndex").as_i32(), 9);
        assert_eq!(sfield(&parts[1], "CommittedOffset").as_i64(), -1, "未提交回 -1（Kafka 语义）");
        assert!(matches!(sfield(&parts[1], "Metadata"), Value::Null));

        // v2：Topics=null → 仅**有已提交 offset** 的 topic（Kafka 语义；
        // g1 只提交过 t1——与未知组回空 Topics 的断言同一边界）
        let st = s([("GroupId", Value::str("g1")), ("Topics", Value::Null)]);
        let r = of(&ctx, 2, st).await;
        let topics = arr(&r, "Topics");
        let names: Vec<String> = topics.iter().map(|t| sfield(t, "Name").as_str().to_string()).collect();
        assert_eq!(names, vec!["t1".to_string()], "null Topics 只回已提交 topic: {names:?}");

        // v8：Groups[] 布局 + null Topics=该组全部已提交；顶层不得再有 Topics
        let st = s([("Groups", Value::Array(vec![s([
            ("GroupId", Value::str("g1")),
            ("Topics", Value::Null),
        ])])), ("RequireStable", Value::Bool(false))]);
        let r = of(&ctx, 8, st).await;
        assert!(missing(&r, "Topics"), "v8+ 顶层无 Topics");
        assert_eq!(fld(&r, "ThrottleTimeMs").as_i32(), 0);
        let groups = arr(&r, "Groups");
        assert_eq!(groups.len(), 1);
        assert_eq!(sfield(&groups[0], "GroupId").as_str(), "g1");
        assert_eq!(sfield(&groups[0], "ErrorCode").as_i16(), 0);
        let gtopics = arr(as_struct(&groups[0]), "Topics");
        let t1 = gtopics.iter().find(|t| sfield(t, "Name").as_str() == "t1").expect("t1 must be returned");
        let gparts = arr(as_struct(t1), "Partitions");
        assert_eq!(gparts.len(), 2, "v8 null Topics = 该组全部已提交（t1 双分区）");
        assert_eq!(sfield(&gparts[0], "CommittedOffset").as_i64(), 15);
        assert_eq!(sfield(&gparts[1], "CommittedOffset").as_i64(), 16);

        // v8：显式分区过滤 + 未提交 -1
        let st = s([("Groups", Value::Array(vec![s([
            ("GroupId", Value::str("g1")),
            ("Topics", Value::Array(vec![s([
                ("Name", Value::str("t1")),
                ("PartitionIndexes", Value::Array(vec![Value::I32(1), Value::I32(9)])),
            ])])),
        ])]))]);
        let r = of(&ctx, 8, st).await;
        let groups = arr(&r, "Groups");
        let gtopics = arr(as_struct(&groups[0]), "Topics");
        let gparts = arr(as_struct(&gtopics[0]), "Partitions");
        assert_eq!(gparts.len(), 2);
        assert_eq!(sfield(&gparts[0], "CommittedOffset").as_i64(), 16);
        assert_eq!(sfield(&gparts[1], "CommittedOffset").as_i64(), -1);

        // v8：未知组 → GroupId 逐条回显 + 空 Topics + ErrorCode 0
        let st = s([("Groups", Value::Array(vec![s([
            ("GroupId", Value::str("ghost")),
            ("Topics", Value::Null),
        ])]))]);
        let r = of(&ctx, 8, st).await;
        let groups = arr(&r, "Groups");
        assert_eq!(sfield(&groups[0], "GroupId").as_str(), "ghost", "组 id 必须回显");
        assert_eq!(sfield(&groups[0], "ErrorCode").as_i16(), 0);
        assert!(arr(as_struct(&groups[0]), "Topics").is_empty());

        // v9：+ MemberId/MemberEpoch 请求域
        let st = s([("Groups", Value::Array(vec![s([
            ("GroupId", Value::str("g1")),
            ("MemberId", Value::Null),
            ("MemberEpoch", Value::I32(-1)),
            ("Topics", Value::Null),
        ])]))]);
        let r = of(&ctx, 9, st).await;
        let groups = arr(&r, "Groups");
        assert_eq!(sfield(&groups[0], "GroupId").as_str(), "g1");
    }

    #[tokio::test]
    async fn probe_delete_topics_layouts() {
        let k = basalt_protocol::api::key::DELETE_TOPICS;
        let ctx = make_ctx(&[("t1", 1)]).await;
    
        // v0：TopicNames 分支；响应无 TopicId（6+）无 ErrorMessage（5+）
        let st = s([("TopicNames", Value::Array(vec![Value::str("t1")])), ("TimeoutMs", Value::I32(3000))]);
        let r = dt(&ctx, 0, st).await;
        let rs = arr(&r, "Responses");
        assert_eq!(rs.len(), 1);
        assert_eq!(sfield(&rs[0], "Name").as_str(), "t1");
        assert_eq!(sfield(&rs[0], "ErrorCode").as_i16(), 0, "t1 在路由中 → None");
        assert!(missing(as_struct(&rs[0]), "TopicId"), "v0 无 TopicId");
        assert!(missing(as_struct(&rs[0]), "ErrorMessage"), "v0 无 ErrorMessage");
        assert!(missing(&r, "ThrottleTimeMs"), "v0 无 Throttle");
        // v5：ErrorMessage 出现（5+），TopicId 仍无
        let st = s([("TopicNames", Value::Array(vec![Value::str("t1")])), ("TimeoutMs", Value::I32(3000))]);
        let r = dt(&ctx, 5, st).await;
        let rs = arr(&r, "Responses");
        assert!(as_struct(&rs[0]).get("ErrorMessage").is_some(), "v5 起 ErrorMessage 存在");
        assert!(missing(as_struct(&rs[0]), "TopicId"), "v5 无 TopicId");
        assert_eq!(fld(&r, "ThrottleTimeMs").as_i32(), 0);
        // v5 未知 topic
        let st = s([("TopicNames", Value::Array(vec![Value::str("nope")])), ("TimeoutMs", Value::I32(3000))]);
        let r = dt(&ctx, 5, st).await;
        let rs = arr(&r, "Responses");
        assert_eq!(sfield(&rs[0], "ErrorCode").as_i16(), 3, "UnknownTopicOrPartition");
        // v6：Topics（DeleteTopicState）分支 + TopicId 回显
        let st = s([("Topics", Value::Array(vec![s([
            ("Name", Value::str("t1")),
            ("TopicId", Value::Uuid(7)),
        ])])), ("TimeoutMs", Value::I32(3000))]);
        let r = dt(&ctx, 6, st).await;
        let rs = arr(&r, "Responses");
        assert_eq!(sfield(&rs[0], "Name").as_str(), "t1");
        assert_eq!(sfield(&rs[0], "TopicId").as_uuid(), 7, "v6 TopicId 回显");
        assert_eq!(sfield(&rs[0], "ErrorCode").as_i16(), 0);
        // v6：纯 TopicId（Name null）→ UNKNOWN_TOPIC_ID(100)
        let st = s([("Topics", Value::Array(vec![s([
            ("Name", Value::Null),
            ("TopicId", Value::Uuid(7)),
        ])])), ("TimeoutMs", Value::I32(3000))]);
        let r = dt(&ctx, 6, st).await;
        let rs = arr(&r, "Responses");
        assert_eq!(sfield(&rs[0], "ErrorCode").as_i16(), 100, "纯 TopicId 暂不支持 → UnknownTopicId");
    }

    async fn bring_group_stable(ctx: &Ctx, group: &str, sub: &[u8], assign: &[u8]) -> String {
        let (tx, rx) = tokio::sync::oneshot::channel();
        ctx.group_tx
            .send(GroupCmd::JoinGroup(JoinSpec {
                group: group.into(),
                member_id: "".into(),
                protocol_type: "consumer".into(),
                session_timeout_ms: 10_000,
                rebalance_timeout_ms: 10_000,
                protocols: vec![("range".into(), sub.to_vec())],
                client_host: "localhost".into(),
            }, tx))
            .await
            .unwrap();
        let j = rx.await.unwrap();
        assert_eq!(j.error.code(), 0, "join failed: {:?}", j.error);
        let (tx, rx) = tokio::sync::oneshot::channel();
        ctx.group_tx
            .send(GroupCmd::SyncGroup(SyncSpec {
                group: group.into(),
                generation: j.generation,
                member_id: j.member_id.clone(),
                protocol_type: Some("consumer".into()),
                protocol: Some("range".into()),
                assignments: vec![(j.member_id.clone(), assign.to_vec())],
            }, tx))
            .await
            .unwrap();
        let sy = rx.await.unwrap();
        assert_eq!(sy.error.code(), 0, "sync failed: {:?}", sy.error);
        j.member_id
    }

    #[tokio::test]
    async fn probe_describe_groups_layouts() {
        let k = basalt_protocol::api::key::DESCRIBE_GROUPS;
        let ctx = make_ctx(&[]).await;
        let mid = bring_group_stable(&ctx, "gg", b"SUB", b"ASSIGN").await;

    
        // v0：无 AuthorizedOperations（3+）无 ErrorMessage（6+）——协调器真值 + 成员明细
        let r = dg(&ctx, 0, vec![Value::str("gg")]).await;
        let gs = arr(&r, "Groups");
        assert_eq!(gs.len(), 1);
        let g = as_struct(&gs[0]);
        assert_eq!(fld(g, "ErrorCode").as_i16(), 0);
        assert_eq!(fld(g, "GroupState").as_str(), "Stable");
        assert_eq!(fld(g, "ProtocolType").as_str(), "consumer");
        assert_eq!(fld(g, "ProtocolData").as_str(), "range");
        assert!(missing(g, "AuthorizedOperations"), "v0-v2 无 AuthOps");
        assert!(missing(g, "ErrorMessage"), "v0-v5 无 ErrorMessage");
        let ms = arr(g, "Members");
        assert_eq!(ms.len(), 1, "Stable 组必须有成员明细");
        assert_eq!(sfield(&ms[0], "MemberId").as_str(), mid);
        assert_eq!(sfield(&ms[0], "MemberMetadata").as_bytes().unwrap().as_ref(), b"SUB");
        assert_eq!(sfield(&ms[0], "MemberAssignment").as_bytes().unwrap().as_ref(), b"ASSIGN");
        // v3：AuthorizedOperations 出现
        let r = dg(&ctx, 3, vec![Value::str("gg")]).await;
        let gs = arr(&r, "Groups");
        assert!(as_struct(&gs[0]).get("AuthorizedOperations").is_some(), "v3+ 有 AuthOps");
        // v5（flexible 上界）
        let r = dg(&ctx, 5, vec![Value::str("gg")]).await;
        let gs = arr(&r, "Groups");
        assert_eq!(sfield(&gs[0], "GroupState").as_str(), "Stable");
        // 不存在的组 → Dead + ErrorCode 0 + 空 Members
        let r = dg(&ctx, 0, vec![Value::str("ghost")]).await;
        let gs = arr(&r, "Groups");
        let g = as_struct(&gs[0]);
        assert_eq!(fld(g, "ErrorCode").as_i16(), 0);
        assert_eq!(fld(g, "GroupState").as_str(), "Dead");
        assert!(arr(g, "Members").is_empty());
    }

    #[tokio::test]
    async fn probe_list_groups_layouts_and_filter() {
        let k = basalt_protocol::api::key::LIST_GROUPS;
        let ctx = make_ctx(&[]).await;
        bring_group_stable(&ctx, "gg", b"S", b"A").await;
    
        // v1：协调器真值（此前恒空列表）
        let r = lg(&ctx, 1, None).await;
        assert_eq!(fld(&r, "ErrorCode").as_i16(), 0);
        let gs = arr(&r, "Groups");
        assert_eq!(gs.len(), 1, "ListGroups 必须返回协调器真值");
        assert_eq!(sfield(&gs[0], "GroupId").as_str(), "gg");
        assert!(missing(as_struct(&gs[0]), "GroupState"), "v1-v3 无 GroupState 字段");
        // v4：GroupState 字段出现 + StatesFilter 生效
        let r = lg(&ctx, 4, Some(vec!["Stable".into()])).await;
        let gs = arr(&r, "Groups");
        assert_eq!(gs.len(), 1);
        assert_eq!(sfield(&gs[0], "GroupState").as_str(), "Stable");
        let r = lg(&ctx, 4, Some(vec!["Empty".into()])).await;
        assert!(arr(&r, "Groups").is_empty(), "StatesFilter 不匹配 → 空");
    }

    #[tokio::test]
    async fn probe_init_producer_id_layouts() {
        let k = basalt_protocol::api::key::INIT_PRODUCER_ID;
        let ctx = make_ctx(&[]).await;
    
        let r0 = ip(&ctx, 0).await;
        assert_eq!(fld(&r0, "ErrorCode").as_i16(), 0);
        let pid0 = fld(&r0, "ProducerId").as_i64();
        assert!(missing(&r0, "OngoingTxnProducerId"), "v6 字段不得出现（宣告上界 5）");
        let r1 = ip(&ctx, 5).await;
        assert_eq!(fld(&r1, "ProducerId").as_i64(), pid0 + 1, "单调分配");
        assert_eq!(fld(&r1, "ProducerEpoch").as_i16(), 0);
    }

    async fn fc(ctx: &Ctx, v: i16, st: Value) -> Struct {
        let k = basalt_protocol::api::key::FIND_COORDINATOR;
        let st = as_struct_owned(st);
        let req = req_at(k, v, &st);
        let resp = handlers_groups::find_coordinator(v, &req, ctx).await;
        resp_decode(k, v, &resp_bytes(k, v, &resp))
    }
    async fn of(ctx: &Ctx, v: i16, st: Value) -> Struct {
        let k = basalt_protocol::api::key::OFFSET_FETCH;
        let st = as_struct_owned(st);
        let req = req_at(k, v, &st);
        let resp = handlers_groups::offset_fetch(&req, v, ctx).await;
        resp_decode(k, v, &resp_bytes(k, v, &resp))
    }
    async fn dt(ctx: &Ctx, v: i16, st: Value) -> Struct {
        let k = basalt_protocol::api::key::DELETE_TOPICS;
        let st = as_struct_owned(st);
        let req = req_at(k, v, &st);
        let resp = handlers_groups::delete_topics(&req, ctx).await;
        resp_decode(k, v, &resp_bytes(k, v, &resp))
    }
    async fn dg(ctx: &Ctx, v: i16, groups: Vec<Value>) -> Struct {
        let k = basalt_protocol::api::key::DESCRIBE_GROUPS;
        let mut st = Struct::new();
        st.set("Groups", Value::Array(groups));
        if v >= 3 {
            st.set("IncludeAuthorizedOperations", Value::Bool(false));
        }
        let req = req_at(k, v, &st);
        let resp = handlers_groups::describe_groups(&req, ctx).await;
        resp_decode(k, v, &resp_bytes(k, v, &resp))
    }
    async fn lg(ctx: &Ctx, v: i16, filter: Option<Vec<String>>) -> Struct {
        let k = basalt_protocol::api::key::LIST_GROUPS;
        let mut st = Struct::new();
        if let Some(f) = filter {
            st.set("StatesFilter", Value::Array(f.into_iter().map(Value::str).collect()));
        }
        let req = req_at(k, v, &st);
        let resp = handlers_groups::list_groups(&req, ctx).await;
        resp_decode(k, v, &resp_bytes(k, v, &resp))
    }
    async fn ip(ctx: &Ctx, v: i16) -> Struct {
        let k = basalt_protocol::api::key::INIT_PRODUCER_ID;
        let st = as_struct_owned(s([("TransactionalId", Value::Null), ("TransactionTimeoutMs", Value::I32(60000))]));
        let req = req_at(k, v, &st);
        let resp = handlers_groups::init_producer_id(&req, ctx).await;
        resp_decode(k, v, &resp_bytes(k, v, &resp))
    }
}
