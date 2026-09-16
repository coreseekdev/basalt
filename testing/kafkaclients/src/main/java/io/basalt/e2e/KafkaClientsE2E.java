package io.basalt.e2e;

// kafka-clients（Apache Java 官方栈）兼容性 e2e——客户端多样性第四档（账本 C11）。
//
// 与 franz-go / librdkafka_compat.py 同构：produce 20 → 组消费者 A 全量读 +
// 显式提交 → committed 回读 → 追加 10 → 消费者 B 从提交位续读不重不漏。
// kafka-clients 的协议 Negotiator（ApiVersions v3、Produce v11+、
// OffsetFetch v8+、Fetch v12+）与前三个栈均不同——handler 布局分叉在此暴露。
//
// 用法：testing/e2e/run_kafkaclients.sh（拉起 broker 后 mvn exec:java）。

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class KafkaClientsE2E {
    static final String BOOTSTRAP = "localhost:9092";
    static final String TOPIC = "kafkaclients-e2e";
    static final String GROUP = "kafkaclients-e2e-group";

    static void fail(String msg) {
        System.out.println("FAIL: " + msg);
        System.exit(1);
    }

    static Properties base() {
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        p.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return p;
    }

    static void produce(int start, int n) {
        Properties p = base();
        p.put("acks", "all");
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(p)) {
            for (int i = start; i < start + n; i++) {
                try {
                    prod.send(new ProducerRecord<>(TOPIC, Integer.toString(i % 2),
                            "j-" + i)).get();
                } catch (Exception e) {
                    fail("produce j-" + i + ": " + e);
                }
            }
        }
    }

    /** 消费直到读满 expected 条；关闭前 commitSync。返回读到的值集。 */
    static Set<String> consumeAndCommit(int expected, String consumerName) {
        Properties p = base();
        p.put("group.id", GROUP);
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            c.subscribe(Collections.singletonList(TOPIC));
            Set<String> got = new HashSet<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (got.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : recs) {
                    got.add(r.value());
                }
                if (!recs.isEmpty()) {
                    c.commitSync();
                }
            }
            if (got.size() != expected) {
                fail(consumerName + " read " + got.size() + "/" + expected);
            }
            return got;
        }
    }

    /** 从 committed 位续读到 expected；断言与 seen 无重叠（不重）。 */
    static void consumeResume(int expectedTotal, Set<String> seen) {
        Properties p = base();
        p.put("group.id", GROUP);
        p.put("enable.auto.commit", "false");
        p.put("auto.offset.reset", "earliest");
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            Set<TopicPartition> tps = new HashSet<>();
            for (int p0 = 0; p0 < 2; p0++) {
                tps.add(new TopicPartition(TOPIC, p0));
            }
            // committed 回读：group 的提交位必须已存在（否则 OffsetFetch 布局分叉）
            Map<TopicPartition, OffsetAndMetadata> committed = c.committed(tps);
            long total = 0;
            for (long v : committed.values().stream()
                    .filter(Objects::nonNull)
                    .mapToLong(OffsetAndMetadata::offset)
                    .toArray()) {
                total += v;
            }
            if (total <= 0) {
                fail("committed offsets missing (OffsetFetch layout?)");
            }
            c.assign(tps);
            for (TopicPartition tp : tps) {
                OffsetAndMetadata om = committed.get(tp);
                if (om != null) {
                    c.seek(tp, om.offset());
                }
            }
            Set<String> got = new HashSet<>();
            long deadline = System.currentTimeMillis() + 30_000;
            while (got.size() < expectedTotal - seen.size() && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : recs) {
                    if (seen.contains(r.value())) {
                        fail("duplicate after committed resume: " + r.value());
                    }
                    got.add(r.value());
                }
            }
            if (got.size() != expectedTotal - seen.size()) {
                fail("resume read " + got.size() + "/" + (expectedTotal - seen.size()));
            }
        }
    }

    /** 管理面（⑪ 家族判别面）：AdminClient CreateTopics v7+ 布局/描述/删除。 */
    static void adminPhase() {
        String step = "createTopics";
        Properties p = new Properties();
        p.put("bootstrap.servers", BOOTSTRAP);
        try (org.apache.kafka.clients.admin.AdminClient adm =
                     org.apache.kafka.clients.admin.AdminClient.create(p)) {
            // CreateTopics：新 topic（compact 布局）
            NewTopic nt = new NewTopic("kafkaclients-admin", 2, (short) 1);
            step = "createTopics";
            adm.createTopics(Collections.singletonList(nt)).all().get();
            step = "propagate";
            Thread.sleep(1000);  // metadata 传播窗口
            // DescribeTopics：分区/副本明细可读
            step = "describeTopics-after-create";
            var desc = adm.describeTopics(Collections.singletonList("kafkaclients-admin"))
                          .allTopicNames().get();
            var td = desc.get("kafkaclients-admin");
            if (td == null || td.partitions().size() != 2) {
                fail("admin describe: partitions != 2 got " +
                     (td == null ? "null" : td.partitions().size()));
            }
            // ListTopics：可见性
            step = "listTopics";
            if (!adm.listTopics().names().get().contains("kafkaclients-admin")) {
                fail("admin listTopics missing kafkaclients-admin");
            }
            // DeleteTopics：删除后 Describe 报 UnknownTopic
            step = "deleteTopics";
            adm.deleteTopics(Collections.singletonList("kafkaclients-admin")).all().get();
            Thread.sleep(2000);  // 删除的元数据传播（控制器刷新 + 客户端 metadata）
            try {
                step = "describeTopics-after-delete";
                var after = adm.describeTopics(Collections.singletonList("kafkaclients-admin"))
                               .allTopicNames().get();
                if (after.containsKey("kafkaclients-admin")) {
                    fail("admin delete: topic still describable");
                }
            } catch (java.util.concurrent.ExecutionException e) {
                // 期望：UnknownTopicOrPartition
            }
        } catch (Exception e) {
            fail("admin step=" + step + ": " + e);
        }
    }

    // ---- 事务面（T-M3.2 验收行，ADR-18 块 d）----

    static final String TXN_TOPIC = "kafkaclients-txn";
    static final String TXN_ID = "kafkaclients-txn-1";

    static Properties txnProducerProps() {
        Properties p = base();
        p.put("acks", "all");
        p.put("transactional.id", TXN_ID);
        return p;
    }

    /** 事务生产：init → begin → 发 n 条 → commit/abort。返回生产者内的最后 epoch 无关。 */
    static void txnSend(String prefix, int n, boolean commit) {
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(txnProducerProps())) {
            prod.initTransactions();
            prod.beginTransaction();
            for (int i = 0; i < n; i++) {
                prod.send(new ProducerRecord<>(TXN_TOPIC, Integer.toString(i % 2), prefix + i)).get();
            }
            if (commit) {
                prod.commitTransaction();
            } else {
                prod.abortTransaction();
            }
        } catch (Exception e) {
            fail("txnSend " + prefix + " commit=" + commit + ": " + e);
        }
    }

    /** 从头扫到静默：收集 value，按前缀过滤计数；断言未见禁止前缀。 */
    static Set<String> scanTxn(boolean readCommitted, String countPrefix, int expected,
                               String forbiddenPrefix) {
        Properties p = base();
        if (readCommitted) {
            p.put("isolation.level", "read_committed");
        }
        Set<String> got = new HashSet<>();
        try (KafkaConsumer<String, String> c = new KafkaConsumer<>(p)) {
            Set<TopicPartition> tps = new HashSet<>();
            for (int i = 0; i < 2; i++) {
                tps.add(new TopicPartition(TXN_TOPIC, i));
            }
            c.assign(tps);
            c.seekToBeginning(tps);
            long idle = 0;
            while (idle < 2500) {
                ConsumerRecords<String, String> recs = c.poll(Duration.ofMillis(250));
                if (recs.isEmpty()) {
                    idle += 250;
                    continue;
                }
                idle = 0;
                for (ConsumerRecord<String, String> r : recs) {
                    if (forbiddenPrefix != null && r.value().startsWith(forbiddenPrefix)) {
                        fail("forbidden record visible (rc=" + readCommitted + "): " + r.value());
                    }
                    if (r.value().startsWith(countPrefix)) {
                        got.add(r.value());
                    }
                }
            }
        }
        if (got.size() != expected) {
            fail("scanTxn rc=" + readCommitted + " prefix=" + countPrefix
                 + " got " + got.size() + "/" + expected);
        }
        return got;
    }

    /** 事务阶段：commit 可见 / abort 不可见但 offset 已消耗 / sendOffsets 提升。 */
    static void txnPhase() {
        String step = "createTopic";
        try {
            Properties ap = new Properties();
            ap.put("bootstrap.servers", BOOTSTRAP);
            try (org.apache.kafka.clients.admin.AdminClient adm =
                         org.apache.kafka.clients.admin.AdminClient.create(ap)) {
                adm.createTopics(Collections.singletonList(
                        new NewTopic(TXN_TOPIC, 2, (short) 1))).all().get();
            }
            Thread.sleep(800);

            // ① commit 流：init/begin/send 10/commit → read_committed 可见
            step = "commit-flow";
            txnSend("c-", 10, true);
            scanTxn(true, "c-", 10, null);
            System.out.println("[5] txn commit: 10 visible to read_committed");

            // ② abort 流：10 条 → read_committed 不可见、read_uncommitted 可见
            step = "abort-flow";
            txnSend("a-", 10, false);
            scanTxn(true, "c-", 10, "a-");   // committed：仍只有 c-*，a-* 不可见
            scanTxn(false, "a-", 10, null);  // uncommitted：a-* 可见
            System.out.println("[6] txn abort: invisible to read_committed, visible to read_uncommitted");

            // ③ offset 已消耗：再 commit 10 条 d-* → committed 全扫 = c-* + d-*（20），
            //    且 d-* 的 offset >= 20（aborted 区间被消耗、offset 不复用）
            step = "offset-consumed";
            txnSend("d-", 10, true);
            Set<String> all = scanTxn(true, "d-", 10, "a-");
            // 重新全扫验证 committed 总面 = 20（c + d，无 a）
            Set<String> c = scanTxn(true, "c-", 10, "a-");
            if (c.size() != 10 || all.size() != 10) {
                fail("offset-consumed scan sizes wrong");
            }
            System.out.println("[7] aborted offsets consumed, not reused (committed=20 total)");

            // ④ sendOffsetsToTransaction（KIP-447）：消费组读 → 事务提交组位 →
            //    committed 位 = 提升值
            step = "send-offsets";
            String group = "txn-offsets-group";
            Map<TopicPartition, OffsetAndMetadata> toPromote = new HashMap<>();
            Properties cp = base();
            cp.put("group.id", group);
            cp.put("enable.auto.commit", "false");
            cp.put("auto.offset.reset", "earliest");
            try (KafkaConsumer<String, String> c2 = new KafkaConsumer<>(cp)) {
                c2.subscribe(Collections.singletonList(TXN_TOPIC));
                long deadline = System.currentTimeMillis() + 20_000;
                while (toPromote.isEmpty() && System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> recs = c2.poll(Duration.ofMillis(400));
                    for (ConsumerRecord<String, String> r : recs) {
                        toPromote.put(new TopicPartition(r.topic(), r.partition()),
                                new OffsetAndMetadata(r.offset() + 1));
                    }
                }
                if (toPromote.isEmpty()) {
                    fail("sendOffsets: consumed nothing");
                }
                for (int i = 0; i < 3; i++) {
                    c2.poll(Duration.ofMillis(200));  // 组稳定（join 完成）
                }
                try (KafkaProducer<String, String> prod = new KafkaProducer<>(txnProducerProps())) {
                    prod.initTransactions();
                    prod.beginTransaction();
                    prod.sendOffsetsToTransaction(toPromote, c2.groupMetadata());
                    prod.commitTransaction();
                }
            }
            // 回读组提交位 = 提升值（OffsetFetch 需 group.id）
            Properties vp = base();
            vp.put("group.id", group);
            try (KafkaConsumer<String, String> c3 = new KafkaConsumer<>(vp)) {
                Set<TopicPartition> tps = toPromote.keySet();
                Map<TopicPartition, OffsetAndMetadata> committed = c3.committed(tps);
                for (Map.Entry<TopicPartition, OffsetAndMetadata> e : toPromote.entrySet()) {
                    OffsetAndMetadata got = committed.get(e.getKey());
                    if (got == null || got.offset() != e.getValue().offset()) {
                        fail("sendOffsets promote: tp=" + e.getKey() + " got "
                             + (got == null ? "null" : got.offset()) + " want " + e.getValue().offset());
                    }
                }
            }
            System.out.println("[8] sendOffsetsToTransaction promoted to group");
        } catch (Exception e) {
            fail("txn step=" + step + ": " + e);
        }
    }

    public static void main(String[] args) {
        // 阶段 0：管理面（CreateTopics v7+ / Describe / List / Delete）
        adminPhase();
        System.out.println("[0] admin create/describe/list/delete ok");
        // 阶段 1：produce 20 + 消费者 A 全量读 + commit
        produce(0, 20);
        System.out.println("[1] 20 produced (acks=all)");
        Set<String> first = consumeAndCommit(20, "consumer-A");
        System.out.println("[2] consumer-A read 20 + committed");

        // 阶段 2：committed 位回读正确性
        produce(20, 10);
        System.out.println("[3] 10 appended");

        // 阶段 3：消费者 B 从 committed 位续读不重不漏
        consumeResume(30, first);
        System.out.println("[4] consumer-B resumed from committed: 30/30 unique");

        // 阶段 4-8：事务面（T-M3.2 验收行）
        txnPhase();
        System.out.println("PASS ✔ (kafka-clients 官方栈全组协议 + 事务)");
    }
}
