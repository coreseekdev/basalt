package io.basalt.e2e;

// kafka-clients（Apache Java 官方栈）兼容性 e2e——客户端多样性第四档（账本 C11）。
//
// 与 franz-go / librdkafka_compat.py 同构：produce 20 → 组消费者 A 全量读 +
// 显式提交 → committed 回读 → 追加 10 → 消费者 B 从提交位续读不重不漏。
// kafka-clients 的协议 Negotiator（ApiVersions v3、Produce v11+、
// OffsetFetch v8+、Fetch v12+）与前三个栈均不同——handler 布局分叉在此暴露。
//
// 用法：testing/e2e/run_kafkaclients.sh（拉起 broker 后 mvn exec:java）。

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

    public static void main(String[] args) {
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
        System.out.println("PASS ✔ (kafka-clients 官方栈全组协议)");
    }
}
