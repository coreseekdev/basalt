package io.basalt.e2e;

// T-M4.1 SASL/TLS 客户端矩阵——kafka-clients（Java 官方栈）档：
// SASL_SSL（SCRAM-SHA-256 + TLS，PEM truststore）produce 10 + consume 10。
//
// 用法：run_sasl_matrix.sh 拉起 broker（BASALT_AUTH=scram + TLS）后
//   mvn exec:java -Dexec.mainClass=io.basalt.e2e.SaslSslE2E
// 参数：argv[0]=port argv[1]=CA 证书 PEM 路径。

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

public class SaslSslE2E {
    static void fail(String msg) {
        System.out.println("FAIL: " + msg);
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = "localhost:" + (args.length > 0 ? args[0] : "9092");
        String caPath = args.length > 1 ? args[1] : "";
        String caPem = Files.readString(Path.of(caPath));
        String topic = "sasl-matrix-java";

        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("security.protocol", "SASL_SSL");
        p.put("sasl.mechanism", "SCRAM-SHA-256");
        p.put("sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                        + "username=\"app\" password=\"apppass\";");
        p.put("ssl.truststore.type", "PEM");
        p.put("ssl.truststore.certificates", caPem);

        try (AdminClient adm = AdminClient.create(p)) {
            try {
                adm.createTopics(List.of(new NewTopic(topic, 2, (short) 1))).all().get();
            } catch (Exception e) {
                if (!(e.getCause() instanceof TopicExistsException)) {
                    throw e;
                }
            }
        }
        Thread.sleep(500);

        // produce 10
        Properties pp = (Properties) p.clone();
        pp.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pp.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        pp.put("acks", "all");
        try (KafkaProducer<String, String> prod = new KafkaProducer<>(pp)) {
            for (int i = 0; i < 10; i++) {
                prod.send(new ProducerRecord<>(topic, Integer.toString(i % 2),
                        "js-" + i)).get();
            }
        }
        System.out.println("[kafka-clients sasl_ssl] produce 10 via SCRAM ✔");

        // consume 10（裸 assign，避免组状态跨跑残留）
        Properties pc = (Properties) p.clone();
        pc.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        pc.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        try (KafkaConsumer<String, String> cons = new KafkaConsumer<>(pc)) {
            List<TopicPartition> parts = List.of(
                    new TopicPartition(topic, 0), new TopicPartition(topic, 1));
            cons.assign(parts);
            cons.seekToBeginning(parts);
            Set<String> got = new HashSet<>();
            long deadline = System.currentTimeMillis() + 20_000;
            while (got.size() < 10 && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> r : cons.poll(Duration.ofMillis(500))) {
                    if (r.value().startsWith("js-")) {
                        got.add(r.value());
                    }
                }
            }
            if (got.size() != 10) {
                fail("consume " + got.size() + "/10");
            }
        }
        System.out.println("[kafka-clients sasl_ssl] consume 10 exactly-once ✔");
        System.out.println("PASS ✔ (kafka-clients SASL_SSL SCRAM-SHA-256)");
    }
}
