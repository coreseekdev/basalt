// 吞吐基准（franz-go 直连面，无组协议）：建题 → produce N×1KB 计时 →
// 裸分区 consume 计时。用于配额/改动的回归基线（数字随硬件浮动，
// 关注相对变化而非绝对值）。
//
// 用法：testing/bench/run_bench.sh [port] [messages]
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	broker := "localhost:9092"
	if len(os.Args) > 1 {
		broker = "localhost:" + os.Args[1]
	}
	total := 200000
	if len(os.Args) > 2 {
		if n, err := strconv.Atoi(os.Args[2]); err == nil {
			total = n
		}
	}
	const parts = 4
	topic := "bench-topic"
	payload := make([]byte, 1000)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ProducerBatchCompression(kgo.NoCompression()),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// 解除客户端背压（默认 10000 条）：让瓶颈落在服务端而非客户端缓冲
		kgo.MaxBufferedRecords(1_000_000),
	)
	if err != nil {
		fail("client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx := context.Background()
	_, _ = adm.CreateTopic(ctx, parts, 1, nil, topic)
	time.Sleep(500 * time.Millisecond)

	// ---- produce ----
	t0 := time.Now()
	for i := 0; i < total; i++ {
		cl.Produce(ctx, &kgo.Record{
			Topic: topic, Partition: int32(i % parts), Value: payload,
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				fail("produce: %v", err)
			}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		fail("flush: %v", err)
	}
	pt := time.Since(t0)
	pmb := float64(total*len(payload)) / 1e6
	fmt.Printf("PRODUCE %d msgs ×1KB (%.0f MB) in %.2fs → %.1f MB/s, %.0f msgs/s\n",
		total, pmb, pt.Seconds(), pmb/pt.Seconds(), float64(total)/pt.Seconds())

	// ---- consume（裸分区，从 0 读满 N 条）----
	offs := map[string]map[int32]kgo.Offset{topic: {}}
	for p := int32(0); p < parts; p++ {
		offs[topic][p] = kgo.NewOffset().At(0)
	}
	ccl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.ConsumePartitions(offs))
	if err != nil {
		fail("consumer: %v", err)
	}
	defer ccl.Close()
	t0 = time.Now()
	got := 0
	deadline := time.Now().Add(120 * time.Second)
	for got < total && time.Now().Before(deadline) {
		fs := ccl.PollRecords(ctx, 10000)
		fs.EachError(func(_ string, _ int32, err error) { fail("fetch: %v", err) })
		fs.EachRecord(func(*kgo.Record) { got++ })
	}
	ct := time.Since(t0)
	if got != total {
		fail("consume %d/%d", got, total)
	}
	cmb := float64(got*len(payload)) / 1e6
	fmt.Printf("CONSUME %d msgs (%.0f MB) in %.2fs → %.1f MB/s, %.0f msgs/s\n",
		got, cmb, ct.Seconds(), cmb/ct.Seconds(), float64(got)/ct.Seconds())
	fmt.Println("BENCH PASS")
}

func fail(format string, a ...any) {
	fmt.Printf("FAIL: %s\n", fmt.Sprintf(format, a...))
	os.Exit(1)
}
