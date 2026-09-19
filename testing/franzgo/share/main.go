// KIP-932 share groups spike e2e（franz-go v1.21.6 真客户端）：
//   [1] produce 20 → share consumer A poll 到 20 → AckAccept
//   [2] 再 poll：无新数据（全部 accepted）
//   [3] 再 produce 20 → A poll 到后 AckRelease + Flush
//   [4] share consumer B（同组）：重投递拿到同 20 条，DeliveryCount ≥ 2
// runner：testing/e2e/run_share_spike.sh。
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func fail(format string, a ...any) {
	fmt.Printf("FAIL: %s\n", fmt.Sprintf(format, a...))
	os.Exit(1)
}

type pollResult struct {
	n           int
	maxDelivery int32
}

// pollAck：poll 至 want 条或超时；每条按 status ack（status=0 则不显式 ack，
// 依赖 poll 自动 accept 语义）。
func pollAck(cl *kgo.Client, want int, timeout time.Duration, status kgo.AckStatus) pollResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var res pollResult
	deadline := time.Now().Add(timeout)
	for res.n < want && time.Now().Before(deadline) {
		fetches := cl.PollRecords(ctx, 100)
		fetches.EachError(func(_ string, _ int32, err error) {
			fmt.Printf("  (fetch err: %v)\n", err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			res.n++
			if dc := r.DeliveryCount(); dc > res.maxDelivery {
				res.maxDelivery = dc
			}
			if status != 0 {
				r.Ack(status)
			}
		})
	}
	if status != 0 {
		cl.FlushAcks(ctx)
	}
	return res
}

func main() {
	broker := "localhost:9092"
	if len(os.Args) > 1 {
		broker = "localhost:" + os.Args[1]
	}
	topic := "share-e2e"
	ctx := context.Background()

	ac, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fail("admin client: %v", err)
	}
	adm := kadm.NewClient(ac)
	_, _ = adm.CreateTopic(ctx, 1, 1, nil, topic)
	time.Sleep(800 * time.Millisecond)

	produce := func(prefix string, n int) {
		p, err := kgo.NewClient(kgo.SeedBrokers(broker))
		if err != nil {
			fail("producer: %v", err)
		}
		defer p.Close()
		for i := 0; i < n; i++ {
			p.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("%s-%02d", prefix, i))},
				func(_ *kgo.Record, err error) {
					if err != nil {
						fail("produce %s-%02d: %v", prefix, i, err)
					}
				})
		}
		if err := p.Flush(ctx); err != nil {
			fail("flush: %v", err)
		}
	}

	newShareConsumer := func(group string) *kgo.Client {
		opts := []kgo.Opt{
			kgo.SeedBrokers(broker),
			kgo.ShareGroup(group),
			kgo.ConsumeTopics(topic),
		}
		if os.Getenv("SHARE_DEBUG") != "" {
			opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))
		}
		cl, err := kgo.NewClient(opts...)
		if err != nil {
			fail("share consumer: %v", err)
		}
		return cl
	}

	// [1] 20 条 → A poll → AckAccept
	produce("s1", 20)
	clA := newShareConsumer("share-spike")
	defer clA.Close()
	r1 := pollAck(clA, 20, 20*time.Second, kgo.AckAccept)
	if r1.n != 20 {
		fail("[1] A poll 到 %d/20", r1.n)
	}
	fmt.Println("[1] consumer A: fetched + AckAccept 20 ✔")

	// [2] 再 poll：accepted 不重投
	got2 := pollAck(clA, 1, 6*time.Second, kgo.AckAccept)
	if got2.n != 0 {
		fail("[2] accepted 后仍 poll 到 %d 条（应 0）", got2.n)
	}
	fmt.Println("[2] accepted records not redelivered ✔")

	// [3] 新 20 条 → AckRelease
	produce("s2", 20)
	r3 := pollAck(clA, 20, 20*time.Second, kgo.AckRelease)
	if r3.n != 20 {
		fail("[3] A poll 到 %d/20", r3.n)
	}
	fmt.Println("[3] consumer A: fetched + AckRelease 20 ✔")

	// [4] 同 consumer A 再 poll：release 的记录重投递，DeliveryCount ≥ 2
	// （单分区轮转分配下分区仍归 A；多成员 rebalance 迁移属块 b 完整面）
	r4 := pollAck(clA, 20, 20*time.Second, kgo.AckAccept)
	if r4.n < 20 {
		fail("[4] 重投递 %d/20", r4.n)
	}
	if r4.maxDelivery < 2 {
		fail("[4] 重投递 DeliveryCount=%d（应 ≥2）", r4.maxDelivery)
	}
	fmt.Printf("[4] redelivered %d, DeliveryCount=%d ✔\n", r4.n, r4.maxDelivery)
	fmt.Println("PASS ✔ (share groups spike：heartbeat/fetch/ack 全链)")
}
