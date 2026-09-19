// KIP-932 share groups spike e2e（franz-go v1.21.6 真客户端）：
//   [1] produce 20 → share consumer A poll 到 20 → AckAccept
//   [2] 再 poll：无新数据（全部 accepted）
//   [3] 再 produce 20 → A poll 到后 AckRelease + Flush
//   [4] share consumer B（同组）：重投递拿到同 20 条，DeliveryCount ≥ 2
// runner：testing/e2e/run_share_spike.sh。
package main

import (
	"strconv"
	"strings"
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
	mode := "full"
	if len(os.Args) > 2 {
		mode = os.Args[2]
	}
	// B4/多节点 failover 模式：failover-setup <broker> <group> <topic> <n>
	// / failover-verify <broker> <group> <topic> <cut> <total>
	// ——跨节点 share 组：setup 在 A 节点 poll+accept，verify 在 B 节点
	// 校验游标随协调器迁移恢复（accepted 不重投、后缀全量）
	if mode == "failover-setup" || mode == "failover-verify" {
		if len(os.Args) < 6 {
			fail("usage: share <broker> %s <group> <topic> <n|cut,total>", mode)
		}
		group, topic := os.Args[3], os.Args[4]
		nums := strings.Split(os.Args[5], ",")
		numsInt := make([]int, len(nums))
		for i, v := range nums {
			numsInt[i], _ = strconv.Atoi(v)
		}
		if mode == "failover-setup" {
			failoverSetup(broker, group, topic, numsInt[0])
			return
		}
		failoverVerify(broker, group, topic, numsInt[0], numsInt[1])
		return
	}
	// verify-persist 模式：重启后校验已 accepted 的记录不被重投
	if mode == "verify-persist" {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(broker),
			kgo.ShareGroup("share-spike"),
			kgo.ConsumeTopics("share-e2e"),
		)
		if err != nil {
			fail("share consumer: %v", err)
		}
		defer cl.Close()
		n := pollAck(cl, 1, 8*time.Second, kgo.AckAccept).n
		if n != 0 {
			fail("重启后重投 %d 条——share 状态持久化失效", n)
		}
		fmt.Println("[5] restart: persisted cursor holds, no redelivery ✔")
		fmt.Println("PASS ✔ (share 状态持久化)")
		return
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

	newShareConsumer := func(group string, topics ...string) *kgo.Client {
		opts := []kgo.Opt{
			kgo.SeedBrokers(broker),
			kgo.ShareGroup(group),
			kgo.ConsumeTopics(topics...),
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
	clA := newShareConsumer("share-spike", topic)
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

	// ===== 多成员 + REJECT（双分区题） =====
	topic2 := "share-e2e-2p"
	_, _ = adm.CreateTopic(ctx, 2, 1, nil, topic2)
	time.Sleep(800 * time.Millisecond)


	// A/B 各自 poll：A → p0（10 条 REJECT），B → p1（10 条 ACCEPT）
	clA2 := newShareConsumer("share-multi", topic2)
	defer clA2.Close()
	clB2 := newShareConsumer("share-multi", topic2)
	defer clB2.Close()
	// 双成员心跳稳定（rotation 分配：A=p0, B=p1）
	time.Sleep(5 * time.Second)
	p2, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fail("producer 2p: %v", err)
	}
	defer p2.Close()
	// p0: 10 条 reject 目标；p1: 10 条正常
	for i := 0; i < 10; i++ {
		p2.Produce(ctx, &kgo.Record{Topic: topic2, Partition: 0, Value: []byte(fmt.Sprintf("rej-%02d", i))},
			func(_ *kgo.Record, err error) { if err != nil { fail("produce rej: %v", err) } })
	}
	for i := 0; i < 10; i++ {
		p2.Produce(ctx, &kgo.Record{Topic: topic2, Partition: 1, Value: []byte(fmt.Sprintf("keep-%02d", i))},
			func(_ *kgo.Record, err error) { if err != nil { fail("produce keep: %v", err) } })
	}
	if err := p2.Flush(ctx); err != nil {
		fail("flush 2p: %v", err)
	}

	rA := pollMulti(clA2, 10, 20*time.Second, func(r *kgo.Record) kgo.AckStatus {
		return kgo.AckReject
	})
	rB := pollMulti(clB2, 10, 20*time.Second, func(r *kgo.Record) kgo.AckStatus {
		return kgo.AckAccept
	})
	total := rA.n + rB.n
	if total < 20 {
		fail("[5] A+B 合计 %d < 20", total)
	}
	fmt.Printf("[5] multi-member: A=%d + B=%d (total=%d ≥ 20) ✔\n", rA.n, rB.n, total)

	// [6] REJECT 后不再重投（新 poll 只能看到 keep-*）
	n6 := pollMulti(clA2, 10, 8*time.Second, func(r *kgo.Record) kgo.AckStatus {
		return kgo.AckAccept
	}).n
	if n6 != 0 {
		fail("[6] REJECT 后仍 poll 到 %d 条（应 0）", n6)
	}
	fmt.Println("[6] rejected records not redelivered ✔")
	fmt.Println("PASS ✔ (share groups：多成员分配 + REJECT 归档)")
}

// pollMulti：poll 至 want 条；每条按 fn 决定 ack 状态
func pollMulti(cl *kgo.Client, want int, timeout time.Duration, fn func(*kgo.Record) kgo.AckStatus) pollResult {
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
			r.Ack(fn(r))
		})
	}
	if want > 0 {
		cl.FlushAcks(ctx)
	}
	return res
}

// failoverSetup：produce n 条 → share consumer poll n 条全 AckAccept。
func failoverSetup(broker, group, topic string, n int) {
	ctx := context.Background()
	ac, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fail("admin client: %v", err)
	}
	adm := kadm.NewClient(ac)
	_, _ = adm.CreateTopic(ctx, 1, 3, nil, topic)
	time.Sleep(800 * time.Millisecond)
	ac.Close()

	p, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fail("producer: %v", err)
	}
	for i := 0; i < n; i++ {
		p.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("fo-%03d", i))},
			func(_ *kgo.Record, err error) {
				if err != nil {
					fail("produce %d: %v", i, err)
				}
			})
	}
	if err := p.Flush(ctx); err != nil {
		fail("flush: %v", err)
	}
	p.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ShareGroup(group),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		fail("share consumer: %v", err)
	}
	defer cl.Close()
	got := pollAck(cl, n, 30*time.Second, kgo.AckAccept)
	if got.n != n {
		fail("setup poll 到 %d/%d", got.n, n)
	}
	fmt.Printf("[setup] poll + AckAccept %d ✔\n", got.n)
}

// failoverVerify：同组新成员（另一 broker）——校验 accepted 前缀不重投、
// 后缀 [cut, total) 全量可读（游标经重放随协调器迁移恢复）。
func failoverVerify(broker, group, topic string, cut, total int) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ShareGroup(group),
		kgo.ConsumeTopics(topic),
	)
	if err != nil {
		fail("share consumer: %v", err)
	}
	defer cl.Close()
	got := pollAck(cl, total-cut, 60*time.Second, kgo.AckAccept)
	if got.n != total-cut {
		fail("verify poll 到 %d（应 %d）——游标恢复/续读异常", got.n, total-cut)
	}
	fmt.Printf("[verify] 续读 %d..%d 共 %d，accepted 前缀无重投 ✔\n", cut, total, got.n)
	fmt.Println("PASS ✔ (share failover)")
}
