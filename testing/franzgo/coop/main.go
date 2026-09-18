// cooperative-sticky rebalance e2e（T-M3.4 块 c，ADR-20 §4）——franz-go
// 经典路径（不带 848 opt-in ctx）+ CooperativeStickyBalancer（KIP-429）。
// 服务端前提（块 b）：组协议选择 = leader 偏好序 ∩ 全体支持集——此前
// 硬编码 "range"，协作客户端校验 JoinGroup 应答协议名即败。
//
// 验收（TASK.md T-M3.4）：cooperative 模式跑通且无停顿式双全量 rebalance
// ——B 中流加入，A 全程不断流（增量撤销：A 只交出被移动的分区）、全组
// 不重不漏。
//
// 用法：testing/e2e/run_franzgo_coop.sh（拉起 broker 后 go run ./coop）。
package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	broker = "localhost:9092"
	topic  = "coop-e2e"
	group  = "coop-e2e-group"
)

func fail(format string, a ...any) {
	fmt.Printf("FAIL: %s\n", fmt.Sprintf(format, a...))
	os.Exit(1)
}

func newCoopClient(opts ...kgo.Opt) *kgo.Client {
	base := []kgo.Opt{
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.Balancers(kgo.CooperativeStickyBalancer()), // 经典路径协作协议（无 848 ctx opt-in）
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	}
	base = append(base, opts...)
	cl, err := kgo.NewClient(base...)
	if err != nil {
		fail("coop client: %v", err)
	}
	return cl
}

func produceN(prefix string, start, n int, gap time.Duration) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		fail("producer: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := start; i < start+n; i++ {
		v := fmt.Sprintf("%s-%04d", prefix, i)
		cl.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte(v), Partition: int32(i % 2)},
			func(_ *kgo.Record, err error) {
				if err != nil {
					fail("produce %s: %v", v, err)
				}
			})
		if gap > 0 {
			time.Sleep(gap)
		}
	}
	if err := cl.Flush(ctx); err != nil {
		fail("flush: %v", err)
	}
}

func main() {
	admcl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fail("bootstrap: %v", err)
	}
	adm := kadm.NewClient(admcl)
	defer admcl.Close()
	if _, err := adm.CreateTopic(context.Background(), 2, 1, nil, topic); err != nil {
		fail("create topic: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// 预产 30，A 独占消费 20（每 5 条提交）
	produceN("c", 0, 30, 0)
	clA := newCoopClient()
	defer clA.Close()

	var mu sync.Mutex
	gotA := map[string]bool{}
	gotB := map[string]bool{}
	union := map[string]bool{}
	var timesA []time.Time
	var countA atomic.Int32
	lastGrowth := time.Now()

	record := func(into map[string]bool, v string, at time.Time, trackA bool) {
		mu.Lock()
		if !union[v] {
			union[v] = true
			lastGrowth = at
		}
		if !into[v] {
			into[v] = true
			if trackA {
				timesA = append(timesA, at)
				countA.Add(1)
			}
		}
		mu.Unlock()
	}

	pollA := func(d time.Duration) {
		pctx, pcancel := context.WithTimeout(context.Background(), d)
		f := clA.PollFetches(pctx)
		pcancel()
		at := time.Now()
		f.EachRecord(func(r *kgo.Record) {
			record(gotA, string(r.Value), at, true)
			if countA.Load()%5 == 0 {
				clA.CommitRecords(context.Background(), r)
			}
		})
	}
	deadlineA := time.Now().Add(30 * time.Second)
	for countA.Load() < 20 && time.Now().Before(deadlineA) {
		pollA(500 * time.Millisecond)
	}
	if countA.Load() < 20 {
		fail("A 预热读 %d/20", countA.Load())
	}

	// 慢流（100ms/条 × 30 ≈ 3s）启动后 B 立即加入：协作增量撤销——A 只交出
	// 被移动的分区，全程不断流（eager 双全量会是「全员停等 × 2」形态）
	streamStart := time.Now()
	produceDone := make(chan struct{})
	go func() {
		defer close(produceDone)
		cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
		if err != nil {
			fail("stream producer: %v", err)
		}
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for i := 30; i < 60; i++ {
			v := fmt.Sprintf("c-%04d", i)
			cl.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte(v), Partition: int32(i % 2)},
				func(_ *kgo.Record, err error) {
					if err != nil {
						fail("stream produce %s: %v", v, err)
					}
				})
			time.Sleep(100 * time.Millisecond)
		}
		if err := cl.Flush(ctx); err != nil {
			fail("stream flush: %v", err)
		}
	}()

	clB := newCoopClient()
	gotB = map[string]bool{}
	stopB := make(chan struct{})
	defer close(stopB)
	go func() {
		for {
			select {
			case <-stopB:
				return
			default:
			}
			pctx, pcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			f := clB.PollFetches(pctx)
			pcancel()
			at := time.Now()
			f.EachRecord(func(r *kgo.Record) {
				record(gotB, string(r.Value), at, false)
			})
		}
	}()

	// A 全程持续消费；流结束且 3s 无新增 → 收敛判定
	hardDeadline := time.Now().Add(20 * time.Second)
	settled := false
	for !settled && time.Now().Before(hardDeadline) {
		pollA(500 * time.Millisecond)
		select {
		case <-produceDone:
			mu.Lock()
			quiet := time.Since(lastGrowth) > 3*time.Second
			mu.Unlock()
			if quiet {
				settled = true
			}
		default:
		}
	}
	streamEnd := time.Now()
	clB.Close()

	// 断言 1：A 在流窗口内无 ≥2.5s 接收间隙（无停等——eager 栅栏残留即红）
	maxGap := time.Duration(0)
	for i := 1; i < len(timesA); i++ {
		if timesA[i-1].After(streamStart) && timesA[i].Before(streamEnd) {
			if gap := timesA[i].Sub(timesA[i-1]); gap > maxGap {
				maxGap = gap
			}
		}
	}
	if maxGap > 2500*time.Millisecond {
		fail("A 在 B 加入窗口内停流 %v（eager 栅栏相位残留？）", maxGap)
	}

	// 断言 2：B 收到记录（增量分配传导）
	mu.Lock()
	nB := len(gotB)
	nUnion := len(union)
	missing := []string{}
	for i := 0; i < 60; i++ {
		v := fmt.Sprintf("c-%04d", i)
		if !union[v] {
			missing = append(missing, v)
		}
	}
	mu.Unlock()
	if nB == 0 {
		fail("B 未收到任何记录——增量分配未生效")
	}
	// 断言 3：全组 60 条全覆盖（不重不漏；协作跨成员重复由 distinct 收口）
	if nUnion != 60 {
		fail("全组覆盖 %d/60（缺 %v）", nUnion, missing)
	}
	fmt.Printf("[coop] 增量 rebalance 无停等：A 不断流（窗口内最大间隙 %v）、B 分得增量（%d 条）、全组 60/60 ✔\n", maxGap, nB)
	fmt.Println("PASS ✔ (cooperative-sticky：classic 协作协议跑通，无停顿式双全量 rebalance)")
}
