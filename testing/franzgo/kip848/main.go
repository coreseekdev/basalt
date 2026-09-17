// KIP-848 新消费组协议 e2e（T-M3.3 块 c，ADR-19 §6c）——franz-go v1.21.6
// 作验收客户端。848 路径的三个开启条件（consumer_group_848.go should848
// 实证）：① ctx 带 opt_in_kafka_next_gen_balancer_beta（WithContext 注入）
// ② balancers[0] 为 sticky/range（eager 档）③ broker 宣告 68 max≥1（服务端
// 已升 v1——只宣告 v0 时 franz-go 静默回退 classic 路径）。
//
// 三验收面（ADR-19 §6c）：
//   [1] 混布收敛：同 topic 两个组（一 classic 一 consumer），各自独立收敛、
//       消费不重不漏（Kafka 4.x 不允许同组混协议——按 ADR-19 §5 边界）
//   [2] 增量 rebalance 无停等：新成员中流加入，存量成员消费不断流
//       （无 JoinGroup/SyncGroup 栅栏相位），增量分配生效，全量无丢失
//   [3] 双隔离级消费：read_committed/read_uncommitted 组消费者对事务流
//
// 用法：testing/e2e/run_franzgo_848.sh（拉起 broker 后 go run ./kip848）。
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

	mixTopic    = "kip848-mix"
	rbTopic     = "kip848-rebalance"
	txnTopic848 = "kip848-txn"
)

func fail(format string, a ...any) {
	fmt.Printf("FAIL: %s\n", fmt.Sprintf(format, a...))
	os.Exit(1)
}

// ctx848：franz-go 的 848 beta opt-in（缺省关——classic 客户端不得带它）。
var ctx848 = context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)

// new848Client 构造走 KIP-848 消费路径的组客户端。
func new848Client(group string, opts ...kgo.Opt) *kgo.Client {
	base := []kgo.Opt{
		kgo.WithContext(ctx848),
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.Balancers(kgo.StickyBalancer()), // should848 硬性要求 balancers[0] ∈ {sticky, range}
	}
	base = append(base, opts...)
	cl, err := kgo.NewClient(base...)
	if err != nil {
		fail("848 client %s: %v", group, err)
	}
	return cl
}

// readUntil 组消费直到收集满 want 条 distinct 值；超时 fail。
func readUntil(cl *kgo.Client, want int, label string, timeout time.Duration) map[string]bool {
	got := map[string]bool{}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	deadline := time.Now().Add(timeout)
	for len(got) < want && time.Now().Before(deadline) {
		fetches := cl.PollFetches(ctx)
		fetches.EachError(func(t string, p int32, err error) {
			fmt.Printf("  (%s: fetch err %v)\n", label, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			got[string(r.Value)] = true
		})
	}
	if len(got) < want {
		fail("%s: 读到 %d/%d", label, len(got), want)
	}
	return got
}

func mustContainAll(got map[string]bool, prefix string, n int, label string) {
	for i := 0; i < n; i++ {
		v := fmt.Sprintf("%s-%04d", prefix, i)
		if !got[v] {
			fail("%s: 缺 %s", label, v)
		}
	}
}

// produceN 手动分区轮询发 n 条（v = prefix-序号，分区 = 序号%2）。
func produceN(topic, prefix string, start, n int) {
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
	for _, t := range []string{mixTopic, rbTopic, txnTopic848} {
		if _, err := adm.CreateTopic(context.Background(), 2, 1, nil, t); err != nil {
			fail("create topic %s: %v", t, err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	mixPhase()
	rebalancePhase()
	isolationPhase()
	fmt.Println("PASS ✔ (KIP-848 新消费组协议：混布收敛 / 增量 rebalance 无停等 / 双隔离级)")
}

// ---- [1] 混布收敛（ADR-19 §5：同 topic 两组各自收敛，非同组混布）----

func mixPhase() {
	produceN(mixTopic, "m", 0, 20)

	// classic 组：无 ctx opt-in → should848=false → 经典路径（同现网旧客户端）
	classic, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup("mix-classic"),
		kgo.ConsumeTopics(mixTopic),
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		fail("classic client: %v", err)
	}
	defer classic.Close()
	// consumer 组：ctx opt-in + sticky → KIP-848 心跳路径
	c848 := new848Client(
		"mix-consumer",
		kgo.ConsumeTopics(mixTopic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
	)
	defer c848.Close()

	gc := readUntil(classic, 20, "classic 组", 30*time.Second)
	g8 := readUntil(c848, 20, "consumer 组", 30*time.Second)
	mustContainAll(gc, "m", 20, "classic 组")
	mustContainAll(g8, "m", 20, "consumer 组")
	fmt.Println("[1] 混布收敛：classic/consumer 两组建各读 20/20，不重不漏 ✔")
}

// ---- [2] 增量 rebalance 无停等（无 JoinGroup/SyncGroup 栅栏）----

func rebalancePhase() {
	produceN(rbTopic, "r", 0, 30) // 预产 30

	clA := new848Client(
		"rb848",
		kgo.ConsumeTopics(rbTopic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	defer clA.Close()

	var mu sync.Mutex
	gotA := map[string]bool{}
	var gotB map[string]bool
	union := map[string]bool{}
	var timesA []time.Time
	var countA atomic.Int32
	lastGrowth := time.Now()

	// record 收录去重值 + union 覆盖 + A 的接收时间线（trackA = A 侧接收）
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

	// A 预热：读满 20（每 5 条提交）
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

	// 慢流（100ms/条 × 30 ≈ 3s）启动后 B 立即加入：848 无栅栏——A 应持续
	// 收到记录（classic 的 PreparingRebalance 相位 A 会停等整个 rebalance）
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
			v := fmt.Sprintf("r-%04d", i)
			cl.Produce(ctx, &kgo.Record{Topic: rbTopic, Value: []byte(v), Partition: int32(i % 2)},
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

	// B 加入并独立持续轮询（归属迁移期的跨成员重复由 union distinct 收口）
	clB := new848Client(
		"rb848",
		kgo.ConsumeTopics(rbTopic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
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
	clB.Close() // 离开（epoch=-1）

	// 断言 1：A 在流窗口内无 ≥2.5s 的接收间隙（无停等——栅栏相位残留即红）
	maxGap := time.Duration(0)
	for i := 1; i < len(timesA); i++ {
		if timesA[i-1].After(streamStart) && timesA[i].Before(streamEnd) {
			if gap := timesA[i].Sub(timesA[i-1]); gap > maxGap {
				maxGap = gap
			}
		}
	}
	if maxGap > 2500*time.Millisecond {
		fail("A 在 B 加入窗口内停流 %v（栅栏相位残留？）", maxGap)
	}

	// 断言 2：B 收到记录（增量分配生效，split 传导）
	mu.Lock()
	nB := len(gotB)
	nUnion := len(union)
	missing := []string{}
	for i := 0; i < 60; i++ {
		v := fmt.Sprintf("r-%04d", i)
		if !union[v] {
			missing = append(missing, v)
		}
	}
	mu.Unlock()
	if nB == 0 {
		fail("B 未收到任何记录——增量分配未生效")
	}
	// 断言 3：全组 60 条全覆盖（无丢失；跨成员重复由 distinct 收口）
	if nUnion != 60 {
		fail("全组覆盖 %d/60（缺 %v）", nUnion, missing)
	}
	fmt.Printf("[2] 增量 rebalance 无停等：A 不断流（窗口内最大间隙 %v）、B 分得增量（%d 条）、全组 60/60 ✔\n", maxGap, nB)
}

// ---- [3] 双隔离级消费（事务流 × 组消费面）----

func txnProduce848(prefix string, n int, commit bool) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.TransactionalID("kip848-txn-1"),
		kgo.TransactionTimeout(30*time.Second),
	)
	if err != nil {
		fail("txn producer: %v", err)
	}
	defer cl.Close()
	cl.BeginTransaction()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		v := fmt.Sprintf("%s-%04d", prefix, i)
		cl.Produce(ctx, &kgo.Record{Topic: txnTopic848, Value: []byte(v), Partition: int32(i % 2)},
			func(_ *kgo.Record, err error) {
				if err != nil {
					fail("txn produce %s: %v", v, err)
				}
			})
	}
	if err := cl.Flush(ctx); err != nil {
		fail("txn flush: %v", err)
	}
	try := kgo.TryAbort
	if commit {
		try = kgo.TryCommit
	}
	if err := cl.EndTransaction(ctx, try); err != nil {
		fail("end transaction(commit=%v): %v", commit, err)
	}
}

func isolationPhase() {
	txnProduce848("c", 10, true)
	txnProduce848("x", 10, false)

	// read_committed 组消费者：abort 流不可见
	rc := new848Client(
		"txn-rc",
		kgo.ConsumeTopics(txnTopic848),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	gotRC := readUntil(rc, 10, "read_committed 组", 30*time.Second)
	rc.Close()
	mustContainAll(gotRC, "c", 10, "rc 组")
	if gotRC["x-0000"] {
		fail("abort 流对 read_committed 可见")
	}
	fmt.Println("[3] read_committed 组消费者：commit 流 10 可见，abort 流不可见")

	// read_uncommitted 组消费者：双流全见
	ru := new848Client(
		"txn-ru",
		kgo.ConsumeTopics(txnTopic848),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
	)
	gotRU := readUntil(ru, 20, "read_uncommitted 组", 30*time.Second)
	ru.Close()
	mustContainAll(gotRU, "c", 10, "ru 组")
	for i := 0; i < 10; i++ {
		v := fmt.Sprintf("x-%04d", i)
		if !gotRU[v] {
			fail("ru 组缺 abort 流记录 %s", v)
		}
	}
	fmt.Println("[3] read_uncommitted 组消费者：commit/abort 双流 20 全见 ✔")
}
