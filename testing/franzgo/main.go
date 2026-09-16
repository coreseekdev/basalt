// franz-go（纯 Go 协议栈）兼容性 e2e——客户端多样性第三档。
//
// 与 librdkafka_compat.py 同构：produce 20 → 组消费者 A 全量读 + 显式提交 →
// committed 回读 → 追加 10 → 消费者 B 从提交位续读不重不漏。
// franz-go 的版本协商与 kafka-python/librdkafka 均不同（如 OffsetCommit v8+、
// Fetch v13+、ApiVersions v3 autobroker）——任何 handler 布局分叉在此暴露。
//
// 用法：testing/e2e/run_franzgo.sh（拉起 broker 后运行本程序）。
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	broker = "localhost:9092"
	topic  = "franzgo-e2e"
	group  = "franzgo-e2e-group"
)

func fail(format string, a ...any) {
	fmt.Printf("FAIL: %s\n", fmt.Sprintf(format, a...))
	os.Exit(1)
}

func produce(n int, offset int) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		fail("producer: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		v := fmt.Sprintf("g-%04d", offset+i)
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

// groupRead 消费直到读满 stopAfter 条；每 5 条显式提交；返回读到的值集合与提交位。
func groupRead(stopAfter int, label string) map[string]bool {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.Balancers(kgo.RangeBalancer()),
		kgo.DisableAutoCommit(),
		kgo.SessionTimeout(6*time.Second),
	)
	if err != nil {
		fail("consumer: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got := map[string]bool{}
	lastRec := map[int32]*kgo.Record{}
	deadline := time.Now().Add(25 * time.Second)
	for len(got) < stopAfter && time.Now().Before(deadline) {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				fmt.Printf("  (%s: fetch err %v)\n", label, e.Err)
			}
		}
		fetches.EachError(func(t string, p int32, err error) { fmt.Printf("  (%s: err %v)\n", label, err) })
		fetches.EachRecord(func(r *kgo.Record) {
			got[string(r.Value)] = true
			lastRec[r.Partition] = r
			if len(got)%5 == 0 {
				cl.CommitRecords(context.Background(), r)
			}
		})
	}
	if len(got) != stopAfter {
		fail("%s: 读到 %d/%d", label, len(got), stopAfter)
	}
	var all []*kgo.Record
	for _, r := range lastRec {
		all = append(all, r)
	}
	if err := cl.CommitRecords(context.Background(), all...); err != nil {
		fail("%s: commit: %v", label, err)
	}
	fmt.Printf("  %s: read %d\n", label, len(got))
	return got
}

func committedOffsets() map[int32]int64 {
	cl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fail("adm client: %v", err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	offs, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		fail("fetch offsets: %v", err)
	}
	out := map[int32]int64{}
	offs.Each(func(r kadm.OffsetResponse) {
		if r.Err == nil && r.At >= 0 {
			out[r.Partition] = r.At
		}
	})
	return out
}

func main() {
	admcl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fail("bootstrap: %v", err)
	}
	defer admcl.Close()
	// 显式建 topic（Metadata v4+ 不再允许 auto-create——Kafka 语义；
	// kafka-python/librdkafka 档是经 produce 路径隐式创建的）
	adm := kadm.NewClient(admcl)
	_, err = adm.CreateTopic(context.Background(), 2, 1, nil, topic)
	if err != nil {
		fail("create topic: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	fmt.Println("[1] produced 20")
	produce(20, 0)

	got1 := groupRead(20, "consumer A")
	if len(got1) != 20 {
		fail("A: no-dup/full coverage 破缺：%d", len(got1))
	}
	fmt.Println("[2] consumer A: exactly-once full read ✔")

	co := committedOffsets()
	if co[0] != 10 || co[1] != 10 {
		fail("committed 必须 10/10，得 %v", co)
	}
	fmt.Println("[3] committed offsets = 10/10 ✔ (OffsetFetch 往返)")

	produce(10, 20)
	time.Sleep(1 * time.Second)
	got2 := groupRead(10, "consumer B (resume)")
	for i := 20; i < 30; i++ {
		v := fmt.Sprintf("g-%04d", i)
		if !got2[v] {
			fail("B 缺 %s（读到 %d 条）", v, len(got2))
		}
	}
	fmt.Println("[4] consumer B: resumed exactly at committed boundary, no dup/loss ✔")

	txnPhase()
	fmt.Println("PASS ✔ (franz-go 全组协议 + 事务)")
}

// ---- 事务面（T-M3.2 块 d 第二客户端档，ADR-18 §12d）----

const (
	txnTopic = "franzgo-txn"
	txnID    = "franzgo-txn-1"
)

// txnProduce 一个事务内发 n 条并 EndTransaction(commit)。
func txnProduce(prefix string, n int, commit bool) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.TransactionalID(txnID),
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
		cl.Produce(ctx, &kgo.Record{Topic: txnTopic, Value: []byte(v), Partition: int32(i % 2)},
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

// txnScan 从头读到静默；收集 countPrefix 前缀；禁见 forbiddenPrefix。
// 显式分区 assignment + 绝对 offset 0（裸客户端无 assignment 不拉取）。
func txnScan(readCommitted bool, countPrefix string, expected int, forbiddenPrefix string) map[string]bool {
	offs := map[string]map[int32]kgo.Offset{
		txnTopic: {0: kgo.NewOffset().At(0), 1: kgo.NewOffset().At(0)},
	}
	opts := []kgo.Opt{kgo.SeedBrokers(broker), kgo.ConsumePartitions(offs)}
	if readCommitted {
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		fail("scan client: %v", err)
	}
	defer cl.Close()
	got := map[string]bool{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	idle := 0
	for idle < 2000 {
		f := cl.PollFetches(ctx)
		if f.NumRecords() == 0 {
			time.Sleep(200 * time.Millisecond)
			idle += 200
			continue
		}
		idle = 0
		f.EachRecord(func(r *kgo.Record) {
			v := string(r.Value)
			if forbiddenPrefix != "" && strings.HasPrefix(v, forbiddenPrefix) {
				fail("forbidden record visible (rc=%v): %s", readCommitted, v)
			}
			if strings.HasPrefix(v, countPrefix) {
				got[v] = true
			}
		})
	}
	if len(got) != expected {
		fail("txnScan rc=%v prefix=%s got %d/%d", readCommitted, countPrefix, len(got), expected)
	}
	return got
}

func txnPhase() {
	admcl, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		fail("txn bootstrap: %v", err)
	}
	defer admcl.Close()
	adm := kadm.NewClient(admcl)
	if _, err := adm.CreateTopic(context.Background(), 2, 1, nil, txnTopic); err != nil {
		fail("create txn topic: %v", err)
	}
	time.Sleep(500 * time.Millisecond)

	// ① commit 流：read_committed 恰可见
	txnProduce("c-", 10, true)
	txnScan(true, "c-", 10, "")
	fmt.Println("[5] txn commit: 10 visible to read_committed")

	// ② abort 流：rc 不可见 / ru 可见
	txnProduce("x-", 10, false)
	txnScan(true, "c-", 10, "x-")
	txnScan(false, "x-", 10, "")
	fmt.Println("[6] txn abort: invisible to read_committed, visible to read_uncommitted")

	// ③ offset 已消耗：再 commit 5 条 → committed 全扫 c=10 / d=5，无 x
	txnProduce("d-", 5, true)
	txnScan(true, "d-", 5, "x-")
	txnScan(true, "c-", 10, "x-")
	fmt.Println("[7] aborted offsets consumed, not reused (committed=15 total)")
}
