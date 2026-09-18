// T-M4.1 SASL/TLS 客户端矩阵——franz-go 档（SCRAM-SHA-256）。
//
// 用法：sasl <port> <mode>
//
//	plain   SASL_PLAINTEXT：SCRAM produce 20 + consume 20
//	ssl     SASL_SSL：SCRAM + TLS（CA 校验，BASALT_TLS_CA 指向证书 PEM）
//	negpw   错误口令 → 认证失败（produce 必须报错）
//
// runner：testing/e2e/run_sasl_matrix.sh。
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func fail(format string, a ...any) {
	fmt.Printf("FAIL: %s\n", fmt.Sprintf(format, a...))
	os.Exit(1)
}

func main() {
	if len(os.Args) < 3 {
		fail("usage: sasl <port> <plain|ssl|negpw>")
	}
	port, _ := strconv.Atoi(os.Args[1])
	mode := os.Args[2]
	broker := fmt.Sprintf("localhost:%d", port)
	topic := "sasl-matrix-e2e"

	auth := scram.Auth{User: "app", Pass: "apppass"}
	if mode == "negpw" {
		auth = scram.Auth{User: "app", Pass: "WRONG-password"}
	}
	mech := auth.AsSha256Mechanism()

	opts := []kgo.Opt{kgo.SeedBrokers(broker), kgo.SASL(mech),
		kgo.RecordPartitioner(kgo.ManualPartitioner())}
	if mode == "ssl" {
		caPEM, err := os.ReadFile(os.Getenv("BASALT_TLS_CA"))
		if err != nil {
			fail("read CA: %v", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			fail("CA PEM 解析失败")
		}
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			RootCAs:    pool,
			ServerName: "localhost",
			MinVersion: tls.VersionTLS12,
		}))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		fail("client: %v", err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if mode == "negpw" {
		// 错误口令：produce 必须以 SASL/认证类错误收场（无投递成功）
		errs := 0
		acks := 0
		for i := 0; i < 5; i++ {
			cl.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte("must-fail")},
				func(_ *kgo.Record, err error) {
					if err != nil {
						errs++
					} else {
						acks++
					}
				})
		}
		if err := cl.Flush(ctx); err != nil {
			errs++ // flush 超时也算未投递
		}
		if acks > 0 {
			fail("错误口令竟投递 %d 条——认证门禁失效", acks)
		}
		if errs == 0 {
			fail("错误口令无任何错误回报（timeout 窗口内既未投递也未报错）")
		}
		fmt.Println("[franz-go negpw] auth rejected ✔")
		return
	}

	// plain / ssl：建题 → produce 20 → consume 20 不重不漏
	adm := kadm.NewClient(cl)
	_, _ = adm.CreateTopic(ctx, 2, 1, nil, topic) // 已存在可容忍（矩阵各档共享 broker）
	time.Sleep(300 * time.Millisecond)

	for i := 0; i < 20; i++ {
		cl.Produce(ctx, &kgo.Record{Topic: topic, Partition: int32(i % 2),
			Value: []byte(fmt.Sprintf("fg-%s-%02d", mode, i))},
			func(_ *kgo.Record, err error) {
				if err != nil {
					fail("produce: %v", err)
				}
			})
	}
	if err := cl.Flush(ctx); err != nil {
		fail("flush: %v", err)
	}

	want := fmt.Sprintf("fg-%s-", mode)
	// 裸分区消费（独立 context——produce 阶段的 30s ctx 可能已耗尽）
	cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer ccancel()
	offs := map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart(), 1: kgo.NewOffset().AtStart()}}
	ccl, err := kgo.NewClient(append(opts[:len(opts):len(opts)],
		kgo.ConsumePartitions(offs),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, nil)))...)
	if err != nil {
		fail("consumer: %v", err)
	}
	defer ccl.Close()
	got := 0
	total := 0
	deadline := time.Now().Add(25 * time.Second)
	for got < 20 && time.Now().Before(deadline) {
		fs := ccl.PollRecords(cctx, 100)
		fs.EachError(func(_ string, _ int32, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				fail("fetch: %v", err)
			}
		})
		fs.EachRecord(func(r *kgo.Record) {
			total++
			if len(r.Value) >= len(want) && string(r.Value[:len(want)]) == want {
				got++
			}
		})
		if fs.NumRecords() == 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}
	_ = total
	if got != 20 {
		fail("%s: consume %d/20", mode, got)
	}
	fmt.Printf("[franz-go %s] produce+consume 20 via SCRAM ✔\n", mode)
}
