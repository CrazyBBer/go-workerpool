package utils

import (
	"math/rand"
	"runtime"
	"testing"
	"time"
  
)

func init() {
	log.SetLevel(5)
}

var rnd = func() *rand.Rand {
	return rand.New(rand.NewSource(time.Now().UnixNano()))
}()

type mockActionObject struct {
	Order int
}

func (m *mockActionObject) Process() error {

	timeToSleep := rnd.Intn(10)
	time.Sleep(time.Millisecond * time.Duration(timeToSleep))
	return nil
}

func TestWorkerPoolStartStopSerial(t *testing.T) {
	testWorkerPoolStartStop(t)
}

func TestWorkerPoolStartStopConcurrent(t *testing.T) {
	concurrency := 10
	ch := make(chan struct{ Order int }, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(i int) {
			testWorkerPoolStartStop(t)
			ch <- struct{ Order int }{Order: i}
		}(i)
	}
	for i := 0; i < concurrency; i++ {
		select {
		case value := <-ch:
			t.Logf("finsih signal %d got from order %d", value.Order, i)
		case <-time.After(time.Second):
			t.Fatalf("timeout: %d", i)
		}
	}
}

func testWorkerPoolStartStop(t *testing.T) {
	logicCPUCount := runtime.NumCPU()
	wp := &workerPool{
		MaxWorkersCount: logicCPUCount,
	}
	t.Logf("MaxWorkersCount %d,CPU Core is %d", logicCPUCount, runtime.GOMAXPROCS(0))
	for i := 0; i < 10; i++ {
		wp.Start()
		wp.Stop()
	}
}

func TestWorkerPoolMaxWorkersCountConcurrent(t *testing.T) {
	concurrency := 4
	ch := make(chan struct{ Order int }, concurrency)
	for i := 0; i < concurrency; i++ {
		go func(order, i int) {
			TestWorkerPoolMaxWorkersCountMulti(t)
			ch <- struct{ Order int }{Order: i}
			t.Logf("order %d concurrency %d start", order, i)
		}(i, i)
	}
	for i := 0; i < concurrency; i++ {
		select {
		case msg := <-ch:
			t.Logf("finsih signal %d got from order %d", msg.Order, i)
		case <-time.After(time.Second):
			t.Fatalf("timeout")
		}
	}
}

func TestWorkerPoolMaxWorkersCountMulti(t *testing.T) {
	for i := 0; i < 5; i++ {
		TestWorkerPoolMaxWorkersCount(t)
	}
}

func TestWorkerPoolMaxWorkersCount(t *testing.T) {

	logicCPUCount := runtime.NumCPU()
	wp := &workerPool{
		MaxWorkersCount: logicCPUCount,
	}
	t.Logf("MaxWorkersCount %d,CPU Core is %d", logicCPUCount, runtime.GOMAXPROCS(0))
	wp.Start()

	for i := 0; i < wp.MaxWorkersCount; i++ {
		mb := &mockActionObject{Order: i}
		if !wp.Serve(mb) {
			t.Fatalf("worker pool must have enough workers to serve Action")
		} else {
			t.Logf("worker start to handle function proc %d", mb.Order)
		}
	}

	for i := 0; i < 5; i++ {
		mb := &mockActionObject{Order: i}
		if !wp.Serve(mb) {
			t.Fatalf("worker pool must be full when do new work %d", mb.Order)
		} else {
			t.Logf("worker start to handle function new proc %d", mb.Order)
		}
	}

	wp.Stop()
}

func TestMutilProducerAndOneConsumer(t *testing.T) {
	logicCPUCount := runtime.NumCPU()
	wp := &workerPool{
		MaxWorkersCount: logicCPUCount,
	}
	t.Logf("MaxWorkersCount %d,CPU Core is %d", logicCPUCount, runtime.GOMAXPROCS(0))
	wp.Start()

	subCh := make(chan struct{ Order int }, wp.MaxWorkersCount)
	for i := 0; i < wp.MaxWorkersCount; i++ {
		go func(i int) {
			t.Logf("order %d created & produced", i)
			subCh <- struct{ Order int }{Order: i}
		}(i)
	}

	for i := 0; i < wp.MaxWorkersCount; i++ {
		select {
		case msg := <-subCh:
			t.Logf("order %d got & consumerd", msg.Order)
		case <-time.After(time.Second):
			t.Fatalf("timeout")
		}
	}
	wp.Stop()
}
