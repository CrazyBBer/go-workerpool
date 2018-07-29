package workerpool

import (
	"runtime"
	"strings"
	"sync"
	"time"
)

//Action ..
type Action interface {
	Process() error
}

// workerPool serves incoming connections via a pool of workers
// in FILO order, i.e. the most recently stopped worker will serve the next
// incoming connection.
//
// Such a scheme keeps CPU caches hot (in theory).
type workerPool struct {
	MaxWorkersCount int

	LogAllErrors bool

	MaxIdleWorkerDuration time.Duration

	lock         sync.Mutex
	workersCount int
	mustStop     bool

	ready []*workerChan

	stopCh chan struct{}

	workerChanPool sync.Pool
}

type workerChan struct {
	lastUseTime time.Time
	act         chan Action
}

func (wp *workerPool) Start() {
	if wp.stopCh != nil {
		panic("BUG: workerPool already started")
	}
	wp.stopCh = make(chan struct{})
	stopCh := wp.stopCh
	log.Debugf(nil, "workerPool stop chan created at %p", stopCh)
	go func() {
		var scratch []*workerChan
		for {
			log.Debugf(nil, "default scratch len：%d address is %p", len(scratch), &scratch)
			wp.clean(&scratch)
			select {
			case <-stopCh:
				log.Debugf(nil, "workerpool singnal to stop received from %p", stopCh)
				return
			default:
				log.Debugf(nil, "watch dog will sleep %d Sec", wp.getMaxIdleWorkerDuration()/time.Second)
				time.Sleep(wp.getMaxIdleWorkerDuration())
			}
		}
	}()
}

func (wp *workerPool) Stop() {
	if wp.stopCh == nil {
		panic("BUG: workerPool wasn't started")
	}
	log.Debugf(nil, "notice workerPool %p to exit", &wp.stopCh)
	close(wp.stopCh)
	wp.stopCh = nil

	// Stop all the workers waiting for incoming connections.
	// Do not wait for busy workers - they will stop after
	// serving the connection and noticing wp.mustStop = true.
	wp.lock.Lock()
	ready := wp.ready
	for i, ch := range ready {
		ch.act <- nil
		log.Debugf(nil, "notice action channel %p to exit", &ch.act)
		ready[i] = nil
	}
	wp.ready = ready[:0]
	if wp.ready == nil {
		log.Debugf(nil, "current wp.ready is empty at %p", &wp.ready)
	}

	wp.mustStop = true
	wp.lock.Unlock()
}

func (wp *workerPool) getMaxIdleWorkerDuration() time.Duration {

	if wp.MaxIdleWorkerDuration <= 0 {
		return 10 * time.Second
	}
	return wp.MaxIdleWorkerDuration
}

func (wp *workerPool) clean(scratch *[]*workerChan) {
	maxIdleWorkerDuration := wp.getMaxIdleWorkerDuration()

	log.Debugf(nil, "prepre cleaning timeout worker :maxIdleWorkerDuration is %d Sec", maxIdleWorkerDuration/time.Second)
	// Clean least recently used workers if they didn't serve connections
	// for more than maxIdleWorkerDuration.
	currentTime := time.Now()

	wp.lock.Lock()
	ready := wp.ready
	n := len(ready)
	log.Debugf(nil, "current ready worker count: %d", n)
	i := 0
	for i < n && currentTime.Sub(ready[i].lastUseTime) > maxIdleWorkerDuration {
		i++
	}
	log.Debugf(nil, "current scratch address is : %p", scratch)

	if header := (*scratch)[:0]; header == nil {
		log.Debugf(nil, "current scratch header is nil at address %p", &header)
	} else {
		log.Debugf(nil, "current scratch header is not empty at address %p", &header)
	}

	if end := ready[:i]; end == nil {
		log.Debugf(nil, "current scratch end is nil at address %p", &end)
	} else {
		log.Debugf(nil, "current scratch end is not empty at address %p, len is %d", &end, len(end))
	}
	*scratch = append((*scratch)[:0], ready[:i]...)
	if i > 0 {
		m := copy(ready, ready[i:])
		for i = m; i < n; i++ {
			ready[i] = nil
		}
		wp.ready = ready[:m]
	}
	wp.lock.Unlock()

	// Notify obsolete workers to stop.
	// This notification must be outside the wp.lock, since ch.ch
	// may be blocking and may consume a lot of time if many workers
	// are located on non-local CPUs.
	tmp := *scratch
	log.Debugf(nil, "current tmp worker scratched to exit len is %d", len(tmp))
	for i, ch := range tmp {
		log.Debugf(nil, "ch index %d was noticed to exit,last use %s", i, ch.lastUseTime)
		ch.act <- nil
		tmp[i] = nil
	}
}

func (wp *workerPool) Serve(act Action) bool {
	ch := wp.getCh()
	if ch == nil {
		return false
	}
	ch.act <- act
	return true
}

var workerChanCap = func() int {
	// Use blocking workerChan if GOMAXPROCS=1.
	// This immediately switches Serve to WorkerFunc, which results
	// in higher performance (under go1.5 at least).
	if runtime.GOMAXPROCS(0) == 1 {
		return 0
	}

	// Use non-blocking workerChan if GOMAXPROCS>1,
	// since otherwise the Serve caller (Acceptor) may lag accepting
	// new connections if WorkerFunc is CPU-bound.
	return 1
}()

func (wp *workerPool) getCh() *workerChan {
	var ch *workerChan
	createWorker := false

	wp.lock.Lock()
	ready := wp.ready
	n := len(ready) - 1
	log.Debugf(nil, "ready worker queue len: %d,plan to fech index %d", len(ready), n)
	if n < 0 {
		if wp.workersCount < wp.MaxWorkersCount {
			log.Debugf(nil, "current workersCount %d  MaxWorkersCount %d", wp.workersCount, wp.MaxWorkersCount)
			createWorker = true
			wp.workersCount++
			log.Debugf(nil, "the workersCount %d will be created", wp.workersCount)
		} else {
			log.Debugf(nil, "new work got,but max worker level reached, current workersCount %d, we will create new", wp.workersCount)
			createWorker = true
			wp.workersCount++
		}
	} else {
		ch = ready[n]
		log.Debugf(nil, "got a workchan at %p", ch)
		ready[n] = nil
		wp.ready = ready[:n] //shirink the ready slice
	}
	wp.lock.Unlock()

	if ch == nil {
		if !createWorker {
			log.Debugf(nil, "some critical error happened for workerChan at %p", &ch)
			return nil
		}
		//retrieve the hot pool
		log.Debugf(nil, "retrieve the hot pool for workerChan at %p", &wp.workerChanPool)
		vch := wp.workerChanPool.Get()
		if vch == nil {
			vch = &workerChan{
				act: make(chan Action, workerChanCap),
			}
			log.Debugf(nil, "retrieve pool , but got nothing, will created one at %p", vch)
		}
		ch = vch.(*workerChan)
		log.Debugf(nil, "target workChan(cast) at %p", ch)
	}
	go func() {
		log.Debugf(nil, "new goroutine created for %p", ch)
		wp.workerFunc(ch)
		wp.workerChanPool.Put(ch)
		log.Debugf(nil, "work done,put workChan %p into hot pool", ch)
	}()
	// got the workChan

	return ch
}

func (wp *workerPool) release(ch *workerChan) bool {
	ch.lastUseTime = CoarseTimeNow()
	wp.lock.Lock()
	if wp.mustStop {
		wp.lock.Unlock()
		return false
	}
	log.Debugf(nil, "put workChan %p into ready queue", ch)
	wp.ready = append(wp.ready, ch)
	wp.lock.Unlock()
	return true
}

func (wp *workerPool) workerFunc(ch *workerChan) {

	for act := range ch.act {
		if act == nil {
			break
		}
		if err := act.Process(); err != nil {
			if wp.LogAllErrors || !(strings.Contains(err.Error(), "wanted error")) {
				log.Errorf(nil, "error when handling work: %s", err)
			}
		}
		act = nil

		if !wp.release(ch) {
			break
		}
	}

	wp.lock.Lock()
	wp.workersCount--
	wp.lock.Unlock()
}
