// MIT License

// Copyright (c) 2018 Andy Pan

// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package ants

import (
	"sync"
	"sync/atomic"
	"time"
)

// Pool accept the tasks from client,it limits the total
// of goroutines to a given number by recycling goroutines.
type Pool struct {
	// capacity of the pool.
	// 最大容量阈值
	capacity int32

	// running is the number of the currently running goroutines.
	// 运行grt数量
	running int32

	// expiryDuration set the expired time (second) of every worker.
	// 有效期，单位s
	expiryDuration time.Duration

	// workers is a slice that store the available workers.
	// 可用workers
	workers []*Worker

	// release is used to notice the pool to closed itself.
	// 关闭通知 1关闭
	release int32

	// lock for synchronous operation.
	// 同步锁，同步控制workers
	lock sync.Mutex

	// cond for waiting to get a idle worker.
	// 获得空闲worker的条件变量
	cond *sync.Cond

	// once makes sure releasing this pool will just be done for one time.
	// 实例只执行一次操作
	once sync.Once

	// workerCache speeds up the obtainment of the an usable worker in function:retrieveWorker.
	// worker pool 注意初始化
	/*
		Get返回Pool中的任意一个对象。

	如果Pool为空，则调用New返回一个新创建的对象。

	如果没有设置New，则返回nil。

	还有一个重要的特性是，放进Pool中的对象，会在说不准什么时候被回收掉。

	所以如果事先Put进去100个对象，下次Get的时候发现Pool是空也是有可能的。

	不过这个特性的一个好处就在于不用担心Pool会一直增长，因为Go已经帮你在Pool中做了回收机制。

	这个清理过程是在每次垃圾回收之前做的。垃圾回收是固定两分钟触发一次。

	而且每次清理会将Pool中的所有对象都清理掉！
	*/
	workerCache sync.Pool

	// PanicHandler is used to handle panics from each worker goroutine.
	// if nil, panics will be thrown out again from worker goroutines.
	// 每个grt的异常处理
	PanicHandler func(interface{})
}

// clear expired workers periodically.
// 开启定时清理 worker 的grt
func (p *Pool) periodicallyPurge() {
	heartbeat := time.NewTicker(p.expiryDuration)
	defer heartbeat.Stop()

	for range heartbeat.C {
		currentTime := time.Now()
		p.lock.Lock()
		idleWorkers := p.workers
		if len(idleWorkers) == 0 && p.Running() == 0 && atomic.LoadInt32(&p.release) == 1 {
			p.lock.Unlock()
			return
		}
		n := -1
		for i, w := range idleWorkers {
			if currentTime.Sub(w.recycleTime) <= p.expiryDuration {
				break
			}
			n = i
			w.task <- nil
			idleWorkers[i] = nil
		}
		if n > -1 {
			if n >= len(idleWorkers)-1 {
				p.workers = idleWorkers[:0]
			} else {
				p.workers = idleWorkers[n+1:]
			}
		}
		p.lock.Unlock()
	}
}

// NewPool generates an instance of ants pool.
func NewPool(size int) (*Pool, error) {
	return NewTimingPool(size, DefaultCleanIntervalTime)
}

// NewTimingPool generates an instance of ants pool with a custom timed task.
func NewTimingPool(size, expiry int) (*Pool, error) {
	if size <= 0 {
		return nil, ErrInvalidPoolSize
	}
	if expiry <= 0 {
		return nil, ErrInvalidPoolExpiry
	}
	p := &Pool{
		capacity:       int32(size),
		expiryDuration: time.Duration(expiry) * time.Second,
	}
	// 初始化条件变量
	p.cond = sync.NewCond(&p.lock)
	//开启定时清理 worker 的grt
	go p.periodicallyPurge()
	return p, nil
}

//---------------------------------------------------------------------------

// Submit submits a task to this pool.
func (p *Pool) Submit(task func()) error {
	if 1 == atomic.LoadInt32(&p.release) {
		return ErrPoolClosed
	}
	// 获取worker 且传递task
	p.retrieveWorker().task <- task
	return nil
}

// Running returns the number of the currently running goroutines.
func (p *Pool) Running() int {
	return int(atomic.LoadInt32(&p.running))
}

// Free returns the available goroutines to work.
func (p *Pool) Free() int {
	return int(atomic.LoadInt32(&p.capacity) - atomic.LoadInt32(&p.running))
}

// Cap returns the capacity of this pool.
func (p *Pool) Cap() int {
	return int(atomic.LoadInt32(&p.capacity))
}

// Tune changes the capacity of this pool.
func (p *Pool) Tune(size int) {
	if size == p.Cap() {
		return
	}
	atomic.StoreInt32(&p.capacity, int32(size))
	diff := p.Running() - size
	for i := 0; i < diff; i++ {
		p.retrieveWorker().task <- nil
	}
}

// Release Closes this pool.
func (p *Pool) Release() error {
	p.once.Do(func() {
		atomic.StoreInt32(&p.release, 1)
		p.lock.Lock()
		idleWorkers := p.workers
		for i, w := range idleWorkers {
			w.task <- nil
			idleWorkers[i] = nil
		}
		p.workers = nil
		p.lock.Unlock()
	})
	return nil
}

//---------------------------------------------------------------------------

// incRunning increases the number of the currently running goroutines.
func (p *Pool) incRunning() {
	atomic.AddInt32(&p.running, 1)
}

// decRunning decreases the number of the currently running goroutines.
func (p *Pool) decRunning() {
	atomic.AddInt32(&p.running, -1)
}

// retrieveWorker returns a available worker to run the tasks.
// 获取worker
func (p *Pool) retrieveWorker() *Worker {
	var w *Worker

	//枷锁
	p.lock.Lock()
	idleWorkers := p.workers
	n := len(idleWorkers) - 1
	// 如果有空闲worker,取最后的
	if n >= 0 {
		w = idleWorkers[n]
		idleWorkers[n] = nil
		p.workers = idleWorkers[:n]
		p.lock.Unlock()
		// 注意，空闲队列的worker是running状态，所以不需要run
	} else if p.Running() < p.Cap() {
		// 没有空闲，且运行grt < 最大数量阈值
		// 则pool 创建
		p.lock.Unlock()
		if cacheWorker := p.workerCache.Get(); cacheWorker != nil {
			w = cacheWorker.(*Worker)
		} else {
			w = &Worker{
				pool: p,
				task: make(chan func(), workerChanCap),
			}
		}
		// 首次创建、或者pool 获取的worker 需要运行;
		w.run()
	} else {
		// 否则，等待条件变量通知
		for {
			// 阻塞等待
			p.cond.Wait()
			// 如果没有空闲，则继续等待（已经加锁）
			l := len(p.workers) - 1
			if l < 0 {
				continue
			}
			// 取出空闲worker
			w = p.workers[l]
			p.workers[l] = nil
			p.workers = p.workers[:l]
			break
		}
		p.lock.Unlock()
	}
	return w
}

// revertWorker puts a worker back into free pool, recycling the goroutines.
func (p *Pool) revertWorker(worker *Worker) {
	worker.recycleTime = time.Now()
	p.lock.Lock()
	p.workers = append(p.workers, worker)
	// Notify the invoker stuck in 'retrieveWorker()' of there is an available worker in the worker queue.
	// 通知等待请求，新worker 产生
	p.cond.Signal()
	p.lock.Unlock()
}
