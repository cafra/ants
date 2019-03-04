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
	"log"
	"time"
)

// Worker is the actual executor who runs the tasks,
// it starts a goroutine that accepts tasks and
// performs function calls.
type Worker struct {
	// pool who owns this worker.
	// 所属的pool
	pool *Pool

	// task is a job should be done.
	// task job
	task chan func()

	// recycleTime will be update when putting a worker back into queue.
	// 返回队列时间
	recycleTime time.Time
}

// run starts a goroutine to repeat the process
// that performs the function calls.
func (w *Worker) run() {
	// 增加pool run 计数
	w.pool.incRunning()
	// 启动grt
	go func() {
		defer func() {
			if p := recover(); p != nil {
				// 如果发生panic
				w.pool.decRunning()
				if w.pool.PanicHandler != nil {
					// 异常处理
					w.pool.PanicHandler(p)
				} else {
					log.Printf("worker exits from a panic: %v", p)
				}
			}
		}()

		for f := range w.task {
			if f == nil {
				// 执行结束
				// dec 计数
				w.pool.decRunning()
				// pool put 回收
				w.pool.workerCache.Put(w)
				return
			}
			// 收到job，执行
			f()
			// 归还worker，放在slice 尾部
			w.pool.revertWorker(w)
		}
	}()
}
