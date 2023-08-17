package timewheel

import (
	"container/list"
	"sync"
	"time"
)

type TimeWheel struct {
	interval time.Duration
	slotnum int
	hand int 			// 指针
	ticker *time.Ticker
	slots []*list.List
	tasks map[string]*Location	// 保存列表元素位置

	closeCh chan struct{}
	addtaskCh chan *Task
	removetaskCh chan string
}

type Location struct {
	slotIndex int
	ele *list.Element
}

type Task struct {
	key string
	interval time.Duration // 延迟的时间
	pos int                // 在时间轮盘上的位置
	circle int             // 需要在时间轮盘上走的圈数
	job func()
}

var once sync.Once
var Tw *TimeWheel

func InitTimeWheel(interval time.Duration, slotnum int) *TimeWheel {
	once.Do(func() {
		Tw = &TimeWheel{
			interval: interval,
			slotnum: slotnum,
			hand: 0, 
			slots: make([]*list.List, slotnum),
			tasks: make(map[string]*Location),
		}
		for i := 0; i < slotnum; i++ {
			Tw.slots[i] = &list.List{}
		}
	})
	return Tw
}

func (Tw *TimeWheel) Start() {
	Tw.ticker = time.NewTicker(Tw.interval)
	Tw.closeCh = make(chan struct{})
	Tw.addtaskCh = make(chan *Task)
	Tw.removetaskCh = make(chan string)
	go Tw.run()
}

func (Tw *TimeWheel) Stop() {
	Tw.closeCh <- struct{}{}
}

func (Tw *TimeWheel) run() {
	for {
		select {
		case <-Tw.ticker.C:
			Tw.movehand()
		case task := <-Tw.addtaskCh:
			Tw.addTask(task)
		case key := <-Tw.removetaskCh:
			Tw.removeTask(key)
		case <-Tw.closeCh:
			Tw.ticker.Stop()
			return
		}	
	}
}

func (Tw *TimeWheel) movehand() {
	Tw.hand++
	if Tw.hand == Tw.slotnum {
		Tw.hand = 0
	}
	l := Tw.slots[Tw.hand]

	for item := l.Front(); item != nil; {
		task, _ := item.Value.(*Task)
		if task.circle > 0 {
			task.circle--
			item = item.Next()
			continue
		}

		task.job()
		delete(Tw.tasks, task.key)
		next := item.Next()
		l.Remove(item)
		item = next
	}
}


func (Tw *TimeWheel) addTask(task *Task) {
	delaySec := int(task.interval.Seconds())
	intervalSec := int(Tw.interval.Seconds())
	task.circle = delaySec / (intervalSec * Tw.slotnum)
	task.pos = (Tw.hand + delaySec/intervalSec) % Tw.slotnum
	
	if task.pos == Tw.hand {
		if task.circle > 0 {
			// 已经走过当前位置了,circle要减1
			task.circle--
		} else {
			// 最小延时时间为1个interval
			task.pos++
		}
	}
	
	ele := Tw.slots[task.pos].PushBack(task)
	Tw.tasks[task.key] = &Location{
		ele: ele,
		slotIndex: task.pos,
	}
}

func (Tw *TimeWheel) AddTask(key string, delay time.Duration, job func()) {
	if delay < 0 {
		return
	}
	Tw.addtaskCh <- &Task{
		key: key,
		interval: delay,
		job: job,
	}
}

func (Tw *TimeWheel) removeTask(key string) {
	v, ok := Tw.tasks[key]
	if !ok {
		return
	}
	Tw.slots[v.slotIndex].Remove(v.ele)
	delete(Tw.tasks, key)
}

func (Tw *TimeWheel) RemoveTask(key string) {
	if key == "" {
		return
	}
	Tw.removetaskCh <- key
}

func init() {
	InitTimeWheel(time.Second, 60)
	Tw.Start()
}