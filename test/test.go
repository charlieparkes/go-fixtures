package test

import (
	"fmt"
	"time"
)

func NewTimer() *Timer {
	t := &Timer{}
	t.Start()
	return t
}

type Timer struct {
	start      time.Time
	end        *time.Time
	splitStart *time.Time
	splits     []time.Duration
}

func (t *Timer) Start() {
	t.start = time.Now()
}

func (t *Timer) split(ts time.Time) {
	if t.splitStart == nil {
		t.splitStart = &t.start
	}
	t.splits = append(t.splits, time.Since(*t.splitStart))
	t.splitStart = &ts
}

func (t *Timer) Split() {
	t.split(time.Now())
}

func (t *Timer) PrintSplit(msg string) {
	t.Split()
	fmt.Printf("%v: %v\n", msg, t.splits[len(t.splits)-1])
}

func (t *Timer) Stop() time.Time {
	if t.end == nil {
		t.end = ref(time.Now())
		t.split(*t.end)
	}
	return *t.end
}

func (t *Timer) Duration() time.Duration {
	return t.start.Sub(t.Stop())
}

func ref(t time.Time) *time.Time {
	return &t
}
