package fixtures

import (
    "fmt"
    "time"
)

func newTimer() *timer {
    t := &timer{}
    t.Start()
    return t
}

type timer struct {
    start      time.Time
    end        *time.Time
    splitStart *time.Time
    splits     []time.Duration
}

func (t *timer) Start() {
    t.start = time.Now()
}

func (t *timer) split(ts time.Time) {
    if t.splitStart == nil {
        t.splitStart = &t.start
    }
    t.splits = append(t.splits, time.Since(*t.splitStart))
    t.splitStart = &ts
}

func (t *timer) Split() {
    t.split(time.Now())
}

func (t *timer) PrintSplit(msg string) {
    t.Split()
    fmt.Printf("%v: %v\n", msg, t.splits[len(t.splits)-1])
}

func (t *timer) Stop() time.Time {
    if t.end == nil {
        t.end = ref(time.Now())
        t.split(*t.end)
    }
    return *t.end
}

func (t *timer) Duration() time.Duration {
    return t.start.Sub(t.Stop())
}

func ref(t time.Time) *time.Time {
    return &t
}
