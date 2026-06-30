package clock

import "time"

type Clock interface {
	Now() time.Time
}

type Scheduler interface {
	Clock
	Since(t time.Time) time.Duration
	After(d time.Duration) <-chan time.Time
	NewTicker(d time.Duration) Ticker
	Sleep(d time.Duration)
}

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type System struct{}

func (System) Now() time.Time {
	return time.Now()
}

func (System) Since(t time.Time) time.Duration {
	return time.Since(t)
}

func (System) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (System) NewTicker(d time.Duration) Ticker {
	return realTicker{ticker: time.NewTicker(d)}
}

func (System) Sleep(d time.Duration) {
	time.Sleep(d)
}

type Func func() time.Time

func (f Func) Now() time.Time {
	if f == nil {
		return time.Now()
	}
	return f()
}

type realTicker struct {
	ticker *time.Ticker
}

func (r realTicker) C() <-chan time.Time {
	return r.ticker.C
}

func (r realTicker) Stop() {
	r.ticker.Stop()
}
