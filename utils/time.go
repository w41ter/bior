package utils

import "time"

// Timer provides cancelable tick.
type Timer struct {
	done chan chan struct{}
}

// Stop will cancel tick task, and blocking until task exit.
func (t *Timer) Stop() {
	callback := make(chan struct{}, 1)
	t.done <- callback
	<-callback
}

// StartTimer create a timer trigger per millis, and return a channel
// can close trigger and release it.
func StartTimer(millis int, f func(time.Time)) *Timer {
	timer := &Timer{
		done: make(chan chan struct{}, 1),
	}
	go func() {
		ticker := time.NewTicker(time.Duration(millis) * time.Millisecond)
		for {
			select {
			case now := <-ticker.C:
				f(now)
			case callback := <-timer.done:
				ticker.Stop()
				callback <- struct{}{}
				return
			}
		}
	}()
	return timer
}
