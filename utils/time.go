package utils

import "time"

/**
 * StartTimer create a timer trigger per millis, and return a channel
 * can close trigger and release it.
 */
func StartTimer(millis int, f func(time.Time)) chan struct{} {
	done := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(time.Duration(millis) * time.Millisecond)
		for {
			select {
			case now := <-ticker.C:
				f(now)
			case <-done:
				ticker.Stop()
				return
			}
		}
	}()
	return done
}
