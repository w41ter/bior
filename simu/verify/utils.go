package verify

import "time"

func sleep(millisecond int) {
	time.Sleep(time.Duration(millisecond) * time.Millisecond)
}
