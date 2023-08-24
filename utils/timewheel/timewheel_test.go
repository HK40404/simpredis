package timewheel

import (
	"sync"
	"testing"
	"time"
)

func TestTimeWheel(t *testing.T) {

	counter := 0

	var wg sync.WaitGroup
	wg.Add(3)
	job1 := func() {
		defer wg.Done()
		if !(counter >= 0 && counter <= 2) {
			t.Fail()
		}
		t.Log("Job1 has been done!")
	}
	job5 := func() {
		defer wg.Done()
		if !(counter >= 4 && counter <= 6) {
			t.Fail()
		}
		t.Log("Job5 has been done!")
	}
	job11 := func() {
		defer wg.Done()
		if !(counter >= 10 && counter <= 12) {
			t.Fail()
		}
		t.Log("Job11 has been done!")
	}
	job3 := func() {
		t.Fail()
	}

	go func() {
		for range time.NewTicker(time.Second).C {
			counter++
			t.Log(counter)
		}
	}()
	Tw.AddTask("1", 1*time.Second, job1)
	Tw.AddTask("3", 3*time.Second, job3)
	Tw.AddTask("5", 5*time.Second, job5)
	Tw.AddTask("11", 11*time.Second, job11)
	Tw.RemoveTask("3")

	wg.Wait()
}
