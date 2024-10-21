package memmq

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type Message struct {
	Type string
}

func TestPublishSubscribe(t *testing.T) {
	assertNil(Publish("publish-subscribe", Message{Type: "nothing"}))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		assertNil(Subscribe("publish-subscribe", func(a interface{}) bool {
			m := Message{Type: "loading"}
			if a != m {
				t.Fatalf("wrong message")
			}
			wg.Done()
			return true
		}))
	}()
	time.Sleep(time.Millisecond)
	assertNil(Publish("publish-subscribe", Message{Type: "loading"}))
	wg.Wait()
}

func TestTwoSubscribers(t *testing.T) {
	assertNil(Publish("2subs", Message{Type: "nothing"}))

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		assertNil(Subscribe("2subs", func(a interface{}) bool {
			m := Message{Type: "loading"}
			if a != m {
				t.Fatalf("wrong message")
			}
			wg.Done()
			return true
		}))
	}()
	go func() {
		assertNil(Subscribe("2subs", func(a interface{}) bool {
			m := Message{Type: "loading"}
			if a != m {
				t.Fatalf("wrong message")
			}
			wg.Done()
			return true
		}))
	}()
	time.Sleep(2 * time.Millisecond)
	assertNil(Publish("2subs", Message{Type: "loading"}))
	wg.Wait()
}

func TestRetry(t *testing.T) {
	assertNil(Publish("retry", Message{Type: "nothing"}))

	var wg sync.WaitGroup
	// should retry three times and give up
	wg.Add(3)
	go func() {
		assertNil(Subscribe("retry", func(a interface{}) bool {
			m := Message{Type: "loading"}
			if a != m {
				t.Fatalf("wrong message")
			}
			// couldn't process
			wg.Done()
			return false
		}))
	}()

	time.Sleep(2 * time.Millisecond)
	assertNil(Publish("retry", Message{Type: "loading"}))
	wg.Wait()
}

func assertNil(v interface{}) {
	if v != nil {
		panic(fmt.Sprintf("expected nil, got=%v", v))
	}
}
