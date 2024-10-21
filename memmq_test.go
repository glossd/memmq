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
		assertNil(Subscribe("publish-subscribe", func(a interface{}) {
			m := Message{Type: "loading"}
			if a != m {
				t.Fatalf("wrong message")
			}
			wg.Done()
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
		assertNil(Subscribe("2subs", func(a interface{}) {
			m := Message{Type: "loading"}
			if a != m {
				t.Fatalf("wrong message")
			}
			wg.Done()
		}))
	}()
	go func() {
		assertNil(Subscribe("2subs", func(a interface{}) {
			m := Message{Type: "loading"}
			if a != m {
				t.Fatalf("wrong message")
			}
			wg.Done()
		}))
	}()
	time.Sleep(2 * time.Millisecond)
	assertNil(Publish("2subs", Message{Type: "loading"}))
	wg.Wait()
}

func assertNil(v interface{}) {
	if v != nil {
		panic(fmt.Sprintf("expected nil, got=%v", v))
	}
}
