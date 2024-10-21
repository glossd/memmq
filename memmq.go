package memmq

import (
	"errors"
	"sync"
)

const defaultChannelSize = 100

// Topic -> Subscribers
var data = make(map[string][]chan interface{}, 4)
var mx sync.RWMutex

func Publish(topic string, message interface{}) error {
	if topic == "" {
		return errors.New("topic can't be empty")
	}
	if message == nil {
		return errors.New("message can't be empty")
	}
	mx.RLock()
	defer mx.RUnlock()
	chans, ok := data[topic]
	if !ok {
		return nil
	}
	for _, c := range chans {
		c <- message
	}
	return nil
}

type SubscribeConfig struct {
	ChannelSize int
}

// Subscribe is a blocking function
func Subscribe(topic string, process func(message interface{}), sc ...SubscribeConfig) error {
	if topic == "" {
		return errors.New("topic can't be empty")
	}
	mx.Lock()
	channelSize := defaultChannelSize
	if len(sc) > 0 && sc[0].ChannelSize > 0 {
		channelSize = sc[0].ChannelSize
	}
	sub := make(chan interface{}, channelSize)
	data[topic] = append(data[topic], sub)
	mx.Unlock()

	for {
		process(<-sub)
	}
}
