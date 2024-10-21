package memmq

import (
	"errors"
	"sync"
)

const defaultChannelSize = 100

// Will retry 2 more times.
const maxDeliveryRetry = 2

// Topic -> Subscribers
var data = make(map[string][]chan *event, 16)
var mx sync.RWMutex

type event struct {
	payload interface{}
	retry   int
}

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
		c <- &event{payload: message, retry: 0}
	}
	return nil
}

type SubscribeConfig struct {
	ChannelSize int
}

// Subscribe is a blocking function.
// In the process function return false if you want to retry the message.
func Subscribe(topic string, process func(message interface{}) bool, sc ...SubscribeConfig) error {
	if topic == "" {
		return errors.New("topic can't be empty")
	}
	mx.Lock()
	channelSize := defaultChannelSize
	if len(sc) > 0 && sc[0].ChannelSize > 0 {
		channelSize = sc[0].ChannelSize
	}
	subChan := make(chan *event, channelSize)
	data[topic] = append(data[topic], subChan)
	mx.Unlock()

	for {
		evt := <-subChan
		ack := process(evt.payload)
		if !ack && evt.retry < maxDeliveryRetry {
			evt.retry++
			subChan <- evt
		}
	}
}
