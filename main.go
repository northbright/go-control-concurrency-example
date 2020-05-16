package main

import (
	"context"
	"log"
	"math/rand"
	"time"
)

type MessageType uint

const (
	ERROR MessageType = iota
	SCHEDULED
	STARTED
	STOPPED
	RESUMED
	PROGRESSUPDATED
	DONE
	maxMessageType
)

type Message struct {
	Type   MessageType
	TaskID int
	Data   interface{}
}

func newMessage(t MessageType, taskID int, data interface{}) *Message {
	return &Message{t, taskID, data}
}

type TaskManager struct {
	concurrency int
	ch          chan *Message
	sem         chan struct{}
}

func NewTaskManager(concurrency int) (*TaskManager, <-chan *Message) {
	ch := make(chan *Message)

	// Use a buffered channel to control the number of concurrency.
	sem := make(chan struct{}, concurrency)

	return &TaskManager{concurrency, ch, sem}, ch
}

func (m *TaskManager) Start(ctx context.Context, ID int, prevProgress int) {
	go func() {
		m.start(ctx, ID, prevProgress)
	}()
}

func (m *TaskManager) start(ctx context.Context, ID int, prevProgress int) {
	defer func() {
		<-m.sem
	}()

	progress := 0

	m.ch <- newMessage(SCHEDULED, ID, nil)

BEFORE_START:
	// This loop make it possible to cancel the task even it's scheduled:
	// blocked at m.mem <- struct{}{}.
	for {
		select {
		case <-ctx.Done():
			m.ch <- newMessage(STOPPED, ID, progress)
			return
		case m.sem <- struct{}{}:
			break BEFORE_START
		}
	}

	// New task.
	if prevProgress == 0 {
		m.ch <- newMessage(STARTED, ID, nil)
	} else {
		//Resume the task if previous progress is not 0.
		progress = prevProgress
		m.ch <- newMessage(RESUMED, ID, progress)
	}

	// Do work and update progress.
	for {
		select {
		case <-ctx.Done():
			m.ch <- newMessage(STOPPED, ID, progress)
			return
		default:
			if progress < 100 {
				progress++
				m.ch <- newMessage(PROGRESSUPDATED, ID, progress)
				time.Sleep(time.Millisecond * 10)
			} else {
				m.ch <- newMessage(DONE, ID, nil)
				return
			}
		}
	}
}

func main() {
	man, ch := NewTaskManager(2)

	// Use a timeout to exit the program.
	tm := time.After(5 * time.Second)

	// Record the cancel func for each task ID.
	cancelMap := map[int]context.CancelFunc{}

	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cancelMap[i] = cancel
		// Start tasks.
		man.Start(ctx, i, 0)
	}

	// Stop 1 task randomly.
	go func() {
		time.Sleep(50 * time.Millisecond)
		ID := rand.Intn(4)
		cancel := cancelMap[ID]
		cancel()
	}()

	for {
		select {
		case <-tm:
			log.Printf("timeout")
			return
		case msg := <-ch:
			switch msg.Type {
			case ERROR:
				log.Printf("task %d error", msg.TaskID)
			case SCHEDULED:
				log.Printf("task %d scheduled", msg.TaskID)
			case STARTED:
				log.Printf("task %d started", msg.TaskID)
			case RESUMED:
				progress := msg.Data.(int)
				log.Printf("task %d resumed, progress: %d", msg.TaskID, progress)
			case STOPPED:
				progress := msg.Data.(int)
				log.Printf("task %d stopped, progress: %d", msg.TaskID, progress)

				// Resume the task if it's stopped.
				ctx, cancel := context.WithCancel(context.Background())
				cancelMap[msg.TaskID] = cancel
				man.Start(ctx, msg.TaskID, progress)

			case PROGRESSUPDATED:
				progress := msg.Data.(int)
				log.Printf("task %d progress updated: %d", msg.TaskID, progress)

			case DONE:
				log.Printf("task %d done", msg.TaskID)
			}
		}
	}
}
