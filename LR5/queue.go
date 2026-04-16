package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
)

type Task struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

type LogPayload struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

func (s *Service) PushTask(ctx context.Context, queueName string, taskType string, payload any) error {
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	task := Task{
		ID:      uuid.New().String(),
		Type:    taskType,
		Payload: payloadBytes,
	}

	taskBytes, err := json.Marshal(task)
	if err != nil {
		return err
	}

	return s.db.LPush(ctx, queueName, taskBytes).Err()
}

func (s *Service) StartWorker(ctx context.Context, queueName string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			res, err := s.db.BRPop(ctx, 0, queueName).Result()
			if err != nil {
				log.Printf("Worker error (BRPop): %v", err)
				time.Sleep(1 * time.Second)
				continue
			}

			if len(res) != 2 {
				continue
			}

			var task Task
			if err := json.Unmarshal([]byte(res[1]), &task); err != nil {
				log.Printf("Worker failed to unmarshal task: %v", err)
				continue
			}

			s.processTask(task)
		}
	}
}

func (s *Service) processTask(task Task) {
	switch task.Type {
	case "email":
		var p EmailPayload
		if err := json.Unmarshal(task.Payload, &p); err != nil {
			log.Printf("Task %s: invalid email payload: %v", task.ID, err)
			return
		}
		log.Printf("Processed email task [%s]: sent to %s (subject: %s)", task.ID, p.To, p.Subject)
	case "log":
		var p LogPayload
		if err := json.Unmarshal(task.Payload, &p); err != nil {
			log.Printf("Task %s: invalid log payload: %v", task.ID, err)
			return
		}
		log.Printf("Processed log task [%s]: [%s] %s", task.ID, p.Level, p.Message)
	default:
		log.Printf("Processed unknown task type [%s]: %s", task.ID, task.Type)
	}
}
