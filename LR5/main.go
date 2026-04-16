package main

import (
	"context"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

type Service struct {
	db *redis.Client
}

func NewService(db *redis.Client) *Service {
	return &Service{db: db}
}

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	svc := NewService(rdb)
	ctx := context.Background()

	queueName := "queue:tasks"
	go svc.StartWorker(ctx, queueName)

	log.Println("Worker started. Pushing test tasks...")

	err := svc.PushTask(ctx, queueName, "email", EmailPayload{
		To:      "test@example.com",
		Subject: "Lab 5 notification",
		Body:    "Queue works!",
	})
	if err != nil {
		log.Fatal(err)
	}

	err = svc.PushTask(ctx, queueName, "log", LogPayload{
		Level:   "INFO",
		Message: "User completed action in lab",
	})
	if err != nil {
		log.Fatal(err)
	}

	err = svc.PushTask(ctx, queueName, "email", EmailPayload{
		To:      "test1@example.com",
		Subject: "Lab 5 notification",
		Body:    "Queue works test 2 hello!",
	})
	if err != nil {
		log.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	log.Println("--- Demonstrating Fixed vs Sliding Window difference ---")
	testUserID := "diff_user"
	limit := int64(3)
	window := 10 * time.Second

	now := time.Now()
	secondsToWait := 8 - (now.Unix() % 10)
	if secondsToWait <= 0 {
		secondsToWait += 10
	}

	log.Printf("Waiting %d seconds to hit the end of the fixed window...", secondsToWait)
	time.Sleep(time.Duration(secondsToWait) * time.Second)

	log.Println(">>> Phase 1: Sending 3 requests at the END of window")
	for i := 1; i <= 3; i++ {
		svc.RateLimitSimple(ctx, testUserID, limit, window)
		svc.RateLimitSlidingWindow(ctx, testUserID, limit, window)
	}

	log.Println(">>> Waiting 3 seconds (crossing the fixed window boundary)...")
	time.Sleep(3 * time.Second)

	log.Println(">>> Phase 2: Sending more requests right after boundary")
	for i := 1; i <= 3; i++ {
		simple, _ := svc.RateLimitSimple(ctx, testUserID, limit, window)
		sliding, _ := svc.RateLimitSlidingWindow(ctx, testUserID, limit, window)

		log.Printf("Request %d: Simple Counter allowed: %v | Sliding Window allowed: %v", i, simple, sliding)
	}

	log.Println("Shutting down...")
}
