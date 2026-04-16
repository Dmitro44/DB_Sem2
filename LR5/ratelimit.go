package main

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

func (s *Service) RateLimitSimple(ctx context.Context, userID string, limit int64, window time.Duration) (bool, error) {
	windowStart := time.Now().Truncate(window).Unix()
	key := fmt.Sprintf("rate:%s:%d", userID, windowStart)

	count, err := s.db.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}

	if count == 1 {
		err = s.db.Expire(ctx, key, window).Err()
		if err != nil {
			return false, err
		}
	}

	if count > limit {
		return false, nil
	}

	return true, nil
}

const slidingWindowLua = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window_ms = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local clear_before = now_ms - window_ms
local member = ARGV[4]

redis.call('ZREMRANGEBYSCORE', key, '-inf', clear_before)
local count = redis.call('ZCARD', key)

if count < limit then
	redis.call('ZADD', key, now_ms, member)
	redis.call('PEXPIRE', key, window_ms)
	return 1
end

return 0
`

func (s *Service) RateLimitSlidingWindow(ctx context.Context, userID string, limit int64, window time.Duration) (bool, error) {
	key := fmt.Sprintf("rate:sliding:%s", userID)
	now := time.Now()
	nowMS := now.UnixMilli()
	windowMS := window.Milliseconds()
	member := fmt.Sprintf("%d-%s", nowMS, uuid.New().String()[:8])

	res, err := s.db.Eval(ctx, slidingWindowLua, []string{key}, limit, windowMS, nowMS, member).Result()
	if err != nil {
		return false, err
	}

	allowed, ok := res.(int64)
	if !ok {
		return false, fmt.Errorf("unexpected lua result type")
	}

	return allowed == 1, nil
}
