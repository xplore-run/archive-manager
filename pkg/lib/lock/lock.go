package lock

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisLockService struct {
	client *redis.Client
}

func NewRedisLockService(client *redis.Client) *RedisLockService {
	return &RedisLockService{client: client}
}

func (rls *RedisLockService) AcquireLock(ctx context.Context, lockKey string, identifier string, timeout time.Duration) (string, error) {
	// Try to acquire the lock
	ok, err := rls.client.SetNX(ctx, lockKey, identifier, timeout).Result()
	if err != nil {
		return "", err
	}

	if ok {
		// Lock acquired successfully
		return identifier, nil
	} else {
		// Failed to acquire lock
		return "", nil
	}
}

func (rls *RedisLockService) ReleaseLock(ctx context.Context, lockKey string, identifier string) (bool, error) {
	// Check if the lock is currently held by the provided identifier
	currentIdentifier, err := rls.client.Get(ctx, lockKey).Result()
	if err != nil && err != redis.Nil {
		return false, err
	}

	if currentIdentifier == identifier {
		// Release the lock
		_, err := rls.client.Del(ctx, lockKey).Result()
		if err != nil {
			return false, err
		}
		return true, nil
	} else {
		// Lock is held by another client or expired
		return false, nil
	}
}

type AcquireLockWithRetryArgs struct {
	LockName      string
	Identifier    string
	Timeout       time.Duration
	RetryInterval time.Duration
	Retry         int
}

func (rls *RedisLockService) AcquireLockWithRetry(ctx context.Context, args AcquireLockWithRetryArgs) (string, error) {
	var gotIdentifier string
	var err error
	retries := 0

	for retries < args.Retry {
		gotIdentifier, err = rls.AcquireLock(ctx, args.LockName, args.Identifier, args.Timeout)
		if err != nil {
			return "", err
		}
		if gotIdentifier != "" {
			return gotIdentifier, nil
		}

		// If lock acquisition fails, wait for a specified interval before retrying
		time.Sleep(args.RetryInterval)
		retries++
	}

	return "", nil
}
