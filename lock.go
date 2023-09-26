package tools

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/gofrs/uuid"
)

const (
	// defaultExp  default timeout for lock
	defaultExp = 10 * time.Second

	// sleepDur default sleep time for spin lock
	sleepDur = 10 * time.Millisecond

	// spinMaxTry default max try number
	spinMaxTry = 20

	// spinMinTry default min try number
	spinMinTry = 0

	// err
	ErrLockFailed = "ErrLockFailed"
)

// RedisLock .
type RedisLock struct {
	RedClients 		[]RedisClient
	successClients 		[]RedisClient
	Client     		RedisClient
	Key        		string // resources that need to be locked
	uuid       		string // lock owner uuid
	retryTimes 		int
	cancelFunc 		context.CancelFunc
}

// NewRedisLock new a redis distribute lock
func NewRedisLock(client RedisClient, key string) (*RedisLock, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	return &RedisLock{
		Client: client,
		Key:    key,
		uuid:   id.String(),
		retryTimes: 10,
	}, nil
}

// NewRedisLock new a redis distribute lock
func NewRedLock(clients []RedisClient, key string) (*RedisLock, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return nil, err
	}
	return &RedisLock{
		RedClients: 	clients,
		Key:    	key,
		uuid:   	id.String(),
		retryTimes: 	10,
		successClients: []RedisClient{},
	}, nil
}

// TryLock attempt to lock, return true if the lock is successful, otherwise false
func (rl *RedisLock) TryLock(ctx context.Context) (bool, error) {
	succ, err := rl.Client.SetNX(ctx, rl.Key, rl.uuid, defaultExp).Result()
	if err != nil || !succ {
		return false, err
	}
	c, cancel := context.WithCancel(ctx)
	rl.cancelFunc = cancel
	rl.refresh(c)
	return succ, nil
}

// TryRedLock attempt to lock, return true if the lock is successful, otherwise false
func (rl *RedisLock) TryRedLock(ctx context.Context) (bool, error) {
	var wg sync.WaitGroup
	wg.Add(len(rl.RedClients))
	for _, client := range rl.RedClients{
		go func(client RedisClient) {
			defer wg.Done()
			succ, err := client.SetNX(ctx, rl.Key, rl.uuid, defaultExp).Result()
			if err != nil || !succ {
				return
			}
			rl.successClients = append(rl.successClients, client)
		}(client)
	}
	wg.Wait()

	c, cancel := context.WithCancel(ctx)
	rl.cancelFunc = cancel
	if len(rl.successClients) < len(rl.RedClients)/2+1{
		// Failed to add lock, release the acquired lock.
		delRes, err := rl.UnlockRed(context.Background())
		rl.successClients = []RedisClient{}
		if !delRes{
			return false, err
		}
		return false, errors.New(ErrLockFailed)
	}
	rl.redRefresh(c)

	return true, nil
}

// SpinLock Loop `retryTimes` times to call TryLock
func (rl *RedisLock) SpinLock(ctx context.Context, retryTimes int) (bool, error) {
	rl.retryTimes = retryTimes
	for i := 0; i < rl.retryTimes; i++ {
		resp, err := rl.TryLock(ctx)
		if err != nil {
			return false, err
		}
		if resp {
			return resp, nil
		}
		time.Sleep(sleepDur)
	}
	return false, nil
}

// SelfAdaptionSpinLock Self-adaption Loop `retryTimes` times to call TryLock
func (rl *RedisLock) SelfAdaptionSpinLock(ctx context.Context) (bool, error) {
	for i := 0; i < rl.retryTimes; i++ {
		resp, err := rl.TryLock(ctx)
		if err != nil {
			return false, err
		}
		if resp {
			if rl.retryTimes < spinMaxTry{
				rl.retryTimes++
			}
			return resp, nil
		}
		time.Sleep(sleepDur)
	}
	if rl.retryTimes > spinMinTry{
		rl.retryTimes--
	}
	return false, nil
}

// Unlock attempt to unlock, return true if the lock is successful, otherwise false
func (rl *RedisLock) Unlock(ctx context.Context) (bool, error) {
	resp, err := NewTools(rl.Client).Cad(ctx, rl.Key, rl.uuid)
	if err != nil {
		return false, err
	}

	if resp {
		rl.cancelFunc()
	}
	return resp, nil
}

// UnlockRed attempt to unlock, return true if the lock is successful, otherwise false
func (rl *RedisLock) UnlockRed(ctx context.Context) (bool, error) {
	errCollect := make(chan error, len(rl.successClients))
	defer close(errCollect)
	var wg sync.WaitGroup
	wg.Add(len(rl.successClients))
	for _, client := range rl.successClients{
		go func(client RedisClient) {
			defer wg.Done()
			_, err := NewTools(client).Cad(ctx, rl.Key, rl.uuid)
			if err != nil{
				errCollect <- err
			}
		}(client)
	}
	wg.Wait()
	if len(errCollect) > 0 {
		return false, <-errCollect
	}

	rl.cancelFunc()
	return true, nil
}

// refresh refresh term
func (rl *RedisLock) refresh(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(defaultExp / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				rl.Client.Expire(ctx, rl.Key, defaultExp)
			}
		}
	}()
}

// redRefresh refresh red lock term
func (rl *RedisLock) redRefresh(ctx context.Context){
	go func() {
		ticker := time.NewTicker(defaultExp / 2)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				for _, client := range rl.successClients{
					go func(client RedisClient){
						client.Expire(ctx, rl.Key, defaultExp)
					}(client)
				}
			}
		}
	}()
}
