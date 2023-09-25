package tools

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"testing"
)

func TestTools(t *testing.T){
	testNewLock(t)
	testNewRedLock(t)
}

func testNewLock(t *testing.T){
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6001",
		Password: "", // no password set
		DB:       4,  // use default DB
	})

	disLock, err := NewRedisLock(client, "lock resource")
	if err != nil {
		t.Fatal(err)
	}

	succ, err := disLock.TryLock(context.Background())
	if err != nil {
		t.Log(err)
		return
	}

	if succ {
		fmt.Println("上锁成功")
		defer disLock.Unlock(context.Background())
		fmt.Println("解锁成功")
	}
}

func testNewRedLock(t *testing.T) {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6001",
		Password: "", // no password set
		DB:       4,  // use default DB
	})
	client2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:6002",
		Password: "", // no password set
		DB:       4,  // use default DB
	})
	client3 := redis.NewClient(&redis.Options{
		Addr:     "localhost:6003",
		Password: "", // no password set
		DB:       4,  // use default DB
	})

	disLock, err := NewRedLock([]RedisClient{client, client2, client3}, "lock resource")
	if err != nil {
		t.Fatal(err)
	}

	succ, err := disLock.TryRedLock(context.Background())
	if err != nil {
		t.Log(err)
		return
	}

	if succ {
		fmt.Println("上锁成功")
		defer disLock.UnlockRed(context.Background())
		fmt.Println("解锁成功")
	}
}
