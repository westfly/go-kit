package balancer

import (
	"fmt"
	"time"
	//"sync"
	"testing"
)

func TestGRPC(t *testing.T) {
	config := Config{
		"tcp",
		"tcp",
		"tcp",
		"tcp",
		10.0,
		0.7,
		time.Duration(30),
		time.Duration(30),
		time.Duration(30),
		NewGRpcClient,
		CloseGRpcClient,
	}
	pool, _ := NewClientPoolByConfig(&config)
	pool.Get()
	fmt.Println("hello")
}
