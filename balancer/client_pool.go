package balancer

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type ClientPool struct {
	consulResolver *ConsulResolver
	connPool       sync.Map
	timeout        time.Duration
}

type ConnWithTs struct {
	UpdateTime int64
	Conn       *grpc.ClientConn
}

func NewClientPool(address string, service string, myService string, interval time.Duration,
	serviceRatio float64, cpuThreshold float64, zone string, timeout time.Duration) (*ClientPool, error) {
	resolver, err := NewConsulResolver(
		address,
		service,
		myService,
		interval,
		serviceRatio,
		cpuThreshold,
		zone,
	)
	if err != nil {
		return nil, err
	}
	return NewClientPoolWithResolver(resolver, timeout)
}

func NewClientPoolWithResolver(resolver *ConsulResolver, timeout time.Duration) (*ClientPool, error) {
	clientPool := &ClientPool{}
	clientPool.consulResolver = resolver
	clientPool.timeout = timeout
	clientPool.InitPool()
	return clientPool, nil
}

func (pool *ClientPool) InitPool() {

	// with initial pool size of 10
	for i := 0; i < 10; i++ {
		conn, addr, err := pool.NewConnect()
		if err != nil {
			continue
		}
		pool.connPool.Store(addr, &ConnWithTs{time.Now().Unix(), conn})
	}
	go pool.watch()
}

func (pool *ClientPool) watch() {
	// every 30 second
	// delete unused connection
	tick := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-tick.C:
			now := time.Now().Unix()
			pool.connPool.Range(func(key, val interface{}) bool {
				if connWithTs, ok := val.(*ConnWithTs); ok {
					if now-connWithTs.UpdateTime > 30 {
						if connWithTs.Conn != nil {
							connWithTs.Conn.Close()
						}
						pool.connPool.Delete(key)
						
					}
				}
				return true
			})
		}
	}
	return
}
func (pool *ClientPool) NewConnect() (*grpc.ClientConn, string, error) {
	retry := 0
	var err error
	// new with retry
	for {
		retry++
		if retry > 3 {
			break
		}

		addr, err := pool.getNodeAddr()
		if addr == "" {
			continue
		}

		conn, err := pool.NewConnectWithAddr(addr)
		if err == nil {
			return conn, addr, err
		}

	}
	return nil, "", err
}

func (pool *ClientPool) NewConnectWithAddr(addr string) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), pool.timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithBlock(), grpc.WithInsecure())
	return conn, err

}

func (pool *ClientPool) getNodeAddr() (string, error) {
	node := pool.consulResolver.DiscoverNode()
	if node == nil || len(node.Address) <= 0 {
		return "", errors.New("no valid consul node")
	}
	return node.Address, nil
}

func (pool *ClientPool) Get() (*grpc.ClientConn, error) {
	var err error
	addr, err := pool.getNodeAddr()
	if err != nil {
		return nil, err
	}

	// 1st: use the picked adrress from pool connection
	if val, ok := pool.connPool.Load(addr); ok {
		connWithTs := val.(*ConnWithTs)
		connWithTs.UpdateTime = time.Now().Unix()
		return connWithTs.Conn, nil
	}

	var conn *grpc.ClientConn

	// 2nd: use the picked address for new connection
	conn, err = pool.NewConnectWithAddr(addr)
	if err != nil {
		// 3rd: create a new connection
		conn, addr, err = pool.NewConnect()
	}

	// use load or store for repeated write
	val, loaded := pool.connPool.LoadOrStore(addr, &ConnWithTs{time.Now().Unix(), conn})

	if loaded {
		conn.Close()
	}

	connWithTs := val.(*ConnWithTs)
	return connWithTs.Conn, nil
}
/*
// delete conn from pool when error
func (pool *ClientPool) DropConnByAddr(addr string) {
	if val, ok := pool.connPool.Load(addr); ok {
		pool.connPool.Delete(addr)
		connWithTs := val.(*ConnWithTs)
		connWithTs.Conn.Close()
	}
}
*/
