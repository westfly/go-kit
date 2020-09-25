package balancer

import (
	"context"
	"errors"
	"sync"
	"time"
)

type FactoryFn func(addr string) (interface{}, error)
type CloseFn func(interface{}) error

type Config struct {
	Address      string
	Service      string
	RegService   string
	Zone         string
	ServiceRatio float64
	CpuThreshold float64
	Timeout      time.Duration
	Interval     time.Duration
	IdleTimeout  time.Duration
	Factory      FactoryFn
	Close        CloseFn
}

type ClientPool struct {
	consulResolver *ConsulResolver
	connPool       sync.Map
	timeout        time.Duration
	factory        FactoryFn
	close          CloseFn
}

type ConnWithTs struct {
	UpdateTime int64
	Conn       interface{}
}

func NewClientPoolByConfig(config *Config) (*ClientPool, error) {
	resolver, err := NewConsulResolver(
		config.Address,
		config.Service,
		config.RegService,
		config.Interval,
		config.ServiceRatio,
		config.CpuThreshold,
		config.Zone,
	)
	if err != nil {
		return nil, err
	}
	return NewClientPoolWithResolver(resolver, config.Timeout, config.Factory, config.Close)

	return nil, nil
}
func NewClientPool(address string, service string, myService string, interval time.Duration,
	serviceRatio float64, cpuThreshold float64, zone string, timeout time.Duration,
	factory FactoryFn, close CloseFn) (*ClientPool, error) {
	config := &Config{
		address,
		service,
		myService,
		zone,
		serviceRatio,
		cpuThreshold,
		timeout,
		interval,
		interval,
		factory,
		close,
	}
	return NewClientPoolByConfig(config)
}

func NewClientPoolWithResolver(resolver *ConsulResolver, timeout time.Duration,
	factory FactoryFn, close CloseFn) (*ClientPool, error) {
	clientPool := &ClientPool{}
	clientPool.consulResolver = resolver
	clientPool.timeout = timeout
	clientPool.factory = factory
	clientPool.close = close
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
							pool.close(connWithTs.Conn)
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
func (pool *ClientPool) NewConnect() (interface{}, string, error) {
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

func (pool *ClientPool) NewConnectWithAddr(addr string) (interface{}, error) {
	_, cancel := context.WithTimeout(context.Background(), pool.timeout)
	defer cancel()
	conn, err := pool.factory(addr)
	return conn, err

}

func (pool *ClientPool) getNodeAddr() (string, error) {
	node := pool.consulResolver.DiscoverNode()
	if node == nil || len(node.Address) <= 0 {
		return "", errors.New("no valid consul node")
	}
	return node.Address, nil
}

func (pool *ClientPool) Get() (interface{}, error) {
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
	// 2nd: use the picked address for new connection
	conn, err2 := pool.NewConnectWithAddr(addr)
	if err2 != nil {
		// 3rd: create a new connection
		conn, addr, err2 = pool.NewConnect()
	}

	// use load or store for repeated write
	val, loaded := pool.connPool.LoadOrStore(addr, &ConnWithTs{time.Now().Unix(), conn})

	if loaded {
		pool.close(conn)
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
