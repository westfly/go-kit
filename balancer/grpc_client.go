package balancer

import (
	"context"
	//"errors"
	"google.golang.org/grpc"
)

type GRpcClient struct {
	Conn *grpc.ClientConn
}

func NewGRpcClient(addr string) (interface{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithBlock(), grpc.WithInsecure())
	return &GRpcClient{conn}, err
}
func CloseGRpcClient(v interface{}) error {
	return v.(*GRpcClient).Close()
}
func (c *GRpcClient) Close() error {
	c.Conn.Close()
	return nil
}
func (c *GRpcClient) Read(bytes []byte) error {
	return nil
}
func (c *GRpcClient) Write(bytes []byte) error {
	return nil
}
