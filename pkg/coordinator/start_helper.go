package coordinator

import (
	"context"
	"fmt"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "rivage/proto"
)

// StartOnAddr starts the coordinator on the given address and sends the
// actual resolved listen address to the ready channel before serving.
// Passing ":0" lets the OS pick a free port — useful in tests.
func (c *Coordinator) StartOnAddr(ctx context.Context, addr string, ready chan<- string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}

	var grpcOpts []grpc.ServerOption
	grpcOpts = append(grpcOpts, grpc.Creds(insecure.NewCredentials()))

	c.grpcSrv = grpc.NewServer(grpcOpts...)
	pb.RegisterWorkerServiceServer(c.grpcSrv, &grpcServer{coord: c})

	if ready != nil {
		ready <- lis.Addr().String()
	}

	go c.watchdog(ctx)

	serveErr := make(chan error, 1)
	go func() { serveErr <- c.grpcSrv.Serve(lis) }()

	select {
	case <-ctx.Done():
		c.grpcSrv.GracefulStop()
		return nil
	case err := <-serveErr:
		return err
	}
}
