package grpc

import (
	"net"
	"strconv"
	"strings"
	"testing"

	"github.com/micro/go-micro/client"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-micro/registry/mock"
	"github.com/micro/go-micro/selector"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pb "google.golang.org/grpc/examples/helloworld/helloworld"
)

// server is used to implement helloworld.GreeterServer.
type greeterServer struct{}

// SayHello implements helloworld.GreeterServer
func (g *greeterServer) SayHello(ctx context.Context, in *pb.HelloRequest) (*pb.HelloReply, error) {
	return &pb.HelloReply{Message: "Hello " + in.Name}, nil
}

func TestGRPCClient(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer l.Close()

	s := grpc.NewServer()
	pb.RegisterGreeterServer(s, &greeterServer{})

	go s.Serve(l)
	defer s.Stop()

	parts := strings.Split(l.Addr().String(), ":")
	port, _ := strconv.Atoi(parts[len(parts)-1])
	addr := strings.Join(parts[:len(parts)-1], ":")

	r := mock.NewRegistry()
	r.Register(&registry.Service{
		Name:    "test",
		Version: "test",
		Nodes: []*registry.Node{
			&registry.Node{
				Id:      "test-1",
				Address: addr,
				Port:    port,
			},
		},
	})

	se := selector.NewSelector(
		selector.Registry(r),
	)

	c := NewClient(
		client.Registry(r),
		client.Selector(se),
	)

	req := c.NewRequest("test", "/helloworld.Greeter/SayHello", &pb.HelloRequest{
		Name: "John",
	})

	rsp := pb.HelloReply{}

	err = c.Call(context.TODO(), req, &rsp)
	if err != nil {
		t.Fatal(err)
	}

	if rsp.Message != "Hello John" {
		t.Fatalf("Got unexpected response %v", rsp.Message)
	}
}
