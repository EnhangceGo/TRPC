package mclient

import (
	"context"
	"io"
	"reflect"
	"sync"

	"github.com/mindcarver/thin-rpc/client"
	trpc "github.com/mindcarver/thin-rpc/server"
)

type MClient struct {
	d       Discovery
	mode    selectMode
	opt     *trpc.Option
	mu      sync.Mutex // protect following
	clients map[string]*client.Client
}

func (mc *MClient) Close() error {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	for key, c := range mc.clients {
		// I have no idea how to deal with error, just ignore it.
		_ = c.Close()
		delete(mc.clients, key)
	}
	return nil
}

var _ io.Closer = (*MClient)(nil)

func NewMClient(d Discovery, mode selectMode, opt *trpc.Option) *MClient {
	return &MClient{d: d, mode: mode, opt: opt, clients: make(map[string]*client.Client)}
}

func (mc *MClient) dial(rpcAddr string) (*client.Client, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	c, ok := mc.clients[rpcAddr]
	if ok && !c.IsAvailable() {
		_ = c.Close()
		delete(mc.clients, rpcAddr)
		c = nil
	}
	if c == nil {
		var err error
		c, err = client.MDial(rpcAddr, mc.opt)
		if err != nil {
			return nil, err
		}
		mc.clients[rpcAddr] = c
	}
	return c, nil
}

func (mc *MClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	c, err := mc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return c.Call(ctx, serviceMethod, args, reply)
}

// Call invokes the named function, waits for it to complete,
// and returns its error status.
// xc will choose a proper server.
func (mc *MClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := mc.d.Get(mc.mode)
	if err != nil {
		return err
	}
	return mc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast invokes the named function for every server registered in discovery
func (mc *MClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := mc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex // protect e and replyDone
	var e error
	replyDone := reply == nil // if reply is nil, don't need to set value
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := mc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				cancel() // if any call failed, cancel unfinished calls
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}
