package client

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/mindcarver/thin-rpc/codec"
	trpc "github.com/mindcarver/thin-rpc/server"
)

//Call represents an active RPC.
type Call struct {
	Seq    uint64
	SrvMod string      // <service>.<method>
	Args   interface{} // arguments to the function
	Reply  interface{} // reply from the function
	Err    error
	Done   chan *Call // Strobes when call is complete, Call call.done() to notify the caller.
}

func (call *Call) done() {
	call.Done <- call
}

type Client struct {
	c        codec.Codec
	opt      trpc.Option
	sending  sync.Mutex // Ensure the orderly sending of the request, that is, to prevent multiple request messages from confusing
	header   codec.Header
	mu       sync.Mutex
	seq      uint64           // Each request has the unique number
	pending  map[uint64]*Call // Store the request that is not processed, the key is the number, and the value is a call instance
	closing  bool             // user has called Close
	shutdown bool             // server has told us to stop
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *trpc.Option) (client *Client, err error)

var _ io.Closer = (*Client)(nil)

// NewHTTPClient new a Client instance via HTTP as transport protocol
func NewHTTPClient(conn net.Conn, opt *trpc.Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", "/_tprc_"))

	// Require successful HTTP response
	// before switching to RPC protocol.
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == "200 Connected to Thin RPC" {
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// DialHTTP connects to an HTTP RPC server at the specified network address
// listening on the default HTTP RPC path.
func DialHTTP(network, address string, opts ...*trpc.Option) (*Client, error) {
	return dialTimeout(NewHTTPClient, network, address, opts...)
}

// Close the connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing {
		return ErrShutdown
	}
	c.closing = true
	return c.c.Close()
}

// IsAvailable return true if the client does work
func (c *Client) IsAvailable() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.shutdown && !c.closing
}

var ErrShutdown = errors.New("connection is shut down")

func (c *Client) registerCall(call *Call) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closing || c.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = c.seq
	c.pending[call.Seq] = call
	c.seq++
	return call.Seq, nil
}

func (c *Client) removeCall(seq uint64) *Call {
	c.mu.Lock()
	defer c.mu.Unlock()
	call := c.pending[seq]
	delete(c.pending, seq)
	return call
}

func (c *Client) terminateCalls(err error) {
	c.sending.Lock()
	defer c.sending.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shutdown = true
	for _, call := range c.pending {
		call.Err = err
		call.done()
	}
}

func (c *Client) receive() {
	var err error
	for err == nil {
		var h codec.Header
		if err = c.c.ReadHeader(&h); err != nil {
			break
		}
		call := c.removeCall(h.Seq)
		switch {
		case call == nil:
			// it usually means that Write partially failed
			// and call was already removed.
			err = c.c.ReadBody(nil)
		case h.Err != "":
			call.Err = fmt.Errorf(h.Err)
			err = c.c.ReadBody(nil)
			call.done()
		default:
			err = c.c.ReadBody(call.Reply)
			if err != nil {
				call.Err = errors.New("reading body " + err.Error())
			}
			call.done()
		}
	}
	// error occurs, so terminateCalls pending calls
	c.terminateCalls(err)
}

func NewClient(conn net.Conn, opt *trpc.Option) (*Client, error) {
	f := codec.CodecMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}
	// send options with server
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error: ", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(c codec.Codec, opt *trpc.Option) *Client {
	client := &Client{
		seq:     1, // seq starts with 1, 0 means invalid call
		c:       c,
		opt:     *opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOptions(opts ...*trpc.Option) (*trpc.Option, error) {
	// if opts is nil or pass nil as parameter
	if len(opts) == 0 || opts[0] == nil {
		return trpc.DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}
	opt := opts[0]
	opt.MagicNumber = trpc.DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = trpc.DefaultOption.CodecType
	}
	return opt, nil
}

// Create client instance based on network type address
func dialTimeout(f newClientFunc, network, address string, opts ...*trpc.Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}
	// close the connection if client is nil
	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()
	ch := make(chan clientResult)
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}
	select {
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}
}

// Dial connects to an RPC server at the specified network address
func Dial(network, address string, opts ...*trpc.Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}
func (c *Client) send(call *Call) {
	// make sure that the client will send a complete request
	c.sending.Lock()
	defer c.sending.Unlock()

	// register this call.
	seq, err := c.registerCall(call)
	if err != nil {
		call.Err = err
		call.done()
		return
	}

	// prepare request header
	c.header.SrvMod = call.SrvMod
	c.header.Seq = seq
	c.header.Err = ""

	// encode and send the request
	if err := c.c.Write(&c.header, call.Args); err != nil {
		call := c.removeCall(seq)
		// call may be nil, it usually means that Write partially failed,
		// client has received the response and handled
		if call != nil {
			call.Err = err
			call.done()
		}
	}
}

// Go invokes the function asynchronously.
// It returns the Call structure representing the invocation.
func (c *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panic("rpc client: done channel is unbuffered")
	}
	call := &Call{
		SrvMod: serviceMethod,
		Args:   args,
		Reply:  reply,
		Done:   done,
	}
	c.send(call)
	return call
}

// Call invokes the named function, waits for it to complete,
// and returns its error status.
// Blocking call.Done, waiting for the response to return, is a synchronous interface
func (c *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := c.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	case <-ctx.Done():
		c.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Err
	}
}

// MDial calls different functions to connect to a RPC server
// according the first parameter rpcAddr.
// rpcAddr is a general format (protocol@addr) to represent a rpc server
// eg, http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/thinrpc.sock
func MDial(rpcAddr string, opts ...*trpc.Option) (*Client, error) {
	parts := strings.Split(rpcAddr, "@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol, addr := parts[0], parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp", addr, opts...)
	default:
		// tcp, unix or other transport protocol
		return Dial(protocol, addr, opts...)
	}
}
