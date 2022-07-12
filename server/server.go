package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/mindcarver/thin-rpc/codec"
	"github.com/mindcarver/thin-rpc/service"
)

// invalidRequest is a placeholder for response argv when error occurs
var invalidRequest = struct{}{}

const (
	connected        = "200 Connected to Thin RPC"
	defaultRPCPath   = "/_tprc_"
	defaultDebugPath = "/debug/trpc"
	MagicNumber      = 0x3bef5c
)

type Option struct {
	MagicNumber    int           // MagicNumber marks this's a thinrpc request
	CodecType      codec.Type    // client may choose different Codec to encode body  // todo change to interface
	ConnectTimeout time.Duration // 0 means no limit
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.Gob,
	ConnectTimeout: time.Second * 10,
}

// Server represents an RPC Server.
type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

var DefaultServer = NewServer()

func (srv *Server) Accept(l net.Listener) {
	// The for loop waits for the socket connection
	// to be established and starts the sub-coroutine processing
	for {
		conn, err := l.Accept()
		if err != nil {
			log.Println("rpc server: accept error:", err)
			return
		}
		go srv.ServeConn(conn) // Handle conn
	}
}

func (srv *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() {
		_ = conn.Close()
	}()

	var opt Option
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}

	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}

	f := codec.CodecMap[opt.CodecType]
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}
	srv.serveCodec(f(conn), &opt)
}

func (srv *Server) serveCodec(c codec.Codec, opt *Option) {
	var (
		sending sync.Mutex
		wg      sync.WaitGroup
	)

	for {
		req, err := srv.readRequest(c)
		if err != nil {
			if req == nil {
				break // it's not possible to recover, so close the connection
			}
			req.h.Err = err.Error()
			srv.sendResponse(c, req.h, invalidRequest, &sending)
			continue
		}
		wg.Add(1)
		go srv.handleRequest(c, req, &sending, &wg, opt.HandleTimeout)
	}
}

type request struct {
	h            *codec.Header  // header of request
	argv, replyv *reflect.Value // argv and replyv of request
	mType        *service.MethodType
	svc          *service.Service
}

func (r *request) MType() *service.MethodType {
	return r.mType
}
func (r *request) ARgv() reflect.Value {
	return *r.argv
}
func (r *request) ReplyV() reflect.Value {
	return r.ReplyV()
}

func (srv *Server) readRequestHeader(c codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := c.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (srv *Server) readRequest(c codec.Codec) (*request, error) {
	h, err := srv.readRequestHeader(c)
	if err != nil {
		return nil, err
	}
	req := &request{h: h}
	req.svc, req.mType, err = srv.findService(h.SrvMod)
	if err != nil {
		return req, err
	}
	req.argv = req.mType.NewArgv()
	req.replyv = req.mType.NewReplyv()

	// make sure that argvi is a pointer, ReadBody need a pointer as parameter
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}
	if err = c.ReadBody(argvi); err != nil {
		log.Println("rpc server: read body err:", err)
		return req, err
	}
	return req, nil
}

func (srv *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {
	defer wg.Done()
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		err := req.svc.Call(req.MType(), req.ARgv(), req.ReplyV())
		called <- struct{}{}
		if err != nil {
			req.h.Err = err.Error()
			srv.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		srv.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}
	select {
	case <-time.After(timeout):
		req.h.Err = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		srv.sendResponse(cc, req.h, invalidRequest, sending)
	case <-called:
		<-sent
	}
}

func (srv *Server) sendResponse(c codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := c.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func Accept(l net.Listener) { DefaultServer.Accept(l) }

// Register publishes in the server the set of methods of the
func (srv *Server) Register(rcvr interface{}) error {
	s := service.NewService(rcvr)
	if _, dup := srv.serviceMap.LoadOrStore(s.Name(), s); dup {
		return errors.New("rpc: service already defined: " + s.Name())
	}
	return nil
}

// Register publishes the receiver's methods in the DefaultServer.
func Register(rcvr interface{}) error { return DefaultServer.Register(rcvr) }

// Service.Method
func (srv *Server) findService(serviceMethod string) (svc *service.Service, mType *service.MethodType, err error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	svci, ok := srv.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}
	svc = svci.(*service.Service)
	mType = svc.MethodType(methodName)
	if mType == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

// ServeHTTP implements an http.Handler that answers RPC requests.
func (srv *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ", req.RemoteAddr, ": ", err.Error())
		return
	}
	_, _ = io.WriteString(conn, "HTTP/1.0 "+connected+"\n\n")
	srv.ServeConn(conn)
}

// HandleHTTP registers an HTTP handler for RPC messages on rpcPath.
// It is still necessary to invoke http.Serve(), typically in a go statement.
func (srv *Server) HandleHTTP() {
	http.Handle(defaultRPCPath, srv)
}

// HandleHTTP is a convenient approach for default server to register HTTP handlers
func HandleHTTP() {
	DefaultServer.HandleHTTP()
}
