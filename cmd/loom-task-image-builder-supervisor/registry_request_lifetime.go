package main

import (
	"context"
	"io"
	"net"
	"sync"
)

type registryRequestScopeKey struct{}

// Prevent detached HTTP body readers from retaining a reusable upload chunk
// after request cleanup. Deliberately do not expose io.WriterTo fast paths.
type registryRequestBody struct {
	mu     sync.Mutex
	reader io.Reader
}

func (b *registryRequestBody) Read(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reader == nil {
		return 0, io.EOF
	}
	return b.reader.Read(p)
}
func (b *registryRequestBody) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reader = nil
	return nil
}

// net/http can detach dialing from request cancellation and return an early
// response before its writer exits. This owner fences late dials and joins all
// borrowers before request returns and the source may rotate/unmap credentials.
type registryRequestScope struct {
	mu          sync.Mutex
	dials       sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	closed      bool
	token       []byte
	connections []*registryAuthorizationConn
}

func newRegistryRequestScope(ctx context.Context, token []byte) *registryRequestScope {
	ctx, cancel := context.WithCancel(ctx)
	return &registryRequestScope{ctx: ctx, cancel: cancel, token: token}
}

func registryScopeFromContext(ctx context.Context) *registryRequestScope {
	scope, _ := ctx.Value(registryRequestScopeKey{}).(*registryRequestScope)
	return scope
}

func (s *registryRequestScope) dial(connect func(context.Context) (net.Conn, error)) (net.Conn, error) {
	if s == nil {
		return nil, errRegistryTransport
	}
	s.mu.Lock()
	if s.closed || s.ctx.Err() != nil {
		s.mu.Unlock()
		return nil, errRegistryTransport
	}
	s.dials.Add(1)
	s.mu.Unlock()
	defer s.dials.Done()
	conn, err := connect(s.ctx)
	if err != nil {
		if conn != nil {
			conn.Close()
		}
		return nil, errRegistryTransport
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.ctx.Err() != nil || conn == nil || len(s.token) == 0 {
		if conn != nil {
			conn.Close()
		}
		return nil, errRegistryTransport
	}
	wrapped := newRegistryAuthorizationConn(conn, s.token)
	s.connections = append(s.connections, wrapped)
	return wrapped, nil
}

// Close has one sequential request owner. Connection.Close itself also supports
// concurrent transport closes and blocks until its token writer has returned.
func (s *registryRequestScope) Close() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	connections := s.connections
	s.connections = nil
	s.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
	}
	s.dials.Wait()
	s.mu.Lock()
	s.token = nil
	s.mu.Unlock()
}
