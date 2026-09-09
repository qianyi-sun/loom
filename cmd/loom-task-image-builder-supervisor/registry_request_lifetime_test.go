package main

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"net/url"
	"sync"
	"testing"
	"time"
)

// A scheduler-paused token write may outlive an early HTTP response. Closing
// network I/O wakes the writer, but only a join proves its borrowed bytes dead.
type earlyRegistryResponseConn struct {
	token []byte
	borrowed, release, closed chan struct{}
	once sync.Once
	reader *bytes.Reader
}
func (c *earlyRegistryResponseConn) Read(p []byte)(int,error) { <-c.borrowed; return c.reader.Read(p) }
func (c *earlyRegistryResponseConn) Write(p []byte)(int,error) {
	if len(p)>0 && &p[0]==&c.token[0] { close(c.borrowed); <-c.release; return 0,net.ErrClosed }
	return len(p),nil
}
func (c *earlyRegistryResponseConn) Close()error { c.once.Do(func(){close(c.closed)}); return nil }
func (c *earlyRegistryResponseConn) LocalAddr()net.Addr{return nil}
func (c *earlyRegistryResponseConn) RemoteAddr()net.Addr{return nil}
func (c *earlyRegistryResponseConn) SetDeadline(time.Time)error{return nil}
func (c *earlyRegistryResponseConn) SetReadDeadline(time.Time)error{return nil}
func (c *earlyRegistryResponseConn) SetWriteDeadline(time.Time)error{return nil}

func TestRegistryRequestJoinsEarlyResponseTokenWriter(t *testing.T) {
	token:=[]byte("review.private.signature")
	conn:=&earlyRegistryResponseConn{token:token,borrowed:make(chan struct{}),release:make(chan struct{}),closed:make(chan struct{}),
		reader:bytes.NewReader([]byte("HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"))}
	transport:=&http.Transport{DisableKeepAlives:true,DialContext:func(ctx context.Context,_,_ string)(net.Conn,error){
		return registryScopeFromContext(ctx).dial(func(context.Context)(net.Conn,error){return conn,nil})
	}}
	defer transport.CloseIdleConnections()
	origin,_:=url.Parse("http://example.test")
	credential := &RegistryCredential{BearerToken:token,ExpiresAt:time.Now().Add(time.Minute)}
	s:=registryUploadSession{policy:RegistryUploadPolicy{origin:origin},credential:credential,client:&http.Client{Transport:transport}}
	done:=make(chan error,1)
	go func(){_,err:=s.request(context.Background(),"PUT",origin,nil,"","");done<-err}()
	select { case <-conn.closed: case <-time.After(time.Second): close(conn.release);t.Fatal("early response did not close network") }
	// Network close is observed; deliberately keep its writer scheduled out.
	select { case <-done: close(conn.release); t.Fatal("request returned with borrowed credential writer live"); case <-time.After(20*time.Millisecond): }
	close(conn.release)
	select { case <-done: case <-time.After(time.Second):t.Fatal("request did not join writer") }
}

func TestRegistryRequestClosesAndJoinsDetachedDial(t *testing.T) {
	scope:=newRegistryRequestScope(context.Background(),[]byte("private.signature"))
	started,release:=make(chan struct{}),make(chan struct{})
	dialed:=make(chan error,1)
	go func(){_,err:=scope.dial(func(ctx context.Context)(net.Conn,error){close(started);<-ctx.Done();<-release;return nil,ctx.Err()});dialed<-err}()
	<-started
	closed:=make(chan struct{})
	go func(){scope.Close();close(closed)}()
	<-scope.ctx.Done()
	if _,err:=scope.dial(func(context.Context)(net.Conn,error){t.Error("late detached dial admitted");return nil,nil});err==nil {t.Error("closed scope dial accepted")}
	select {case <-closed:close(release);t.Fatal("scope did not join dial");case <-time.After(20*time.Millisecond):}
	close(release)
	<-dialed
	<-closed
	if scope.token!=nil {t.Fatal("closed request retained token borrow")}
}
