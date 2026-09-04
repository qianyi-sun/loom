//go:build !linux

package main

import (
	"context"
	"errors"
)

var ErrCloneIntoCgroupUnsupported = errors.New("clone3 into cgroup unsupported")

type Process struct {
	PID              int
	ExecutableSHA256 string
	CgroupDevice     uint64
	CgroupInode      uint64
}

func LaunchInCgroup(context.Context, ExecutableMember, []string, []string, int) (*Process, error) {
	return nil, ErrCloneIntoCgroupUnsupported
}

func (p *Process) Wait() error {
	return nil
}

func (p *Process) Close() error {
	return nil
}
