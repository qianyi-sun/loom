package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestStartupReleaseBindsExactInstalledExecutable(t *testing.T) {
	for _,mode:=range []string{"valid","deleted","wrong_digest","outside","replacement","hardlink","symlink","writable_binary","writable_bin","writable_release","wrong_owner"} {
		t.Run(mode,func(t *testing.T){
			base:=t.TempDir()
			useTestConfigPolicy(t,base)
			digest:=strings.Repeat("a",64)
			root:=filepath.Join(base,digest)
			bin:=filepath.Join(root,"bin")
			if err:=os.MkdirAll(bin,0o755);err!=nil {t.Fatal(err)}
			path:=filepath.Join(bin,"loom-task-builder-supervisor")
			if err:=os.WriteFile(path,[]byte("running-inode"),0o555);err!=nil {t.Fatal(err)}
			file,err:=os.Open(path);if err!=nil {t.Fatal(err)};defer file.Close()
			observed:=path
			switch mode {
			case "deleted": observed+=" (deleted)"
			case "wrong_digest": observed=strings.Replace(path,digest,strings.Repeat("b",64),1)
			case "outside": observed=filepath.Join(base,"other","bin","loom-task-builder-supervisor")
			case "replacement":
				if err:=os.Rename(path,path+".old");err!=nil {t.Fatal(err)}
				if err:=os.WriteFile(path,[]byte("replacement"),0o555);err!=nil {t.Fatal(err)}
			case "hardlink": if err:=os.Link(path,path+".link");err!=nil {t.Fatal(err)}
			case "symlink":
				if err:=os.Rename(path,path+".old");err!=nil {t.Fatal(err)}
				if err:=os.Symlink(path+".old",path);err!=nil {t.Fatal(err)}
			case "writable_binary": if err:=os.Chmod(path,0o755);err!=nil {t.Fatal(err)}
			case "wrong_owner": requiredOwnerUID++
			}
			if err:=os.Chmod(root,0o555);err!=nil {t.Fatal(err)}
			if err:=os.Chmod(bin,0o555);err!=nil {t.Fatal(err)}
			defer os.Chmod(root,0o755);defer os.Chmod(bin,0o755)
			if mode=="writable_bin" {if err:=os.Chmod(bin,0o755);err!=nil {t.Fatal(err)}}
			if mode=="writable_release" {if err:=os.Chmod(root,0o755);err!=nil {t.Fatal(err)}}
			baseFD:=openDirectoryFD(t,base);defer syscall.Close(baseFD)
			got,err:=verifyInstalledSupervisor(baseFD,base,observed,int(file.Fd()))
			if mode=="valid" {if err!=nil || got!=digest {t.Fatalf("valid running inode rejected: %v",err)}} else if err==nil {t.Fatal("untrusted installed identity accepted")}
		})
	}
}

func TestStartupReleaseInstallTraversalRejectsUnsafeAncestors(t *testing.T) {
	base:=t.TempDir()
	useTestConfigPolicy(t,base)
	parent:=openDirectoryFD(t,base);defer syscall.Close(parent)
	if err:=os.Mkdir(filepath.Join(base,"safe"),0o755);err!=nil {t.Fatal(err)}
	if err:=os.Mkdir(filepath.Join(base,"unsafe"),0o777);err!=nil {t.Fatal(err)}
	if err:=os.Chmod(filepath.Join(base,"unsafe"),0o777);err!=nil {t.Fatal(err)}
	if err:=os.Symlink("safe",filepath.Join(base,"link"));err!=nil {t.Fatal(err)}
	for _,name:=range []string{"safe","unsafe","link","..","safe/.."} {
		fd,err:=openOwnedStartupDirectoryAt(parent,name,false)
		if fd>=0 {syscall.Close(fd)}
		if name=="safe" {if err!=nil {t.Fatal(err)}} else if err==nil {t.Fatalf("unsafe ancestor %s accepted",name)}
	}
}
