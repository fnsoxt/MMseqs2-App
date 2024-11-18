# Msa-Server

## install golang 1.18
```
wget https://golang.google.cn/dl/go1.18.10.linux-amd64.tar.gz
tar -xzf go1.18.10.linux-amd64.tar.gz -C ~/go
export PATH=$PATH:~/go/bin
go version
```
## Usage
```
go build --mod=vendor -O msa-server
```
