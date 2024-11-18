# Msa-Server

## install golang 1.18
```
wget https://golang.google.cn/dl/go1.18.10.linux-amd64.tar.gz
tar -xzf go1.18.10.linux-amd64.tar.gz -C ~/
export PATH=$PATH:~/go/bin
go version
```

## conda install
```
conda install anaconda::go
go version
```

## Usage
```
mkdir msa-server
tar -xzf msa-server.tar.gz -C msa-server
cd msa-server
go build --mod=vendor -o msa-server
```
