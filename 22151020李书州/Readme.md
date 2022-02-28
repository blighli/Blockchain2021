本次Raft算法的实现基于MIT中6.824课程给出的框架，对细节进行填充完成了其Lab2相关的实验（Lab2A（领导人选举和心跳机制）， Lab2B（日志记录）， Lab2C(服务器的持久化与重启)，Lab2D（日志清理与版本快照）），并且通过了测试。下面给出课程的相关链接。

[课程的链接](https://pdos.csail.mit.edu/6.824)

[Lab2](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html)

如要运行相关测试，可进入./src/raft中执行命令

```go test -race```

其中Go语言版本为1.15



