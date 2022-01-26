# Raft 实现

## 介绍

基于 [MIT6.824](https://pdos.csail.mit.edu/6.824/index.html) 课程的 [Lab2](https://pdos.csail.mit.edu/6.824/labs/lab-raft.html) 实现了一个 Raft 系统。

完成并通过了实验中的 2A 2B 2C 2D 部分，实现的功能包括：

- **leader election** 领导人选举。在规定时间内选举出当前任期内的唯一领导人；领导人可通过心跳提醒维持它的统治；当旧领导人失效，新的领导人会被重新选举。
- **log** 日志管理。领导人会将日志复制到其它节点，并保证日志的一致。
- **persistence** 持久化。将关键信息持久化存储，当节点宕机再重启时，可从中恢复到原先状态。
- **log compaction** 日志压缩。通过快照的方式压缩日志，并在不同节点间保持快照的一致。

主要实现代码在 `raft/` 目录下，各模块说明如下：

- `raft.go` Raft 核心模块，包含：数据结构的定义、对外接口的定义。
- `follower.go` follower 状态模块。
- `candidate.go` candidate 状态模块，包含当前状态下相关数据的初始化、行为的定义。
- `leader.go` leader 状态模块，同样包含当前状态下相关数据的初始化、行为的定义。
- `request_vote.go` RequestVote RPC 模块，包含：请求参数、响应参数的定义，请求的处理，请求的发送。
- `append_entries.go` AppendEntries RPC 模块，包含内容同上。
- `install_snapshot.go` InstallSnapshot RPC 模块，包含内容同上。
- `log.go` 日志模块，包含：日志条目的定义，日志相关行为的定义。

## 运行

运行环境：

- Linux
- go1.15

进入对应目录：

```bash
cd raft
```

运行全部测试：

```bash
go test
```

输出样例：

```log
Test (2A): initial election ...
  ... Passed --   3.1  3   30    8189    0
Test (2A): election after network failure ...
  ... Passed --   4.4  3   74   14813    0
Test (2A): multiple elections ...
  ... Passed --   6.2  7  342   69896    0
Test (2B): basic agreement ...
  ... Passed --   1.6  3   16    4416    3
Test (2B): RPC byte count ...
  ... Passed --   4.8  3   48  114028   11
Test (2B): agreement despite follower disconnection ...
labgob warning: Decoding into a non-default variable/field Term may not work
  ... Passed --   7.4  3   91   23047    8
Test (2B): no agreement if too many followers disconnect ...
  ... Passed --   4.2  5  133   27865    3
Test (2B): concurrent Start()s ...
  ... Passed --   0.8  3    8    2220    6
Test (2B): rejoin of partitioned leader ...
  ... Passed --   7.6  3  117   28886    4
Test (2B): leader backs up quickly over incorrect follower logs ...
  ... Passed --  47.4  5 2246 1705124  102
Test (2B): RPC counts aren't too high ...
  ... Passed --   2.4  3   24    7036   12
Test (2C): basic persistence ...
  ... Passed --   5.6  3   61   15785    6
Test (2C): more persistence ...
  ... Passed --  20.1  5  717  149590   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   2.8  3   30    7520    4
Test (2C): Figure 8 ...
  ... Passed --  32.3  5  706  144275   22
Test (2C): unreliable agreement ...
  ... Passed --  16.6  5  365  117948  256
Test (2C): Figure 8 (unreliable) ...
  ... Passed --  36.3  5 2083 4430942   58
Test (2C): churn ...
  ... Passed --  16.6  5  331  304284  210
Test (2C): unreliable churn ...
  ... Passed --  16.4  5  540  205981  156
Test (2D): snapshots basic ...
  ... Passed --  13.2  3  132   49418  251
Test (2D): install snapshots (disconnect) ...
  ... Passed --  98.7  3 1243  334110  383
Test (2D): install snapshots (disconnect+unreliable) ...
  ... Passed --  107.6  3 1474  372470  381
Test (2D): install snapshots (crash) ...
  ... Passed --  55.9  3  567  163990  388
Test (2D): install snapshots (unreliable+crash) ...
  ... Passed --  70.5  3  741  203537  371
PASS
ok      6.824/raft      582.243s
```

测试某个部分（比如2D）：

```bash
go test -run 2D
```

输出相关调试信息：

```bash
sed -i 's/Debug = false/Debug = true/g' util.go
go test -run 2D > log.log
```
