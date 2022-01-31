# Raft算法实现

本系统实现参考In Search of an Understandable Consensus Algorithm(Extended Version)这篇论文的图2部分，并基于MIT 6.824 distributed system课程的测试框架完成了Leader Election功能，具体包括如下：
* 1.leader节点选举。

* 2.HeartBeat模块。

使用Go语言的go routine来实现并发，并使用了lock保护资源的并发读写访问，使用了timer控制节点的选举和心跳。

## leader节点选举
每个节点设置一个随机的定时器（150ms-300ms），定时器超时触发节点选举，从follower变成candidate。candidate通过rpc给其他的节点发送RequestVote，并收集其它节点发来的投票，如果获得（包括自身）超过半数的节点投票，节点选举完成，状态变为leader。

## HeartBeat模块
leader节点需要定期（每隔150ms）给followers发送HeartBeat防止新的选举产生，其它节点收到heartbeat后判断是否合法，判断通过后重置定时器。

# 代码测试通过：
<img width="859" alt="截屏2022-01-31 下午5 34 34" src="https://user-images.githubusercontent.com/40267804/151770012-f0280a85-f55a-4920-8438-f0433c7e55e5.png">

