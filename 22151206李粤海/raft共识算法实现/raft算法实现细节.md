#### raft概述

raft算法是一个可理解性强的，基于leader选举的共识算法，常用于私有链共识中。这次关于raft的实现是使用了mit 6.824分布式课程的测试框架，在其rpc机制下，需要在raft libarary中提供Make，Start，GetState和ApplyMsg接口。主要的实现逻辑集中在RequestVoteRPC，AppendentriesRPC上完成raft节点之间的信息交互。此外，通过协程+定时器的方式定期进行appendentries的发送（包括心跳）、重选举的检查以及将commited的log apply的操作。



本次对raft的实现主要完成了A,B,C三个模块，也就是RequestVoteRPC，AppendentriesRPC和Persist模块。



实现要求可参见：

https://pdos.csail.mit.edu/6.824/labs/lab-raft.html

测试代码库地址为：

```
git://g.csail.mit.edu/6.824-golabs-2021 6.824
```



从上述仓库中下载6.824的实验测试框架代码后，打开raft文件夹，可看到如下文件：

![image-20211227142515897](raft算法实现细节.assets\image-20211227142515897.png)

其中我们需要完成raft的实现，对test_test提供make(),start()，getstate()三个接口，以及applych的信息传递。

config是tester的底层配置实现，persister是持久化对象的底层实现，util则是它们提供用于debug用的DPrinf函数

这个文件夹对应的目录为./myraft/src/raft

接下来我们就raft的三部分实现展开说明。



#### RequestVoteRPC

这个rpc主要实现的就是某个raft节点触发了electionTimeout的超时后，就会升级成为candidate。然后candidate需要和其他节点进行交互，要求其他节点对他进行投票。当它获得大多数的票数后，它就可以升级成为一个leader。



==1.raft结构体==

我们首先需要根据raft论文的描述，填充raft结构体中的属性



==2.make函数实现raft节点初始化==

首先进行各属性的初始化，然后拉起checkElection(选举定时器)、checkAppendEntries（心跳/日志定时器）、checkApplied（将committed转为applied定时器）的三个goroutine



==3.checkElection Ticker的实现==

step1：定义在一定范围内随机的electionTimeout

step2：若超过时间限制，那么当前rf的状态会变成candidate（前提它不是leader）



==4.stateChange的实现==

step1：参数中需要包含更新后的状态，以及是否重置选举超时

step2：若变成follower，简单重置必要的属性即可

step3：若变成candidate，重置必要的属性后要发起选举(takePartInElection)

step4：若变成leader，更新必要的变量，包括nextIndex和matchIndex



==5.takePartInElection的实现==

step1：遍历处自己外的raft节点，对每个peers拉起一个goroutine

step2：通过requestVoteRpc进行输入以及通过交互得到输出参数

step3：判断candidate的state或term在传播rpc的时间中是否改变

step4：若candidate获得了投票并且rpc的term和目前的相等（期待的情况），判断是否超过半数票（是则变成leader）

step5：若candidate收到的rpc回答的Term大于传过去的Term，candidate更新term并降级为follower



==6.RequestVoteRpc 处理器的实现==

step1：若candidate的term比当前rf的currentTerm小，则不投票

step2：若candidate的term比当前rf的currentTerm大，当前rf降级为follower，且更新currentTerm

step3：若candidate的log不满足论文所属的“UP TO DATE",则不投票

step4：这时候确保了当前rf的currentTerm和requestvote传过来的args.term是一样的，若当前rf已经给别人投过票了，则不再投票

step5：其余情况正常投票，并更新选举超时时间



#### AppendentriesRPC

appendentries主要实现了leader发送心跳，以及统一log信息的功能。



==1.checkAppendEntries的实现==

leader每隔心跳的时间间隔进行entries的检查和加入，也就是leaderAppendEntries



==2.leaderAppendEntries的实现==

step1：遍历处自己外的raft节点，对每个peers拉起一个goroutine

step2：检查是否保持leader状态

step3：检查该server的prevLogIndex是否合法

step4：若leader的lastindex和大于等于follower的nextindex，leader将多出来的log塞进去appendEntriesRpc的参数中；否则，参数中的entries信息为空

step5：进行rpc交互并得到回应

step6：再次检查是否保持leader状态

step7：若follower返回的term更大，leader更新term并降级为follower

step8：若server成功append，则leader更新matchindex和nextIndex，同时改变commit的状态

step9：若server进行append失败了，找到回应中的confilictIndex并将其存入该rf的nextIndex中，方便下次继续查询并追加



==3.changeCommitIndex的实现==

step1：若当前rf为follower，则将rf.commitIndex更新到leaderCommit和自己最后一个index中的最小值

step2：若当前rf是leader，找到最大的index使得超过半数的peers的matchIndex都大于等于index，这即为leader新的commitIndex



==4.AppendEntriesRpc 处理器的实现==

step1：若leader给的term比我自己的还小， 返回false和自己的term

step2：若leader给的term大于等于自己的，则更新自己的状态和electionTimeout

step3：检查prevlogIndex的合法性

step4：若rf缺失了一部分信息，修改返回的ConflictIndex的值为rf最后的Idx

step5：如果rf上prevlogindex对应的term和RPCargs的不匹配，rf把该term的log全部视为conflict，然后传回上一个term的ConflictIndex

step6：若prelogindex前已经一致，从prelogindex后加入logs



#### checkApplied Ticker（）的实现

step1：若rf的lastApplied大于等于commitIndex，则都提交完了，不需再提交

step2：将lastApplied到commitIndex这段的logs传给channel（给tester）



#### Persist模块

step1：实现persistData函数和persist函数完成需要持久化数据的编码

step2：实现readPersist函数完成持久化数据的解码

step3：在上述实现的所有更改持久化属性的地方进行持久化操作即可









