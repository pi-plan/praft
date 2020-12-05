# PRaft

PRaft 是一个 Python 实现的 [Raft](https://raft.github.io/) [共识算法](https://en.wikipedia.org/wiki/Consensus_(computer_science))库。

## 实现功能

- [x] Leader 选举。
- [x] PreVote 逻辑
- [x] 日志复制。
- [x] 状态机快照与日志清理。
- [x] Raft 集群成员动态变更。
- [x] Leader 节点转移。
- [x] 无投票权成员 Leaner 状态实现。
- [x] 自定义网络网络协议、日志存储、状态机。
- [x] 异步 IO，提高效率。

## 为什么是库，而不是服务？

Raft 实现的服务，大多是配合其他服务，作为被依赖的服务存在。这种场景下网络通信协议、状态机、日志存储都希望沿用现有的实现，或者有自己定制化的需求。所以 PRaft 对这些做了抽象，使用的时候可以按照自己的需要实现相应的组件。具体的方法可以参考下面的文档。或者参考 demos。

## DEMO

demo 是一个基于 PRaft 实现的一个Key/Value 存储。用 HTTP 协议交互对外交互，URL 中的 Path 即为 Key，Body 即 Value。单实例也可以启动，不过建议多实例运行。

### 单实例

```
python3 demos/demo.py --debug

```

默认启动 `9023` 端口监听客户端请求集群请求。

```
❯ curl -X PUT <http://127.0.0.1:9023/test/key> -d "{\\\\"value\\\\":1}"
{"value": 1}

```

设置和更新值，Path `/test/key` 就是 Key，Body 必须是 JSON 格式。

```
❯ curl <http://127.0.0.1:9023/test/key>
{"value": 1}

```

获取 Key。

```
❯ curl -X DELETE <http://127.0.0.1:9023/test/key>

```

删除 Key。

### 多实例

```
python3 demos/demo.py --listen-url 127.0.0.1:9023  --initial-advertise-peers 127.0.0.1:9023,127.0.0.1:9024,127.0.0.1:9025 --debug
python3 demos/demo.py --listen-url 127.0.0.1:9024  --initial-advertise-peers 127.0.0.1:9023,127.0.0.1:9024,127.0.0.1:9025 --debug
python3 demos/demo.py --listen-url 127.0.0.1:9025  --initial-advertise-peers 127.0.0.1:9023,127.0.0.1:9024,127.0.0.1:9025 --debug

```

多实例启动的时候需要指定没个监听的地址，`--initial-advertise-peers` 参数需要指定群集中的所有成员，也必须包括自己。顺序也需要一致。
单实例一样，都可以通过 HTTP 协议对客户端提供服务，区别就在于如果请求的不是 Leader ，会返回 `302` 状态，跳转到 Leader 进行请求。
假如此时的 Leader 节点是 `127.0.0.1:9024`实例，客户端随机向集群中的一个节点发起请求。

```
❯ curl -i <http://127.0.0.1:9023/test/key>
HTTP/1.1 302 Found
Server: TornadoServer/6.0.4
Content-Type: text/html; charset=UTF-8
Date: Sat, 05 Dec 2020 14:24:26 GMT
Location: <http://127.0.0.1:9024/test/key>
Content-Length: 0

```

节点如果不是 Leader 会返回 `302` 状态让客户端请求 `http://127.0.0.1:9024` Leader 节点。客户端的其他操作都是一样的逻辑。

多实例集群还可以可以通过 HTTP 协议对集群进行管理和操作。
比如：

```
❯ curl -X PUT <http://127.0.0.1:9024/_praft/leadership_transfer>
{"transferring":true}

```

Leader 节点转移。当前 Leader 会从集群的剩余节点里选择一个日志完备的节点作为转移对象。

```
❯ curl -X PUT <http://127.0.0.1:9024/_praft/remove_node hostname=127.0.0.1&port=9023>

```

移除集群内节点，被移除的节点如果不是 Leader ，就会从当前集群内移除。不会参与日志复制和选举投票。

```
❯ python3 demos/demo.py --listen-url 127.0.0.1:9026  --join-url 127.0.0.1:9025 --debug

```

新节点假如集群。`--join-url` 参数可以是目标集群内的任意节点，新节点会自动寻找 Leader，新节点会作为 Learner 从集群内学习，当安装完镜像、同步日志之后才会转变为 Follwer 节点。

## 自定义

demo 里是基于 HTTP 协议进行交互的 Key/Value 系统。可以基于 PRaft 实现自己的系统，只需要实现 PRaft 的抽象类即可。

### 网络协议

实现 [praft.peer.Peer](https://github.com/laomafeima/praft/blob/master/praft/peer.py#L7) 接口即可。详细参考：[HTTPJsonPeer](https://github.com/laomafeima/praft/blob/master/demos/demo.py#L225)

### 日志存储

实现 [praft.log_storage.SnapshotData](https://github.com/laomafeima/praft/blob/master/praft/log_storage.py#L15) 接口即可。详细参考：[ListLogStorage](https://github.com/laomafeima/praft/blob/master/demos/demo.py#L270)

### 状态机

实现 [praft.state_machine.StateMachine](https://github.com/laomafeima/praft/blob/master/praft/state_machine.py#L7) 接口即可。详细参考：[DictStateMachine](https://github.com/laomafeima/praft/blob/master/demos/demo.py#L339)

## 参与

报告 bug，请提交一个 [Issue](https://github.com/laomafeima/praft/issues)。参与贡献改进及新功能, 请提交 [Pull request](https://github.com/laomafeima/praft/pulls) 并创建一个 Issue 以便讨论与进度追踪。
