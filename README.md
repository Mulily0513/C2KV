# C2KV
基于raft和lsm-tree存储引擎的分布式kv存储
# 工程目录结构
~~~
├─app      //应用层的功能主要是对外提供kv存储接口以及与算法层和存储层进行交互
├─bin      //配置文件和一些脚本文件
├─client   //客户端工具
├─code     //全局自定义的错误码
├─config   //kv存储配置相关model
├─db       //存储层，负责持久化日志，用户数据以及一些必要的状态信息
│  ├─arenaskl   //无锁跳表
│  ├─iooperator //io方式包括direct io和mmap
│  ├─marshal    //kv的序列化和反序列化
│  ├─mocks
│  ├─partition
│  └─wal
├─log        //主要是对zap日志库进行了二次封装，支持普通log和sugaredLog，前者性能更高
├─pb         //proto文件 
├─raft       //raft相关
├─transport  //传输层，负责集群内部网络传输相关
└─utils
└─dokcerfile-dev    //线上debug环境
└─dokcercompose-dev //线上debug环境
~~~
# 项目文档
https://www.yuque.com/u32260789/dsbrhh/hw6a6yowgpgz275v?singleDoc# 《C2KV》


