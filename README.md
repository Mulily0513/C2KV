# c2kv
基于SLMDB和raft的分布式kv存储
# 项目结构
```
├── api        //rpc接口
├── app        //应用层
├── c2kvctl    //客户端工具
├── client     //客户端工具
├── code       //错误码
├── config     //配置
├── db         //存储引擎层
├── deploy     //docker部署相关
├── log        //基于zap封装的日志框架   
├── raft       //raft算法层
├── scripts    //开发环境相关脚本
├── tools      //benchmark相关工具
├── transport  //网络传输层
└── utils      //工具库
```
# docker开发环境
```
git clone https://github.com/Mulily0513/C2KV
cd ./C2KV && make build
```
# 文档
https://zhuanlan.zhihu.com/p/27298952995

