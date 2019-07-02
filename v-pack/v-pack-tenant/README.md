# 多租户插件使用说明

## 1. 多租户
请求前添加租户Header则为租户级响应返回。
```json
curl -H AppName:{AppName} http://localhost:9200/xxx
```

## 2. Restful 加密鉴权
  
默认系统配置
```json
vpack.security.enabled: false
vpack.security.username: admin
vpack.security.password: admin
```
  
各配置支持运行期动态修改
```json
PUT /_cluster/settings
{
    "transient" : {
        "vpack.security.enabled" : true
    }
}
```

## 3. 租户级限流

- 添加限流规则
```json
PUT _limit/{tenant}
{
    "index.max_bytes": "20M",
    "search.max_times" : 1000,
    "search.max_times.index1*" : -1
}
```
  - tenant: 租户名，当tenant=_global时为全局限流。优先级：租户级限流 > 全局限流
  - index.max_bytes:写入流量/每秒(shard级) 
  - 限流项为负数时相当于完全屏蔽. 如： index.max_bytes:-1   
  - search.max_times:查询次数/每秒(shard级)
  - search.max_times.index1* 索引级查询限流。

- 删除限流规则
```
DELETE _limit/{tenant}
```

- 获取限流规则
```json
GET _limit
GET _limit/{tenant}
```


- 限流总阀门
```json
PUT /_cluster/settings
{
    "transient" : {
        "vpack.limiter.enabled" : false
    }
}
``` 
  - vpack.limiter.enabled : 默认 true