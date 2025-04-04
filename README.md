## 防撤回和消息记录插件，仅支持项目 [dify-on-wechat](https://github.com/hanfangyuan4396/dify-on-wechat) 的gewechat协议类型

## 功能演示






## 功能特性
- 一个用于防止微信消息撤回的插件。当检测到消息被撤回时, 会将原消息转发给指定接收者。受接口限制目前仅支持文本和图片
- 记录群聊内的聊天记录
- 记录群成员的最后发言时间

## 安装方式


1. 克隆本想项目并将插件目录复制到 `plugins/` 下
3. 配置 `config.json` 中的接收者信息和缓存目录名称
3. 重启应用


## 配置说明

```json
{
    "receiver": {
        "type": "wxid",  //类型，目前仅支持wxid，格式类似这样 wxid_xxxxxxxxxxxxxx
        "name": "wxid_xxxxxxxxxxxxxx" //撤回信息的提示的收信人的wxid
    },
    "message_expire_time": 120, //消息缓存时间，此处为缓存到内存的时间，群聊记录保存在txt文件里长久缓存
    "cleanup_interval": 2, //清理间隔
    "chat_log_dir": "chat_logs",//群聊信息缓存目录，dify-on-wechat/chat_logs
     "last_spoken_command": "最后发言时间" //群聊中触发词
} 
```

## 注意事项

1. 确保接收者wxid配置正确,否则无法转发撤回消息
2. 合理设置消息缓存时间,避免占用过多内存
3. 图片文件缓存在dify-on-wechat/tmp目录，群聊天记录默认缓存在dify-on-wechat/chat_logs目录，建议定期清理

## 更新日志

### v1.0 20250401
- 初始版本发布
