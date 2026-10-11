# 项目约束

- 唯一目标仓库为 jacklilyhello/TG-two-way-chatbot。业务基线见 docs/requirements-v4.md，最新用户指令优先。
- TypeScript + 原生 Cloudflare Worker、D1、Queues、Durable Objects；每 Bot 资源、Secrets、状态和发布完全独立。
- 纯人工双向业务，不引入智能问答、审查、模型、人机验证、RSS、网络诊断或服务器管理。
- 普通用户保持静默，只响应主动触发的固定 /start、/help。
- 保留管理员原件、群组历史与话题。删除只能作用于已知用户私聊消息。
- 外部请求结果未知不能盲目重发；发送、编辑、删除、反应分别记账。
- 密钥、真实实例私有配置和业务数据不得进入源码、提交、报告或日志。
- 本地 Cloudflare API 只读；本项目写入与部署通过 GitHub Actions。
- 不导入旧数据库；正常升级使用兼容 migration，不能清库。
