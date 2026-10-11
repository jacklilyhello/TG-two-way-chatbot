# TG-two-way-chatbot

原生 Cloudflare 的 Telegram 纯人工双向聊天机器人。每个 Bot 使用独立 Worker、D1、任务队列、失败队列、Durable Objects、Secrets 和发布版本；共享的只有源码。

普通用户发送消息后，由管理员在 Forum 用户话题中人工回复。用户端仅在主动发送 `/start`、`/help` 时收到固定文案；投递状态、封禁状态和内部错误均留在管理员侧。

## 功能

- 双向文字、图片、视频、音频、语音、文件、贴纸与相册；通过同一 Bot 的复制能力与 `file_id` 传递媒体。
- `/tj TGID` 注册用户，或在旧话题重新绑定；切换话题须确认，管理员历史及已有消息映射保留。
- 置顶资料卡、人工封禁/解禁、资料入口、引用与文本/说明/媒体编辑同步。
- `/sc` 仅删除回复对象对应的用户私聊消息；资料卡“清空此对话”通过确认快照逐条处理。
- `/mb` 统计、用户分页与查找、独立欢迎/帮助文案、时区设置。
- Webhook 持久接收、去重、D1 任务账本、同用户协调、Bot 级限流和异常恢复；发送、编辑、删除、反应分别记账。
- 单实例 GitHub Actions 发布、维护暂停、恢复调度、加密数据库导出。

Telegram 的删除时限、用户隐私、媒体能力、群组权限和接口限制仍然适用。API 确认发送成功不代表已读；网络超时等未知结果不会自动重发。旧数据库不导入，旧话题通过 `/tj` 重新登记，新系统升级保留已积累的数据。

## 本地检查

使用 Node.js 22 LTS 与 npm。根目录 `wrangler.jsonc` 仅用于本地模拟和构建，含明确的假配置，不能用于生产发布。

```sh
npm ci
npm run check
node --test scripts/deployment.node-test.mjs
node scripts/check-instances.mjs
npm run build
```

本地数据库初始化与运行：

```sh
npx wrangler d1 migrations apply DB --local
npm run dev
```

本地模拟所需的 Secret 只放在被忽略的 `.dev.vars` 中。使用独立测试 Bot，不能使用正式 Bot Token；`wrangler dev` 中的 Telegram 请求仍可能产生真实外部效果。真实配置、数据库和导出文件不要加入 Git。

## 部署与管理

Cloudflare 写入仅通过本仓库 GitHub Actions 执行。`CI` 只检查代码；合并不会自动发布全部实例。`Instance operations` 每次明确选择一个实例及已通过 main CI 的完整提交 SHA。

实例模板中的所有身份均为假值。首次使用需在私有 GitHub Environment 中配置测试 Bot、Forum 群组、授权管理员与 Secrets；未配置时发布会拒绝执行。仓库不提供默认可接管的真实 Bot。

- [部署与实例配置](docs/deployment.md)
- [管理员使用](docs/administration.md)
- [排错、升级、回滚与数据恢复](docs/operations.md)
- [测试与真实 Telegram 验收](docs/testing.md)
- [完整业务需求 V4.0](docs/requirements-v4.md)
