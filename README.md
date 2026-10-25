# TG-two-way-chatbot

原生 Cloudflare 的 Telegram 纯人工双向聊天机器人。使用一个 GitHub 仓库、一个 Cloudflare 账户和共用 Actions 部署凭据；每个 Bot 使用独立 Worker、D1、任务队列、失败队列、Durable Objects、Telegram Secrets、业务设置和发布版本。

普通用户发送消息后，由管理员在 Forum 用户话题中人工回复。自动固定文案仅响应用户主动发送的 `/start`、`/help`；管理员还可经过两次确认发送广播。投递状态、封禁状态和内部错误均留在管理员侧。

## 功能

- 双向文字、图片、视频、音频、语音、文件、贴纸与相册；通过同一 Bot 的复制能力与 `file_id` 传递媒体。
- `/tj TGID` 注册用户，或在旧话题重新绑定；切换话题须确认，管理员历史及已有消息映射保留。
- 首次话题先发送并置顶资料卡，再投递用户消息；人工及关键词/安全正则自动封禁、管理员话题持久封禁记录、资料入口、引用与文本/说明/媒体编辑同步。
- `/sc` 仅删除回复对象对应的用户私聊消息；资料卡“清空此对话”通过确认快照逐条处理。
- `/mb` 统计、用户分页与查找、拦截规则管理、独立欢迎/帮助文案、时区设置。
- General `/mb` →“管理员广播”：选择全体合格用户或指定多个 TGID，逐步收集文字、单张图片或图文，展示完整内容及冻结接收人数，两次独立确认后通过持久任务发送；失败、未发送、未知和部分完成分别反映实际结果。
- Webhook 持久接收、去重、D1 任务账本、同用户协调、Bot 级限流和异常恢复；发送、编辑、删除、反应分别记账。
- 单实例 GitHub Actions 发布、维护暂停、恢复调度、加密数据库导出。

Telegram 的删除时限、用户隐私、媒体能力、群组权限和接口限制仍然适用。API 确认发送成功不代表已读；网络超时等未知结果不会自动重发。旧数据库不导入，旧话题通过 `/tj` 重新登记，新系统升级保留已积累的数据。

## 本地检查

使用 Node.js 22 LTS 与 npm。根目录 `wrangler.jsonc` 仅用于本地模拟和构建，含明确的假配置，不能用于生产发布。

```sh
npm ci
npm run check
node --test scripts/*.node-test.mjs
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

广播功能的发布与真实验收仅限 `test-a`、`test-b`，正式实例升级须另行明确授权。测试实例广播还必须配置私有 `BROADCAST_TEST_RECIPIENT_IDS`，只向名单内且当前符合条件的用户发送；缺失或无效时不发送，不能仅凭测试 Bot 身份推定所有注册用户均获授权。

正式实例固定为 `prod-support` / `tg-two-way-prod-support` 与 `prod-ad` / `tg-two-way-prod-ad`。保留现有 `test-a`、`test-b` 的 Environment、资源、数据与历史；正式 Environment 另建并限制来源为 main。管理员账号可以相同，管理员 Forum 群组必须独立。

四份 `instances/*.example.json` 中的身份均为占位假值，示例域名也未经可用性确认。每个 Environment 分别填写同名的 `TELEGRAM_BOT_TOKEN`、`TELEGRAM_WEBHOOK_SECRET`、`INSTANCE_CONFIG_JSON`，不回退到另一个实例的配置。仓库级 `CLOUDFLARE_API_TOKEN` Secret 与 `CLOUDFLARE_ACCOUNT_ID` Variable 共用，Cloudflare Token 只供 Actions 使用，不安装到聊天 Worker。缺少正式配置时可先建立空 Environment，发布会拒绝执行。

首次初始化显式启用 `provision`，在账户初始化锁内重新读取实际资源和管理员群组归属后创建；创建后把精确 `d1_id`、`queue_id`、`dlq_id` 安全回填到对应 Environment 的 `INSTANCE_CONFIG_JSON`。Worker 尚未安装时，即使资源已全部创建并回填，也须启用 `provision` 继续初始化。已有实例按实例互斥独立升级，允许两个实例运行不同版本；部署完成仍暂停，核对 `status` 和 Webhook 后显式 `resume`。完整步骤见部署文档。

- [部署与实例配置](docs/deployment.md)
- [管理员使用](docs/administration.md)
- [排错、升级、回滚与数据恢复](docs/operations.md)
- [测试与真实 Telegram 验收](docs/testing.md)
- [完整业务需求 V4.0](docs/requirements-v4.md)
