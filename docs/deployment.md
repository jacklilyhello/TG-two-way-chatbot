# 部署与实例配置

## 发布边界

唯一目标仓库是 `jacklilyhello/TG-two-way-chatbot`。本地 Cloudflare API/CLI 仅用于读取与诊断；创建资源、迁移、发布、Secrets 与维护写入均由 GitHub Actions 执行。只使用 `lilyya.top` 的子域；工作流在写入前核对 DNS、Worker Custom Domain 与路由占用，不接管无关项目。

一个实例对应一个 Bot。不要把一个 Token 放入多个活动实例，也不要在测试中使用正式 Bot Token。原 Bot 迁移时保留原 Token、原管理员 Forum 群组、原群内历史；旧数据库独立备份但不导入新 D1。

## 私有 Environment

为实例创建 GitHub Environment：`tg-two-way-<instance_id>`。例如模板 `test-a` 对应 `tg-two-way-test-a`。应在 Environment 中限制部署分支为 main，并按团队需要设置审核人。工作流本身也限制从 main 触发，发布版本必须是 main 历史中已经通过 main CI 的完整 40 位 SHA。

复用仓库已配置的 `CLOUDFLARE_API_TOKEN` Secret 和 `CLOUDFLARE_ACCOUNT_ID` Variable，不在日志中取回或重复保存密钥明文。工作流使用账户 Token 的 `/accounts/{account_id}/tokens/verify` 接口；只读验证成功不代表 D1、Queues、Workers、Custom Domains 的写入权限已验证，实际权限不足时工作流会明确失败。

每个 Environment 单独配置：

| 类型 | 名称 | 内容 |
| --- | --- | --- |
| Secret | `TELEGRAM_BOT_TOKEN` | 此实例独有的测试/正式 Bot Token |
| Secret | `TELEGRAM_WEBHOOK_SECRET` | 32–256 位随机字母、数字、下划线或连字符 |
| Secret | `INSTANCE_CONFIG_JSON` | 按模板填写的完整私有 JSON，建议紧凑单行格式 |
| Variable，可选 | `BACKUP_AGE_RECIPIENT` | 加密导出的 age 公钥；私钥离线保管 |

不要把真实 JSON 提交到公开仓库，也不要放入 GitHub Variable：Actions 会在步骤环境中显示普通 Variable，而 Secret 会被遮蔽。安全的本地临时文件可放在被忽略的 `.local/`；避免在终端命令、报告或截图中展开内容。Cloudflare Account ID 使用已存在的仓库 Variable，Environment 未同名覆盖时会继承它。

`instances/test-a.example.json` 与 `test-b.example.json` 仅演示两个完全独立的资源集合。必须替换 `expected_bot_id`、`admin_chat_id`、`admin_ids`；这些示例是假值。`instance_id` 必须与工作流选择一致，资源名必须严格为：

| 字段 | 规则 |
| --- | --- |
| `worker_name` | `tg-two-way-<instance_id>` |
| `d1_name` | `tg-two-way-<instance_id>-db` |
| `queue_name` | `tg-two-way-<instance_id>-jobs` |
| `dlq_name` | `tg-two-way-<instance_id>-dlq` |
| `hostname` | 未被占用的 `lilyya.top` 子域 |
| `recovery_epoch` | 初始为 `initial`；数据库恢复时使用全新代次 |

首次创建时省略 `d1_id`、`queue_id`、`dlq_id`。首次创建完成或创建中途失败后，从 Cloudflare 控制台读取上述精确资源的 ID，写回私有配置中的这三个字段，再次执行。工作流不会仅凭名字接纳已存在的数据库或队列，也不会删除失败后留下的资源。这避免误用其他实例，也使未知创建结果不会被盲目重试。

Worker 的两个 DO 绑定始终指向本 Worker 的 `ConversationCoordinator` 和 `BotCoordinator`，不接受跨 Worker 的 namespace 引用。部署前读取账户 Worker 绑定清单，拒绝其他实例或应用共用选中的数据库、队列和 DO namespace。每次上传 Secret 仅作用于目标 Worker。

## 首次测试发布

1. 配置测试 Bot、独立测试 Forum、至少一名明确授权的管理员。Bot 需要管理员、话题管理、置顶权限；群组须允许 👍 和 👎。
2. 完成 PR 与 CI，合并 main，等待合并 SHA 的 main CI 成功。
3. 从 main 运行 `Instance operations`，填写 `instance`、`release_sha`，选择 `deploy`，首次创建勾选 `provision`。
4. 设置 Webhook 时勾选 `configure_webhook`。首次接管或 URL 变更还须明确勾选 `old_consumer_stopped`：此前轮询/Webhook 消费者、旧出站任务与自动重启已停止，在途请求已经核对。无法确认时不要勾选。
5. 工作流先执行 `getMe`、`getChat`、`getChatMember`、`getWebhookInfo` 与资源占用检查，再创建/选择资源、应用兼容 migration，并通过精确 D1 绑定持久写入出站暂停。先上传 `PAUSED=true` 的版本，安装实例 Secrets，再上传解除环境暂停的版本，防止首次 cron 在密钥尚未安装时运行；随后核对健康版本与 Webhook。首次部署和升级的 D1 持久暂停闸门都保留。
6. `setWebhook` 明确使用 `drop_pending_updates: false`，仅订阅 `message`、`edited_message`、`callback_query`。不丢弃 Telegram 尚存的待处理更新。
7. 把创建的精确资源 ID 写回私有配置。运行 `status` 核对版本与暂停状态，在 Webhook 已确认归属本实例后显式运行 `resume`，再按[测试说明](testing.md)完成真实业务与故障验收。未配置 Webhook 时保持暂停；`resume` 会拒绝空白或属于其他消费者的 Webhook URL。

实例初始化时通过 Telegram API 设置命令列表：私聊只展示 `/start`、`/help`，管理员群组的管理员作用域展示 `/tj`、`/sc`、`/mb`。命令菜单设置异常会单独记账；管理员命令的安全性始终由服务端身份和场景校验保证。

## 升级发布

仍选择一个实例与明确 SHA，不使用批量发布。工作流先持久暂停出站并等待已发出的操作归零，再应用迁移与部署；存在未排清的已发出操作时保持暂停并失败，避免代码或 schema 变化掩盖未知副作用。

每次部署完成后都保持暂停。先运行 `status` 核对版本与积压，必要时运行 `recover`，确认未发现需要隔离的外部未知结果，再显式运行 `resume`；恢复前还会读取 Telegram Webhook，要求 URL 精确属于当前实例。维护暂停仍继续持久接收入站更新，队列和数据库保留。

现有实例的维护鉴权使用当前 Webhook Secret；不要在普通升级时同时更换该 Secret。密钥轮换需单独安排暂停、旧密钥鉴权、新 Secret 安装及 Webhook 更新，不能依赖一次未经验证的自动替换。

发布成功和 `/health` 正确仅说明版本已安装、D1 可访问，不代表 Telegram 双向业务已经实测通过。
