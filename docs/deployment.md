# 部署与实例配置

## 发布边界

唯一目标仓库是 `jacklilyhello/TG-two-way-chatbot`，使用一个 Cloudflare 账户和一套仓库级 Actions 部署凭据。本地 Cloudflare API/CLI 仅用于读取与诊断；创建资源、迁移、发布、Secrets 与维护写入均由 GitHub Actions 执行。只使用 `lilyya.top` 的子域；工作流在写入前核对 DNS、Worker Custom Domain 与路由占用，不接管无关项目。

一个实例对应一个 Bot。不要把一个 Token 放入多个活动实例，也不要在测试中使用正式 Bot Token。原 Bot 迁移时保留原 Token、原管理员 Forum 群组、原群内历史；旧数据库独立备份但不导入新 D1。

## 私有 Environment

正式实例与 Environment 固定对应如下，两个 Environment 均为新增，不把测试 Environment 改名或改作生产：

| `instance_id` | GitHub Environment | 脱敏模板 |
| --- | --- | --- |
| `prod-support` | `tg-two-way-prod-support` | `instances/prod-support.example.json` |
| `prod-ad` | `tg-two-way-prod-ad` | `instances/prod-ad.example.json` |

原有 `test-a` / `tg-two-way-test-a`、`test-b` / `tg-two-way-test-b` 及线上资源、数据、历史全部保留。所有 Environment 限制部署来源为 main，不为生产准备新增审批人或额外审核流程；未提供正式配置时可以先保留空 Environment。工作流本身也限制从 main 触发，发布版本必须是 main 历史中已经通过 main CI 的完整 40 位 SHA。合并 main 只运行 CI，不自动部署任何机器人；每次操作仅选择一个实例，实例之间可以运行不同发布 SHA。

应用源码和 migration 使用明确选择的 `release_sha`；部署检查与控制脚本固定使用本次从 main 触发时的完整提交 SHA。这样选择较早的兼容应用版本时，仍执行当前 main 的隔离与初始化保护，不会顺带回退安全检查。

复用仓库已配置的 `CLOUDFLARE_API_TOKEN` Secret 和 `CLOUDFLARE_ACCOUNT_ID` Variable，不为每个 Bot 重复配置 Cloudflare 凭据，不扩大已有 Token 权限。Cloudflare API Token 只用于 Actions 部署和维护，不安装进聊天 Worker。工作流使用账户 Token 的 `/accounts/{account_id}/tokens/verify` 接口；只读验证成功不代表 D1、Queues、Workers、Custom Domains 的写入权限已验证，实际权限不足时工作流会明确失败。

每个 Environment 使用以下相同 Secret 名称，分别填写自己的值；实例配置和 Telegram Secrets 不从其他 Environment 或机器人回退取得：

| 类型 | 名称 | 内容 |
| --- | --- | --- |
| Secret | `TELEGRAM_BOT_TOKEN` | 此实例独有的测试/正式 Bot Token |
| Secret | `TELEGRAM_WEBHOOK_SECRET` | 32–256 位随机字母、数字、下划线或连字符 |
| Secret | `INSTANCE_CONFIG_JSON` | 按模板填写的完整私有 JSON，建议紧凑单行格式 |
| Variable，可选 | `BACKUP_AGE_RECIPIENT` | 加密导出的 age 公钥；私钥离线保管 |

不要把真实 JSON 提交到公开仓库，也不要放入 GitHub Variable：Actions 会在步骤环境中显示普通 Variable，而 Secret 会被遮蔽。安全的本地临时文件可放在被忽略的 `.local/`；避免在终端命令、报告或截图中展开内容。Cloudflare Account ID 使用已存在的仓库 Variable，Environment 未同名覆盖时会继承它。

`instances/*.example.json` 仅演示独立资源集合。所有数字身份均为合成占位值，必须在私有 `INSTANCE_CONFIG_JSON` 中替换 `expected_bot_id`（该 Token 的 Bot ID）、`admin_chat_id`（此 Bot 的管理员 Forum 群组）、`admin_ids`（授权管理员账号）与实际选择的 `hostname`。生产模板故意使用相同占位管理员账号和不同占位群组：管理员账号可以相同，两个实例不能共用管理员 Forum 群组。示例域名不代表已确认可用，上线前须通过实际 DNS、Custom Domain 和路由检查。`instance_id` 必须与工作流选择一致，资源名必须严格为：

| 字段 | 规则 |
| --- | --- |
| `worker_name` | `tg-two-way-<instance_id>` |
| `d1_name` | `tg-two-way-<instance_id>-db` |
| `queue_name` | `tg-two-way-<instance_id>-jobs` |
| `dlq_name` | `tg-two-way-<instance_id>-dlq` |
| `hostname` | 未被占用的 `lilyya.top` 子域 |
| `recovery_epoch` | 初始为 `initial`；数据库恢复时使用全新代次 |

对应正式资源分别为 `tg-two-way-prod-support` / `tg-two-way-prod-ad` Worker，并各自附加 `-db`、`-jobs`、`-dlq` 后缀。不得把测试资源名或 ID 填到正式配置。

## 资源创建与 ID 回填

| 实际状态 | 私有配置要求 | 操作 |
| --- | --- | --- |
| 真正首次创建，Worker、D1、任务队列、失败队列均不存在 | 可以省略 `d1_id`、`queue_id`、`dlq_id` | `deploy`，勾选 `provision` |
| 部分创建后继续，已有部分或全部资源但 Worker 尚未安装 | 先填入每个已经存在资源的精确 ID；尚未创建的资源可以缺 ID | 只读核查后再次 `deploy`，即使三个 ID 都已填妥仍勾选 `provision` |
| 已有实例升级或维护 | 三个精确资源 ID 全部填写，Worker 身份与绑定一致 | `deploy` 或维护操作，不勾选 `provision` |

工作流在任何资源创建前检查全部已有资源：名称和配置 ID 必须精确匹配，队列生产者/消费者必须属于选中 Worker。不能仅凭同名自动接管。缺少回填时错误会列出缺失的 `d1_id`、`queue_id` 或 `dlq_id` 字段及后续步骤，不输出资源 ID、真实身份或完整私有 JSON；若名称与配置 ID 不符，先核对误选实例或配置，不尝试绕过。

首次创建完成或中途失败后：

1. 在同一 Cloudflare 账户控制台只读查找当前实例的精确 D1、任务队列和失败队列名称，确认身份与归属；也可复用已有本地只读 API 权限核查。
2. 将 D1 的准确 ID 填入 `d1_id`，任务队列 ID 填入 `queue_id`，失败队列 ID 填入 `dlq_id`。只回填已确认存在的资源；不要交换任务队列与失败队列。
3. 在对应 GitHub Environment 的 `INSTANCE_CONFIG_JSON` Secret 中安全保存完整 JSON，保留其他实例字段。可在 GitHub Secrets 界面粘贴，或使用已有 GitHub 登录权限从受保护的本地文件经标准输入更新 Secret；不要把 JSON 放入命令参数、日志、PR 或 Variable。仓库提供模板，不能取回已保存 Secret 的明文。
4. 重新执行前再次确认选中的 Environment、实例名和发布 SHA。Worker 尚未安装时，继续初始化使用 `provision`，即使三个资源都已创建并回填。只有 Worker 已安装、身份核对通过且所有资源 ID 回填后，后续升级和维护才关闭 `provision`。

不会为自动回填另设高权限 Token，部署脚本不会修改 GitHub Secrets。创建请求超时或结果未知时，先只读核对实际是否创建，确认后回填再继续；不盲目重建，不删除数据库、队列或未知结果留下的资源。

## 部署前实际隔离检查与初始化串行

模板检查只是第一层。部署前读取同一账户的实际 Worker 配置，核对选中 Worker 的实例、Bot、管理员群组与资源身份，并检查本项目其他实例是否占用同一 Bot 或管理员 Forum 群组。遇到冲突或项目 Worker 的归属无法确认时，在任何资源写入与 Webhook 修改前停止；按脱敏错误核对对应实例的私有配置及实际 Worker 绑定，不能以仓库示例未冲突替代云端核查。

Worker 的两个 DO 绑定始终指向本 Worker 的 `ConversationCoordinator` 和 `BotCoordinator`，不接受跨 Worker 的 namespace 引用。账户 Worker 绑定清单还用于拒绝其他实例或应用共用选中的数据库、队列和 DO namespace。每次上传 Secret 仅作用于目标 Worker；D1 中的业务设置、任务与历史也只属于该实例。

所有部署和维护保留按实例互斥。显式 `provision=true` 的首次创建或部分继续还必须取得本仓库单一账户的初始化锁：获得锁后才启动部署脚本，重新读取实际 Worker、数据库、队列、域名及 Telegram 状态，检查和后续创建均在锁内完成。工作流自动传递锁标记，脚本拒绝缺少标记的初始化写入。日常 `provision=false` 的已有实例升级仍仅按实例隔离。两层 concurrency 使用不同组避免自锁，均采用 `queue: max` 排队（最多 100 个等待项）及 `cancel-in-progress: false`，不以较新任务替换旧等待项或取消正在执行的部署；超出等待容量时查看 Actions 状态再安排运行。不要用重新运行替代对未知外部结果的核查。

## 首次发布步骤

1. 在选定 Environment 填写此实例三个 Secrets；核对 Bot ID、独立 Forum、明确授权管理员和未占用域名。测试只用测试 Bot，正式接管保留该 Bot 原 Token、原 Forum 和管理员历史。Bot 需要管理员、话题管理、置顶权限；群组须允许 👍 和 👎。
2. 完成 PR 与 CI，合并 main，等待合并 SHA 的 main CI 成功。
3. 从 main 运行 `Instance operations`，填写 `instance`、`release_sha`，选择 `deploy`，首次创建勾选 `provision`。
4. 设置 Webhook 时勾选 `configure_webhook`。首次接管或 URL 变更还须明确勾选 `old_consumer_stopped`：此前轮询/Webhook 消费者、旧出站任务与自动重启已停止，在途请求已经核对。无法确认时不要勾选。
5. 工作流先执行 `getMe`、`getChat`、`getChatMember`、`getWebhookInfo` 与资源占用检查，再创建/选择资源、应用兼容 migration，并通过精确 D1 绑定持久写入出站暂停。先上传 `PAUSED=true` 的版本，安装实例 Secrets，再上传解除环境暂停的版本，防止首次 cron 在密钥尚未安装时运行；随后核对健康版本与 Webhook。首次部署和升级的 D1 持久暂停闸门都保留。
6. `setWebhook` 明确使用 `drop_pending_updates: false`，仅订阅 `message`、`edited_message`、`callback_query`。不丢弃 Telegram 尚存的待处理更新。
7. 按上述步骤把三个准确资源 ID 回填到对应 Environment。关闭 `provision`，运行 `status` 核对版本、暂停、积压与已发出操作；必要时先 `recover`，核对未知结果，再在 Webhook 已确认归属本实例后显式运行 `resume`。未配置 Webhook 时保持暂停；`resume` 会拒绝空白或属于其他消费者的 Webhook URL。真实业务与故障验收另按[测试说明](testing.md)执行。

实例初始化时通过 Telegram API 设置命令列表：私聊只展示 `/start`、`/help`，管理员群组的管理员作用域展示 `/tj`、`/sc`、`/mb`。命令菜单设置异常会单独记账；管理员命令的安全性始终由服务端身份和场景校验保证。

## 升级发布

仍选择一个实例与明确 SHA，不使用批量发布。工作流先持久暂停出站并等待已发出的操作归零，再应用迁移与部署；存在未排清的已发出操作时保持暂停并失败，避免代码或 schema 变化掩盖未知副作用。

每次部署完成后都保持暂停。先运行 `status` 核对版本与积压，必要时运行 `recover`，确认未发现需要隔离的外部未知结果，再显式运行 `resume`；恢复前还会读取 Telegram Webhook，要求 URL 精确属于当前实例。维护暂停仍继续持久接收入站更新，队列和数据库保留。

现有实例的维护鉴权使用当前 Webhook Secret；不要在普通升级时同时更换该 Secret。密钥轮换需单独安排暂停、旧密钥鉴权、新 Secret 安装及 Webhook 更新，不能依赖一次未经验证的自动替换。

发布成功和 `/health` 正确仅说明版本已安装、D1 可访问，不代表 Telegram 双向业务已经实测通过。

## 接管旧机器人

每次只切换一个 Bot，遵循[运维切换清单](operations.md#正式接管旧机器人)。保留原 Token、原管理员 Forum 群组、原话题与群内消息；旧数据库独立备份但不导入新 D1。切换前停止旧轮询/Webhook 消费者、旧出站任务和自动重启，核对已发出的在途请求，未知结果不盲目重发。确认后才设置新 Webhook，保留 Telegram 待处理更新。部署完成仍保持暂停，状态核对后显式恢复，管理员在旧话题通过 `/tj TGID` 重新登记。

准备模板、Environment 或通过 CI 均不授权实际切换。实际停止旧系统、设置 Webhook、恢复出站与接管正式流量须在明确的切换窗口执行。
