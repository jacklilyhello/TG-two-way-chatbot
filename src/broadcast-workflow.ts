import type {Env,Job,Params,TGMessage,TGUpdate} from './types';
import {all,now,one,run,setting,stmt} from './db';
import {call,notify} from './operations';
import {closeRuleSession,ruleInputEligible} from './rule-workflow';
import {bodyParts,prepareBroadcast,launchBroadcast,getBroadcast,broadcastStats,cancelBroadcast,normalizeBroadcastAudience,parseBroadcastIds,type BroadcastAudience,type BroadcastRow} from './broadcast-delivery';

export type BroadcastStage='audience'|'audience_ids'|'content'|'photo'|'text'|'preview'|'second_confirm'|'done';
export interface BroadcastSession {instance_id:string;actor_id:string;epoch:string;session_id:string;stage:BroadcastStage;revision:number;payload:string;transition_key:string;expires_at:number}
export interface BroadcastInputAdmission {instance:string;actor:string;session:string;revision:number;stage:BroadcastStage;expiresAt:number;epoch:string}
export interface BroadcastAction {token:string;kind:string;actor_id:string;thread_id:number|null;used:number;expires_at:number;payload:string}
interface Draft {text?:string;photo?:string;campaign?:string;audience?:BroadcastAudience}
interface ButtonSpec {text:string;kind:string;payload?:Params}
type Button={text:string;callback_data:string};
const TTL=15*60_000;
const allowed=(e:Env,actor:string)=>e.ADMIN_IDS.split(/[\s,]+/).includes(actor);
export async function broadcastSession(e:Env,actor:string):Promise<BroadcastSession|null> {
 return one(e,'SELECT * FROM broadcast_sessions WHERE instance_id=? AND actor_id=? AND epoch=?',e.INSTANCE_ID,actor,e.RECOVERY_EPOCH);
}
export async function captureBroadcastInput(e:Env,m:TGMessage):Promise<BroadcastInputAdmission|null> {
 if(!ruleInputEligible(e,m))return null;
 const s=await broadcastSession(e,String(m.from!.id));
 if(!s||s.stage==='done'||s.expires_at<now())return null;
 return {instance:e.INSTANCE_ID,actor:s.actor_id,session:s.session_id,revision:s.revision,stage:s.stage,expiresAt:s.expires_at,epoch:e.RECOVERY_EPOCH};
}
export async function closeBroadcastSession(e:Env,actor:string,completionKey?:string):Promise<void> {
 const s=await broadcastSession(e,actor),draft=s?JSON.parse(s.payload) as Draft:{};
 if(draft.campaign&&s?.stage!=='done')await cancelBroadcast(e,draft.campaign,actor);
 await run(e,"UPDATE broadcast_sessions SET stage='done',revision=revision+1,payload='{}',transition_key=COALESCE(?,transition_key) WHERE instance_id=? AND actor_id=? AND epoch=? AND stage!='done'",completionKey??null,e.INSTANCE_ID,actor,e.RECOVERY_EPOCH);
}
async function start(e:Env,j:Job,actor:string):Promise<BroadcastSession> {
 await closeRuleSession(e,actor);
 const key=`start:${j.id}`,s=await broadcastSession(e,actor);
 if(s?.transition_key===key)return s;
 await closeBroadcastSession(e,actor);
 await run(e,`INSERT INTO broadcast_sessions(instance_id,actor_id,epoch,session_id,stage,payload,transition_key,expires_at) VALUES(?,?,?,?,'audience','{}',?,?) ON CONFLICT(instance_id,actor_id) DO UPDATE SET epoch=excluded.epoch,session_id=excluded.session_id,stage='audience',revision=1,payload='{}',transition_key=excluded.transition_key,expires_at=excluded.expires_at WHERE broadcast_sessions.transition_key!=excluded.transition_key`,e.INSTANCE_ID,actor,e.RECOVERY_EPOCH,`${e.RECOVERY_EPOCH}:${j.id}`,key,now()+TTL);
 return (await broadcastSession(e,actor))!;
}
async function move(e:Env,s:BroadcastSession,key:string,stage:BroadcastStage,payload:Draft):Promise<BroadcastSession|null> {
 await run(e,`UPDATE broadcast_sessions SET stage=?,revision=revision+1,payload=?,transition_key=?,expires_at=? WHERE instance_id=? AND actor_id=? AND epoch=? AND session_id=? AND revision=? AND stage=? AND expires_at>=? AND transition_key!=?`,stage,JSON.stringify(payload),key,now()+TTL,e.INSTANCE_ID,s.actor_id,e.RECOVERY_EPOCH,s.session_id,s.revision,s.stage,now(),key);
 const current=await broadcastSession(e,s.actor_id);
 return current?.session_id===s.session_id&&current.transition_key===key?current:null;
}
async function buttons(e:Env,j:Job,actor:string,rows:ButtonSpec[][],s?:BroadcastSession):Promise<Button[][]> {
 const specs=rows.flat().map((spec,i)=>({...spec,context:`broadcast:${e.RECOVERY_EPOCH}:${j.id}:${s?.session_id??''}:${s?.revision??0}:${i}`}));
 if(!specs.length)return [];
 const queries=specs.map(spec=>stmt(e,`INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) SELECT ?,?,'',?,NULL,0,?,? WHERE NOT EXISTS(SELECT 1 FROM callback_actions WHERE kind=? AND actor_id=? AND json_extract(payload,'$.context')=?)`,crypto.randomUUID().replace(/-/g,''),spec.kind,actor,JSON.stringify({...spec.payload,instance:e.INSTANCE_ID,epoch:e.RECOVERY_EPOCH,context:spec.context,...(s?{session:s.session_id,revision:s.revision,workflowStage:s.stage}:{})}),s?.expires_at??now()+TTL,spec.kind,actor,spec.context));
 const result=await e.DB.batch([...queries,stmt(e,`SELECT token,json_extract(payload,'$.context') context FROM callback_actions WHERE actor_id=? AND json_extract(payload,'$.instance')=? AND json_extract(payload,'$.context') IN (${specs.map(()=>'?').join(',')}) ORDER BY rowid`,actor,e.INSTANCE_ID,...specs.map(x=>x.context))]);
 const tokens=new Map<string,string>();for(const row of result.at(-1)!.results as unknown as {token:string;context:string}[])if(!tokens.has(row.context))tokens.set(row.context,row.token);
 let index=0;return rows.map(row=>row.map(spec=>({text:spec.text,callback_data:`a:${tokens.get(specs[index++].context)!}`})));
}
async function panelSend(e:Env,key:string,text:string,controls:Button[][]) {
 const result=await notify(e,key,null,text,{reply_markup:{inline_keyboard:controls}});
 if(result.status==='success'&&Number.isSafeInteger(result.result.message_id))await e.DB.batch(controls.flat().map(b=>stmt(e,"UPDATE callback_actions SET payload=json_set(payload,'$.messageId',?) WHERE token=?",result.result.message_id,b.callback_data.slice(2))));
 return result;
}
const target=async(e:Env)=>{const username=await setting(e,'bot_username','');return `${e.INSTANCE_ID} · ${username?`@${username}`:`Bot ${e.EXPECTED_BOT_ID}`}`;};
const audienceOf=(d:Draft)=>normalizeBroadcastAudience(d.audience);
function savedAudience(b:BroadcastRow):BroadcastAudience|null {
 try {return normalizeBroadcastAudience(JSON.parse(b.scope_json));}catch{return null;}
}
function audienceDescription(a:BroadcastAudience,total?:number):string {
 if(a.mode==='all')return '接收范围：全体符合条件用户';
 return `接收范围：指定多个 TGID\n去重后指定：${a.ids.length} 个${total===undefined?'':`\n预览时符合接收：${total} 人\n预览时排除：${Math.max(0,a.ids.length-total)} 个（未登记、不符合条件或不在测试授权范围）`}`;
}
const invalidIds='请输入 1–200 个有效用户 ID，以逗号、中文逗号、空格或换行分隔；不能填写用户名、负数、0 或带前导零的 ID。';

async function broadcastDisplayState(e:Env,b:Pick<BroadcastRow,'id'|'state'|'epoch'>):Promise<{label:string;notice:string}> {
 const historical=b.epoch!==e.RECOVERY_EPOCH;
 const historyNotice=historical?'历史广播（恢复周期已变化），仅供核查；旧确认不能再次发送。':'';
 if((b.state==='sending'||b.state==='confirmed')&&historical)return {label:'已暂停 / 需核查',notice:`${historyNotice}未发送项目保持原记录，未知结果不自动重发。`};
 if(b.state==='sending') {
  const task=await one<{state:string}>(e,"SELECT state FROM jobs WHERE dedupe=? AND kind='broadcast' AND epoch=?",`broadcast:${b.id}`,b.epoch);
  if(task&&(task.state==='attention'||task.state==='quarantined'))return {label:'已暂停 / 需核查',notice:'发送任务已暂停，需要核查持久操作记录。未发送项目保持原记录，未知结果不自动重发。'};
 }
 const labels:Record<string,string>={draft:historical?'历史草稿（已失效）':'等待两次确认',confirmed:'准备发送',sending:'发送中',done:'已完成',canceled:'已取消',interrupted:'已中断 / 需核查'};
 return {label:labels[b.state]??b.state,notice:historyNotice};
}
export async function broadcastPanel(e:Env,j:Job,actor:string):Promise<void> {
 if(!allowed(e,actor))return;
 await closeRuleSession(e,actor);await closeBroadcastSession(e,actor);
 const recent=await all<Pick<BroadcastRow,'id'|'state'|'total'|'epoch'>>(e,'SELECT id,state,total,epoch FROM broadcasts WHERE instance_id=? AND bot_id=? AND actor_id=? ORDER BY created_at DESC,id DESC LIMIT 5',e.INSTANCE_ID,e.EXPECTED_BOT_ID,actor);
 const history=await Promise.all(recent.map(async b=>[{text:`查看 ${b.id.slice(0,8)} · ${(await broadcastDisplayState(e,b)).label} · ${b.total} 人`,kind:'broadcast_status',payload:{id:b.id}}]));
 const rows:ButtonSpec[][]=[[{text:'新建广播',kind:'broadcast_new'}],...history,[{text:'返回主面板',kind:'broadcast_main'}]];
 await panelSend(e,`job:${j.id}:broadcast:panel`,`管理员广播\n目标机器人：${await target(e)}\n\n可选择全体符合条件用户，或指定多个 TGID；发送文字、图片或文字加图片。指定 ID 不会新建或启用用户。预览时保存接收名单，发送前再次检查状态。${e.INSTANCE_ID==='test-a'||e.INSTANCE_ID==='test-b'?'测试账号限定范围，仅向配置的获授权测试账号发送。':''}必须先查看完整预览，再进行两次独立确认。\n结果未知不会自动重发。`,await buttons(e,j,actor,rows));
}
async function status(e:Env,j:Job,actor:string,id:string):Promise<void> {
 const b=await getBroadcast(e,id);
 if(!b||b.instance_id!==e.INSTANCE_ID||b.bot_id!==e.EXPECTED_BOT_ID||b.actor_id!==actor)return;
 const stats=await broadcastStats(e,id);if(!stats)return;
 const display=await broadcastDisplayState(e,b),audience=savedAudience(b);
 await panelSend(e,`job:${j.id}:broadcast:status:${id}`,`广播 ${id.slice(0,8)}\n目标机器人：${await target(e)}\n${audience?audienceDescription(audience,stats.total):'接收范围记录异常，需核查；不得重新发送。'}\n状态：${display.label}${display.notice?`\n${display.notice}`:''}\n预计接收人数：${stats.total}\n已确认成功：${stats.success}\n明确失败：${stats.failed}\n未发送：${stats.unsent}\n结果未知：${stats.unknown}\n其中仅部分内容已确认：${stats.partial}\n\n成功表示所有内容均获 Telegram API 确认，不表示已读。未发送含尚待处理和因状态变化跳过；部分成功不算完整成功。未知结果保留记录，不自动重发。`,await buttons(e,j,actor,[[{text:'刷新状态',kind:'broadcast_status',payload:{id}},{text:'返回广播',kind:'broadcast_home'}]]));
}
async function previewId(e:Env,s:BroadcastSession,key:string) {
 const digest=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(`${e.INSTANCE_ID}:${e.RECOVERY_EPOCH}:${s.session_id}:${s.revision}:${key}`));
 return Array.from(new Uint8Array(digest)).slice(0,16).map(b=>b.toString(16).padStart(2,'0')).join('');
}
async function toPreview(e:Env,s:BroadcastSession,key:string) {
 const d=JSON.parse(s.payload) as Draft;if(!d.text&&!d.photo)return null;
 if(d.campaign)await cancelBroadcast(e,d.campaign,s.actor_id);
 return move(e,s,key,'preview',{...d,audience:audienceOf(d),campaign:await previewId(e,s,key)});
}
async function render(e:Env,j:Job,s:BroadcastSession,error?:string):Promise<void> {
 const d=JSON.parse(s.payload) as Draft;
 const rows:ButtonSpec[][]=[];let text='';
 if(s.stage==='audience') {
  text='新建广播 · 选择接收范围\n请选择全体符合条件用户，或输入多个 TGID 进行定向广播。\n只会使用本实例已有且符合条件的用户，指定 ID 不会新建用户。';
  rows.push([{text:'全体符合条件用户',kind:'broadcast_audience',payload:{mode:'all'}}],[{text:'指定多个 TGID',kind:'broadcast_audience',payload:{mode:'directed'}}]);
 } else if(s.stage==='audience_ids') {
  text=`定向广播 · 输入接收名单\n请在 General 直接发送 1–200 个用户 ID，可用逗号、中文逗号、空格或换行分隔。重复 ID 会去重。\n只接受正整数 TGID，不接受用户名、0、负数或前导零。未登记、封禁、无有效绑定等不符合条件的用户会排除；不会新建用户。${e.INSTANCE_ID==='test-a'||e.INSTANCE_ID==='test-b'?'\n测试实例仍只允许获授权测试账号；指定名单不能绕过测试范围。':''}`;
  rows.push([{text:'返回选择范围',kind:'broadcast_audience_back'}]);
 } else if(s.stage==='content')text='新建广播 · 收集内容\n请在 General 发送一条文字（1–3500 字符），或上传一张图片（可附文字说明）。\n后续可继续添加图片或文字；仅你的输入进入本流程。';
 else if(s.stage==='photo') {
  text=`广播 · 添加图片\n${d.text?`已收集文字（${d.text.length} 字符）。请上传一张图片，或点击“仅文字，生成预览”。图片说明会追加到已收集文字。`:'请上传一张图片，可附文字说明。'}`;
  if(d.text)rows.push([{text:'仅文字，生成预览',kind:'broadcast_preview'}]);
  rows.push([{text:'重新选择内容',kind:'broadcast_edit',payload:{stage:'content'}}]);
 } else if(s.stage==='text') {
  text=`广播 · 添加文字\n已收集图片${d.text?`和 ${d.text.length} 字符说明`:''}。可直接发送文字替换当前说明（1–3500 字符），或点击生成预览。`;
  rows.push([{text:d.text?'生成图文预览':'仅图片，生成预览',kind:'broadcast_preview'}],[{text:'重新选择内容',kind:'broadcast_edit',payload:{stage:'content'}}]);
 } else if(s.stage==='preview') {
  if(!d.campaign)return;
  let campaign:Awaited<ReturnType<typeof prepareBroadcast>>;
  try {campaign=await prepareBroadcast(e,{id:d.campaign,actor:s.actor_id,text:d.text,photo:d.photo,audience:audienceOf(d)});}
  catch(error) {
   if(!(error instanceof Error)||error.message!=='broadcast_test_scope_missing')throw error;
   await panelSend(e,`job:${j.id}:broadcast:scope-missing`, '测试收件范围尚未配置或配置无效，当前不能预览或发送广播。请联系维护者设置获授权测试账号范围后重新创建广播。',await buttons(e,j,s.actor_id,[[{text:'取消广播',kind:'broadcast_cancel'},{text:'返回主面板',kind:'broadcast_main'}]],s));return;
  }
  let complete=true;
  const audience=audienceOf(d);
  if(audience.mode==='directed') {
   const scope=await notify(e,`broadcast-scope-preview:${d.campaign}`,null,`定向广播 · 完整接收范围预览\n目标机器人：${await target(e)}\n${audienceDescription(audience,campaign.total)}\n\n以下是去重后的全部指定 TGID；正式名单还须符合接收条件。\n${audience.ids.join('\n')}`);
   complete=scope.status==='success'&&Number.isSafeInteger(scope.result.message_id)&&scope.result.message_id>0;
  }
  for(const [index,part] of bodyParts({text:d.text,photo:d.photo}).entries()) {
   if(!complete)break;
   const outcome=await call(e,`broadcast-preview:${d.campaign}:${index}`,part.method,{chat_id:e.ADMIN_CHAT_ID,...part.params});
   if(outcome.status!=='success'||!Number.isSafeInteger(outcome.result.message_id)||outcome.result.message_id<=0) {complete=false;break;}
  }
  text=`广播预览 · 尚未发送给用户\n目标机器人：${await target(e)}\n预计接收人数：${campaign.total}\n${audienceDescription(audience,campaign.total)}\n范围规则：预览时本实例符合条件的用户；正式发送前再次检查状态。新加入用户不追加。${e.INSTANCE_ID==='test-a'||e.INSTANCE_ID==='test-b'?'\n测试账号限定范围：只包含配置的获授权测试账号。':''}\n${complete?'上方独立消息是实际将发送的完整内容。请检查文字和图片。\n第一次确认只确认内容与范围，不会发送。':'完整预览未获确认（发送失败或结果未知）。本次不能确认发送；可重新修改生成预览或取消。'}`;
  if(complete)rows.push([{text:'第一次确认：内容和范围正确',kind:'broadcast_confirm_first'}]);
  rows.push([{text:'修改文字',kind:'broadcast_edit',payload:{stage:d.photo?'text':'content'}},{text:'修改图片',kind:'broadcast_edit',payload:{stage:'photo'}}]);
  if(d.photo)rows.push([{text:'移除图片并重新预览',kind:'broadcast_remove_photo'}]);
 } else if(s.stage==='second_confirm') {
  if(!d.campaign)return;const campaign=await getBroadcast(e,d.campaign);if(!campaign)return;
  const audience=audienceOf(d);
  text=`即将发送广播 · 第二次确认\n目标机器人：${await target(e)}\n预计接收人数：${campaign.total}\n${audienceDescription(audience,campaign.total)}\n你已确认完整内容与范围。点击下方“第二次确认：开始广播”才会正式发送。\n发送时仍检查用户状态，失败和未知结果分别记录。重复点击不会重复发送。`;
  rows.push([{text:'第二次确认：开始广播',kind:'broadcast_confirm_second'}],[{text:'返回预览（确认失效）',kind:'broadcast_repreview'}],[{text:'修改文字',kind:'broadcast_edit',payload:{stage:d.photo?'text':'content'}},{text:'修改图片',kind:'broadcast_edit',payload:{stage:'photo'}}]);
 } else return;
 if(s.stage!=='audience'&&s.stage!=='audience_ids')rows.push([{text:'修改接收范围',kind:'broadcast_change_audience'}]);
 if(['content','photo','text'].includes(s.stage))text+=`\n\n${audienceDescription(audienceOf(d))}`;
 rows.push([{text:'取消广播',kind:'broadcast_cancel'},{text:'返回主面板',kind:'broadcast_main'}]);
 await panelSend(e,`job:${j.id}:broadcast:draft:${s.revision}${error?':invalid':''}`,`${error?`${error}\n\n`:''}${text}\n\n本步骤 15 分钟有效。可直接发送“取消”。`,await buttons(e,j,s.actor_id,rows,s));
}
export async function handleBroadcastInput(e:Env,j:Job,u:TGUpdate,_waitUntil?:(promise:Promise<unknown>)=>void):Promise<boolean> {
 const m=u.message??u.edited_message;if(!m||!ruleInputEligible(e,m))return false;
 const actor=String(m.from!.id),text=m.text??'',p=JSON.parse(j.payload) as {broadcastInputAdmission?:BroadcastInputAdmission|null};
 const exit=/^\/(mb|tj|sc|cancel)(?:@([A-Za-z0-9_]+))?(?:\s|$)/i.exec(text);
 if(exit&&!u.edited_message&&(!exit[2]||exit[2].toLowerCase()===(await setting(e,'bot_username','')).toLowerCase())) {
  const s=await broadcastSession(e,actor);if(s?.transition_key!==`start:${j.id}`)await closeBroadcastSession(e,actor);
  if(exit[1].toLowerCase()==='cancel'&&s&&s.stage!=='done') {await broadcastPanel(e,j,actor);return true;}return false;
 }
 const a=Object.hasOwn(p,'broadcastInputAdmission')?p.broadcastInputAdmission:await captureBroadcastInput(e,m);
 if(!a)return false;
 if(a.instance!==e.INSTANCE_ID||a.actor!==actor||a.epoch!==e.RECOVERY_EPOCH)return true;
 const s=await broadcastSession(e,actor),key=`input:${j.id}`;
 if(s?.session_id===a.session&&s.transition_key===key) {if(s.stage==='done')await broadcastPanel(e,j,actor);else await render(e,j,s);return true;}
 if(!s||s.session_id!==a.session||s.stage==='done'||s.expires_at<now()||a.expiresAt<now()) {await notify(e,`job:${j.id}:broadcast:expired`,null,'这次广播已结束或过期；旧输入未保存。请从主面板重新进入广播。');return true;}
 if(s.revision!==a.revision||s.stage!==a.stage) {await notify(e,`job:${j.id}:broadcast:stale`,null,'广播已进入下一步，该旧输入未保存。请使用最新提示。');return true;}
 if(u.edited_message) {await render(e,j,s,'请发送一条新消息；编辑旧消息不能修改广播。');return true;}
 if(text.trim()==='取消'||text.trim()==='返回') {await closeBroadcastSession(e,actor,key);await broadcastPanel(e,j,actor);return true;}
 const d=JSON.parse(s.payload) as Draft;
 if(s.stage==='audience_ids') {
  if(!m.text||m.photo||m.document) {await render(e,j,s,invalidIds);return true;}
  let ids:string[];try {ids=parseBroadcastIds(text);}catch(error) {if(!(error instanceof Error)||error.message!=='invalid_broadcast_audience')throw error;await render(e,j,s,invalidIds);return true;}
  const draft={...d,audience:normalizeBroadcastAudience({mode:'directed',ids})};
  const next=await move(e,s,key,d.text||d.photo?'preview':'content',d.text||d.photo?{...draft,campaign:await previewId(e,s,key)}:draft);if(next)await render(e,j,next);return true;
 }
 if(s.stage==='audience') {await render(e,j,s,'请先点击按钮选择全体或指定多个 TGID。');return true;}
 if(m.media_group_id) {await render(e,j,s,'每次广播只支持一张图片，请单独上传一张图片。');return true;}
 const photo=m.photo?.at(-1)?.file_id;
 let next:BroadcastSession|null=null;
 if(s.stage==='content'&&photo) {
  if((m.caption?.length??0)>3500) {await render(e,j,s,'文字最多 3500 字符，请缩短后重新发送。');return true;}
  next=await move(e,s,key,'text',{...d,audience:audienceOf(d),photo,text:m.caption||undefined});
 } else if(s.stage==='content'&&text.trim()&&text.length<=3500)next=await move(e,s,key,'photo',{...d,audience:audienceOf(d),text});
 else if(s.stage==='photo'&&photo) {
  const combined=[d.text,m.caption].filter(Boolean).join('\n');
  if(combined.length>3500) {await render(e,j,s,'合并后的文字超过 3500 字符，请修改文字或上传无说明的图片。');return true;}
  next=await move(e,s,key,'preview',{...d,audience:audienceOf(d),text:combined||undefined,photo,campaign:await previewId(e,s,key)});
 } else if(s.stage==='text'&&text.trim()&&text.length<=3500) {
  next=await move(e,s,key,'preview',{...d,audience:audienceOf(d),photo:d.photo,text,campaign:await previewId(e,s,key)});
 } else {await render(e,j,s,['preview','second_confirm'].includes(s.stage)?'请点击按钮确认、修改或取消；此时新输入不会改变广播。':text.length>3500?'文字最多 3500 字符，请缩短后重新发送。':'请按提示发送文字或单张图片，其他附件不会加入广播。');return true;}
 if(next)await render(e,j,next);return true;
}
export async function handleBroadcastAction(e:Env,j:Job,action:BroadcastAction,p:Params,stamp:number,actor:string,_waitUntil?:(promise:Promise<unknown>)=>void,showMain?:()=>Promise<void>,onCommitted?:(text:string)=>Promise<void>):Promise<string> {
 if(!allowed(e,actor)||action.actor_id!==actor||action.thread_id!==null)return '无权操作。';
 if(p.instance!==e.INSTANCE_ID||p.epoch!==e.RECOVERY_EPOCH||action.expires_at<now()||(action.used!==0&&action.used!==stamp))return '按钮已失效。';
 const key=`action:${action.token}`,workflow=typeof p.session==='string';let s=workflow?await broadcastSession(e,actor):null;
 if(workflow&&(!s||s.session_id!==p.session||s.expires_at<now()||(s.revision!==p.revision&&s.transition_key!==key)))return '流程已变化或结束，请使用最新提示。';
 if(workflow&&s?.stage==='done'&&!(['broadcast_confirm_second','broadcast_cancel','broadcast_main'].includes(action.kind)&&s.transition_key===key))return '广播流程已结束。';
 await run(e,'UPDATE callback_actions SET used=? WHERE token=? AND used=0',stamp,action.token);
 if(action.kind==='broadcast_new') {const next=await start(e,j,actor);await onCommitted?.('广播流程已创建，请选择接收范围。');await render(e,j,next);return '请在 General 按提示输入。';}
 if(action.kind==='broadcast_home') {await broadcastPanel(e,j,actor);return '已打开广播面板。';}
 if(action.kind==='broadcast_status'&&typeof p.id==='string') {await status(e,j,actor,p.id);return '已读取实际发送状态。';}
 if(action.kind==='broadcast_main'||action.kind==='broadcast_cancel') {await closeBroadcastSession(e,actor,key);await onCommitted?.('已取消未发送的广播草稿。');if(action.kind==='broadcast_main'&&showMain)await showMain();else await broadcastPanel(e,j,actor);return '已取消未发送的广播草稿。';}
 if(!s)return '按钮已失效。';
 if(s.transition_key===key) {
  await onCommitted?.('此步骤已处理，请查看 General 中的提示。');
  const d=JSON.parse(s.payload) as Draft;if(s.stage==='done'&&d.campaign){await launchBroadcast(e,d.campaign);await status(e,j,actor,d.campaign);}else await render(e,j,s);return '此步骤已处理，请查看最新提示。';
 }
 if(action.kind==='broadcast_audience'&&s.stage==='audience') {
  const d=JSON.parse(s.payload) as Draft;let next:BroadcastSession|null;
  if(p.mode==='directed')next=await move(e,s,key,'audience_ids',d);
  else if(p.mode==='all') {const draft={...d,audience:normalizeBroadcastAudience({mode:'all'})};next=await move(e,s,key,d.text||d.photo?'preview':'content',d.text||d.photo?{...draft,campaign:await previewId(e,s,key)}:draft);}
  else return '按钮已失效。';
  if(next){await onCommitted?.('接收范围步骤已保存，请查看下一步。');await render(e,j,next);}return next?'请查看 General 中的下一步提示。':'流程已变化。';
 }
 if(action.kind==='broadcast_audience_back'&&s.stage==='audience_ids') {const next=await move(e,s,key,'audience',JSON.parse(s.payload));if(next){await onCommitted?.('已返回接收范围选择。');await render(e,j,next);}return '请重新选择接收范围。';}
 if(action.kind==='broadcast_change_audience'&&['content','photo','text','preview','second_confirm'].includes(s.stage)) {
  const d=JSON.parse(s.payload) as Draft;if(d.campaign)await cancelBroadcast(e,d.campaign,actor);delete d.campaign;
  const next=await move(e,s,key,'audience',d);if(next){await onCommitted?.('旧范围和确认已失效，请重新选择接收范围。');await render(e,j,next,'正文和图片已保留；修改范围后必须重新预览并确认两次。');}return '请重新选择接收范围，旧确认已失效。';
 }
 if(action.kind==='broadcast_preview'&&['photo','text'].includes(s.stage)) {const next=await toPreview(e,s,key);if(next){await onCommitted?.('正在生成完整预览，尚未发送给用户。');await render(e,j,next);}return '请查看完整广播预览。';}
 if(action.kind==='broadcast_confirm_first'&&s.stage==='preview') {
  const next=await move(e,s,key,'second_confirm',JSON.parse(s.payload));if(next){await onCommitted?.('第一次确认已记录，尚未开始发送。');await render(e,j,next);}return '第一次确认已记录，尚未开始发送。';
 }
 if(action.kind==='broadcast_confirm_second'&&s.stage==='second_confirm') {
  const d=JSON.parse(s.payload) as Draft;if(!d.campaign)return '预览已失效。';
  // Commit the second distinct step and its launch permission together. A
  // retry may launch only the campaign authorized by this exact transition.
  await e.DB.batch([
   stmt(e,"UPDATE broadcast_sessions SET stage='done',revision=revision+1,transition_key=?,expires_at=? WHERE instance_id=? AND actor_id=? AND epoch=? AND session_id=? AND revision=? AND stage='second_confirm' AND expires_at>=?",key,now()+TTL,e.INSTANCE_ID,actor,e.RECOVERY_EPOCH,s.session_id,s.revision,now()),
   stmt(e,"UPDATE broadcasts SET state='confirmed' WHERE id=? AND instance_id=? AND actor_id=? AND epoch=? AND state='draft' AND EXISTS(SELECT 1 FROM broadcast_sessions WHERE instance_id=? AND actor_id=? AND epoch=? AND session_id=? AND stage='done' AND transition_key=?)",d.campaign,e.INSTANCE_ID,actor,e.RECOVERY_EPOCH,e.INSTANCE_ID,actor,e.RECOVERY_EPOCH,s.session_id,key)
  ]);
  s=await broadcastSession(e,actor);
  if(s?.transition_key!==key||s.stage!=='done')return '流程已变化，本次未发送。';
  await onCommitted?.('两次确认已记录，正在安排广播；结果以状态为准。');
  await launchBroadcast(e,d.campaign);await status(e,j,actor,d.campaign);return '两次确认完成，广播已安排；成功数以实际结果为准。';
 }
 if(action.kind==='broadcast_repreview'&&s.stage==='second_confirm') {const next=await toPreview(e,s,key);if(next){await onCommitted?.('旧确认已失效，正在重新生成预览。');await render(e,j,next,'之前的确认已失效，请重新检查完整预览。');}return '请重新预览并确认两次。';}
 if((action.kind==='broadcast_edit'||action.kind==='broadcast_remove_photo')&&['preview','second_confirm','text','photo'].includes(s.stage)) {
  const d=JSON.parse(s.payload) as Draft;if(d.campaign)await cancelBroadcast(e,d.campaign,actor);delete d.campaign;
  let stage:BroadcastStage;
  if(action.kind==='broadcast_remove_photo') {
   delete d.photo;
   const next=await move(e,s,key,d.text?'preview':'content',d.text?{...d,audience:audienceOf(d),text:d.text,campaign:await previewId(e,s,key)}:{audience:audienceOf(d)});if(next){await onCommitted?.('图片已移除，旧确认已失效。');await render(e,j,next,'之前的确认已失效，请重新检查预览。');}return '图片已移除，之前的确认已失效。';
  }
  else if(p.stage==='content'||p.stage==='text'||p.stage==='photo')stage=p.stage;else return '按钮已失效。';
  if(stage==='content'){delete d.text;delete d.photo;}
  if(stage==='photo'&&action.kind==='broadcast_edit')delete d.photo;
  const next=await move(e,s,key,stage,d);if(next){await onCommitted?.('旧确认已失效，请按提示修改内容。');await render(e,j,next,'之前的确认已失效；修改后需要重新预览并确认两次。');}return '已返回修改，之前的确认已失效。';
 }
 return '按钮已失效。';
}
