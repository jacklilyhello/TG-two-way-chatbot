import {env} from 'cloudflare:test';
import {describe,expect,it} from 'vitest';
import type {Env,Job,Params,TGMessage,TGUpdate} from '../src/types';
import {all,now,one,run} from '../src/db';
import {RetryLater} from '../src/operations';
import {broadcastPanel,broadcastSession,captureBroadcastInput,closeBroadcastSession,handleBroadcastAction,handleBroadcastInput,type BroadcastAction,type BroadcastInputAdmission} from '../src/broadcast-workflow';
import {handleAdmin} from '../src/admin';
import {captureRuleInput,startRuleSession,ruleSession} from '../src/rule-workflow';
import {accept} from '../src/ingress';
import {recoverBroadcasts} from '../src/broadcast-delivery';
const base=env as unknown as Env;
interface Request {key:string;method:string;params:Params}
function client(onRequest?:(r:Request)=>Response|undefined) {
 const requests:Request[]=[];let sequence=1000;
 const e={...base,ADMIN_IDS:'10001,10002',JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:()=>'',get:()=>({fetch:async()=>new Response('ok')})},BOT:{idFromName:()=>'',get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const r=JSON.parse(String(init.body)) as Request;requests.push(r);return onRequest?.(r)??Response.json({status:'success',result:{message_id:++sequence}});
 }})}} as unknown as Env;
 return {e,requests};
}
const job=(id:number,a?:BroadcastInputAdmission|null):Job=>({id,dedupe:`bc:${id}`,user_key:'admin:10001',kind:'update',payload:JSON.stringify(a===undefined?{}:{broadcastInputAdmission:a}),state:'running',due_at:0,attempts:0,epoch:base.RECOVERY_EPOCH,created_at:now()});
const message=(e:Env,id:number,text?:string,photo?:string,actor=10001):TGMessage=>({message_id:id,date:Math.floor(now()/1000),chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup',is_forum:true},from:{id:actor},...(text?{text}:{}),...(photo?{photo:[{file_id:photo}]}:{})});
async function latest(e:Env,kind:string,actor=10001):Promise<BroadcastAction> {return (await one(e,'SELECT * FROM callback_actions WHERE kind=? AND actor_id=? ORDER BY rowid DESC LIMIT 1',kind,String(actor)))!;}
async function click(e:Env,id:number,kind:string,actor=10001) {const a=await latest(e,kind,actor);expect(a).not.toBeNull();return handleBroadcastAction(e,job(id),a,JSON.parse(a.payload),id+1,String(actor));}
async function input(e:Env,id:number,text?:string,photo?:string,actor=10001,extra:Partial<TGMessage>={}) {const m={...message(e,id,text,photo,actor),...extra};return handleBroadcastInput(e,job(id,await captureBroadcastInput(e,m)),{update_id:id,message:m});}
async function beginAudience(e:Env,id=1,actor=10001) {await broadcastPanel(e,job(id),String(actor));await click(e,id+1,'broadcast_new',actor);}
async function selectAudience(e:Env,id:number,mode:'all'|'directed',actor=10001) {
 const actions=await all<BroadcastAction>(e,"SELECT * FROM callback_actions WHERE kind='broadcast_audience' AND actor_id=? ORDER BY rowid DESC",String(actor)),a=actions.find(a=>JSON.parse(a.payload).mode===mode)!;
 return handleBroadcastAction(e,job(id),a,JSON.parse(a.payload),id+1,String(actor));
}
async function begin(e:Env,id=1,actor=10001) {await beginAudience(e,id,actor);await selectAudience(e,id+10000,'all',actor);}
async function beginDirected(e:Env,id:number,ids:string,actor=10001) {await beginAudience(e,id,actor);await selectAudience(e,id+10000,'directed',actor);await input(e,id+10001,ids,undefined,actor);}

async function textPreview(e:Env,id=1,text='broadcast body',actor=10001) {await begin(e,id,actor);await input(e,id+2,text,undefined,actor);await click(e,id+3,'broadcast_preview',actor);}
async function recipient(e:Env,id='20001',blocked=0) {await run(e,'INSERT INTO users(tgid,first_contact,thread_id,blocked) VALUES(?,?,?,?)',id,1,Number(id)+1,blocked);}
async function campaign(e:Env,actor='10001') {return JSON.parse((await broadcastSession(e,actor))!.payload).campaign as string;}

describe('administrator broadcast workflow',()=>{
 it('collects text, shows exact standalone preview and requires two distinct confirmations',async()=>{
  const {e,requests}=client();await recipient(e);await textPreview(e,1,'same  spacing\nbody');
  const id=await campaign(e);expect(await one(e,'SELECT state,total FROM broadcasts WHERE id=?',id)).toEqual({state:'draft',total:1});
  const actual=requests.find(r=>r.key.includes('broadcast-preview:'))!;expect(actual.params).toEqual({chat_id:e.ADMIN_CHAT_ID,text:'same  spacing\nbody'});
  expect(actual.params.reply_markup).toBeUndefined();expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
  const first=await latest(e,'broadcast_confirm_first');await click(e,5,'broadcast_confirm_first');
  expect((await broadcastSession(e,'10001'))!.stage).toBe('second_confirm');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
  await handleBroadcastAction(e,job(6),first,JSON.parse(first.payload),7,'10001');
  expect((await broadcastSession(e,'10001'))!.stage).toBe('second_confirm');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
  const second=await latest(e,'broadcast_confirm_second');await click(e,7,'broadcast_confirm_second');
  expect(await one(e,'SELECT state FROM broadcasts WHERE id=?',id)).toEqual({state:'sending'});expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(1);
  await handleBroadcastAction(e,job(8),second,JSON.parse(second.payload),9,'10001');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(1);
  expect(requests.filter(r=>r.params.chat_id==='20001')).toHaveLength(0);
 });
 it('collects a picture without text and previews only the picture',async()=>{
  const {e,requests}=client();await begin(e,20);await input(e,22,undefined,'photo-only');expect((await broadcastSession(e,'10001'))!.stage).toBe('text');await click(e,23,'broadcast_preview');
  const preview=requests.filter(r=>r.key.includes('broadcast-preview:'));expect(preview).toHaveLength(1);expect(preview[0].method).toBe('sendPhoto');expect(preview[0].params).toEqual({chat_id:e.ADMIN_CHAT_ID,photo:'photo-only'});
 });
 it('preserves both picture and caption, and both long text parts in preview',async()=>{
  const {e,requests}=client();await begin(e,30);await input(e,32,undefined,'with-caption',10001,{caption:'picture words'});await click(e,33,'broadcast_preview');
  expect(requests.find(r=>r.key.includes('broadcast-preview:'))!.params).toEqual({chat_id:e.ADMIN_CHAT_ID,photo:'with-caption',caption:'picture words'});
  await begin(e,40);const long='long text '.repeat(200);await input(e,42,long);await input(e,43,undefined,'long-picture');
  const id=await campaign(e),parts=requests.filter(r=>r.key.includes(`broadcast-preview:${id}:`));expect(parts).toHaveLength(2);expect(parts[0].params).toEqual({chat_id:e.ADMIN_CHAT_ID,photo:'long-picture'});expect(parts[1].params).toEqual({chat_id:e.ADMIN_CHAT_ID,text:long});
  expect(JSON.stringify(parts)).not.toContain('callback_data');
 });
 it('appends image caption to separately supplied text and prevents oversized loss',async()=>{
  const {e}=client();await begin(e,50);await input(e,52,'first text');await input(e,53,undefined,'photo',10001,{caption:'caption'});
  expect(await one(e,'SELECT text,photo FROM broadcasts')).toEqual({text:'first text\ncaption',photo:'photo'});
  await begin(e,60);await input(e,62,'x'.repeat(3500));await input(e,63,undefined,'photo',10001,{caption:'extra'});expect((await broadcastSession(e,'10001'))!.stage).toBe('photo');
 });
 it('cancels before or after first confirmation without launching any recipient task',async()=>{
  const {e}=client();await textPreview(e,70);await click(e,74,'broadcast_cancel');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
  await textPreview(e,80);await click(e,84,'broadcast_confirm_first');await input(e,85,'取消');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);expect(await all(e,"SELECT id FROM broadcasts WHERE state!='canceled'")).toHaveLength(0);
 });
 it('invalidates old confirmation on editing and creates an immutable new preview',async()=>{
  const {e}=client();await textPreview(e,90,'old');const oldId=await campaign(e);await click(e,94,'broadcast_confirm_first');const old=await latest(e,'broadcast_confirm_second');
  const editActions=await all<BroadcastAction>(e,"SELECT * FROM callback_actions WHERE kind='broadcast_edit' ORDER BY rowid DESC"),edit=editActions.find(a=>JSON.parse(a.payload).stage==='content')!;
  await handleBroadcastAction(e,job(95),edit,JSON.parse(edit.payload),96,'10001');await input(e,96,'new');await click(e,97,'broadcast_preview');
  expect(await campaign(e)).not.toBe(oldId);expect(await one(e,'SELECT state,text FROM broadcasts WHERE id=?',oldId)).toEqual({state:'canceled',text:'old'});
  expect(await handleBroadcastAction(e,job(98),old,JSON.parse(old.payload),99,'10001')).toContain('流程已变化');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
 });
 it('isolates actors, instances and recovery epochs and rejects unauthorized input',async()=>{
  const {e}=client();await textPreview(e,100,'first',10001);await textPreview(e,110,'second',10002);
  const first=await latest(e,'broadcast_confirm_first',10001);expect(await handleBroadcastAction(e,job(114),first,JSON.parse(first.payload),115,'10002')).toBe('无权操作。');
  const m=message(e,115,'ignored');for(const modified of [{...m,from:{id:20001}},{...m,message_thread_id:40,is_topic_message:true},{...m,sender_chat:{id:Number(e.ADMIN_CHAT_ID)}},{...m,from:{id:10001,is_bot:true}},{...m,forward_origin:{type:'hidden_user' as const,sender_user_name:'x',date:1}}])expect(await captureBroadcastInput(e,modified)).toBeNull();
  expect(await captureBroadcastInput({...e,INSTANCE_ID:'other'},m)).toBeNull();expect(await captureBroadcastInput({...e,RECOVERY_EPOCH:'other'},m)).toBeNull();
  expect((await broadcastSession(e,'10002'))!.stage).toBe('preview');
 });
 it('intercepts stale admitted messages and edits without replacing the content',async()=>{
  const {e,requests}=client();await begin(e,120);const m=message(e,122,'first'),admission=await captureBroadcastInput(e,m);
  await handleBroadcastInput(e,job(122,admission),{update_id:122,message:m});await handleBroadcastInput(e,job(123,admission),{update_id:123,message:message(e,123,'late')});expect(String(requests.at(-1)!.params.text)).toContain('旧输入未保存');
  const edit=message(e,124,'edited');await handleBroadcastInput(e,job(124,await captureBroadcastInput(e,edit)),{update_id:124,edited_message:edit});expect(JSON.parse((await broadcastSession(e,'10001'))!.payload).text).toBe('first');
  await closeBroadcastSession(e,'10001');await handleBroadcastInput(e,job(125,admission),{update_id:125,message:m});expect(String(requests.at(-1)!.params.text)).toContain('已结束或过期');
 });
 it('expires drafts and never authorizes a late confirmation',async()=>{
  const {e}=client();await textPreview(e,130);const first=await latest(e,'broadcast_confirm_first');await run(e,'UPDATE broadcast_sessions SET expires_at=?',now()-1);
  expect(await handleBroadcastAction(e,job(134),first,JSON.parse(first.payload),135,'10001')).toContain('流程已变化');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
 });
 it('withholds first confirmation if any part of complete preview is unknown',async()=>{
  const {e,requests}=client(r=>r.key.includes('broadcast-preview:')?Response.json({status:'unknown',reason:'response lost'}):undefined);await textPreview(e,140);
  expect(await latest(e,'broadcast_confirm_first')).toBeNull();expect(String(requests.at(-1)!.params.text)).toContain('完整预览未获确认');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
 });
 it('recovers a rate limited preview without changing the draft revision or duplicating content',async()=>{
  let limited=true;const {e}=client(r=>{if(r.key.includes('broadcast-preview:')&&limited){limited=false;return Response.json({due:now()+10},{status:429});}});await begin(e,150);await input(e,152,'recover');
  const preview=await latest(e,'broadcast_preview'),payload=JSON.parse(preview.payload);await expect(handleBroadcastAction(e,job(153),preview,payload,154,'10001')).rejects.toBeInstanceOf(RetryLater);
  const before=await broadcastSession(e,'10001');await handleBroadcastAction(e,job(153),preview,payload,154,'10001');expect((await broadcastSession(e,'10001'))!.revision).toBe(before!.revision);expect(await all(e,'SELECT id FROM broadcasts')).toHaveLength(1);
 });
 it('exposes the broadcast entry in the main panel and keeps rule workflows mutually exclusive',async()=>{
  const {e,requests}=client();await handleAdmin(e,job(160),{update_id:160,message:message(e,160,'/mb')});expect(JSON.stringify(requests.at(-1)!.params.reply_markup)).toContain('管理员广播');
  await startRuleSession(e,job(161),'10001','pattern');await begin(e,162);expect((await ruleSession(e,'10001'))!.stage).toBe('done');expect(await captureRuleInput(e,message(e,164,'text'))).toBeNull();
  const main=message(e,165,'/mb');await handleAdmin(e,job(165,await captureBroadcastInput(e,main)),{update_id:165,message:main});expect((await broadcastSession(e,'10001'))!.stage).toBe('done');
 });
 it('captures immutable broadcast admission before queueing General input',async()=>{
  const {e}=client();await begin(e,170);const m=message(e,172,'admitted');await accept(e,{update_id:172,message:m});
  const stored=await one<{payload:string}>(e,"SELECT payload FROM jobs WHERE dedupe='u:172'");expect(JSON.parse(stored!.payload).broadcastInputAdmission.session).toBe((await broadcastSession(e,'10001'))!.session_id);
 });
 it('does not accept forged callback messages or unauthorized private account operations',async()=>{
  const {e}=client();await textPreview(e,180);const a=await latest(e,'broadcast_confirm_first'),p=JSON.parse(a.payload);
  const cb={id:'forged',from:{id:10001},data:`a:${a.token}`,message:{...message(e,184),message_id:p.messageId+1,from:{id:Number(e.EXPECTED_BOT_ID)}}};await handleAdmin(e,job(184),{update_id:184,callback_query:cb});expect((await broadcastSession(e,'10001'))!.stage).toBe('preview');
  cb.message.message_id=p.messageId;cb.from.id=20001;await handleAdmin(e,job(185),{update_id:185,callback_query:cb});expect((await broadcastSession(e,'10001'))!.stage).toBe('preview');
 });
 it('recovers second-confirmation feedback after the atomic launch without another broadcast job',async()=>{
  let limited=true;const {e}=client(r=>{if(r.key.includes('job:194:broadcast:status:')&&limited){limited=false;return Response.json({due:now()+10},{status:429});}});
  await textPreview(e,190);await click(e,193,'broadcast_confirm_first');const a=await latest(e,'broadcast_confirm_second'),p=JSON.parse(a.payload);
  await expect(handleBroadcastAction(e,job(194),a,p,195,'10001')).rejects.toBeInstanceOf(RetryLater);
  expect((await broadcastSession(e,'10001'))!.stage).toBe('done');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(1);
  await handleBroadcastAction(e,job(194),a,p,195,'10001');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(1);
 });
 it('recovers cancellation feedback after the cancellation is already committed',async()=>{
  let limited=true;const {e}=client(r=>{if(r.key.endsWith('job:204:broadcast:panel')&&limited){limited=false;return Response.json({due:now()+10},{status:429});}});
  await textPreview(e,200);const a=await latest(e,'broadcast_cancel'),p=JSON.parse(a.payload);
  await expect(handleBroadcastAction(e,job(204),a,p,205,'10001')).rejects.toBeInstanceOf(RetryLater);
  expect((await broadcastSession(e,'10001'))!.stage).toBe('done');expect(await handleBroadcastAction(e,job(204),a,p,205,'10001')).toContain('已取消');
  expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
 });
 it('recovers a combined-content input preview after rate limiting the picture send',async()=>{
  let limited=true;const {e}=client(r=>{if(r.key.includes('broadcast-preview:')&&limited){limited=false;return Response.json({due:now()+10},{status:429});}});
  await begin(e,210);await input(e,212,'original text');const m=message(e,213,undefined,'picture'),a=await captureBroadcastInput(e,m),j=job(213,a),u={update_id:213,message:m};
  await expect(handleBroadcastInput(e,j,u)).rejects.toBeInstanceOf(RetryLater);const before=await broadcastSession(e,'10001');expect(before!.stage).toBe('preview');
  await handleBroadcastInput(e,j,u);expect((await broadcastSession(e,'10001'))!.revision).toBe(before!.revision);expect(await latest(e,'broadcast_confirm_first')).not.toBeNull();
 });
 it('rejects old flow buttons after a new flow starts and intercepts the old admitted input',async()=>{
  const {e}=client();await textPreview(e,220);const old=await latest(e,'broadcast_confirm_first'),m=message(e,224,'late'),a=await captureBroadcastInput(e,m);await begin(e,225);
  expect(await handleBroadcastAction(e,job(227),old,JSON.parse(old.payload),228,'10001')).toContain('流程已变化');await handleBroadcastInput(e,job(228,a),{update_id:228,message:m});
  expect((await broadcastSession(e,'10001'))!.stage).toBe('content');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
 });

 it('explains missing test scope and never offers confirmation or launches a broadcast',async()=>{
  const {e,requests}=client();const test={...e,INSTANCE_ID:'test-a',BROADCAST_TEST_RECIPIENT_IDS:undefined};await textPreview(test,230);
  expect(String(requests.at(-1)!.params.text)).toContain('测试收件范围尚未配置');expect(await latest(test,'broadcast_confirm_first')).toBeNull();expect(await all(test,'SELECT id FROM broadcasts')).toHaveLength(0);
 });
 it('returns to a fresh preview after first confirmation and invalidates its old second button',async()=>{
  const {e}=client();await textPreview(e,240);const original=await campaign(e);await click(e,244,'broadcast_confirm_first');const second=await latest(e,'broadcast_confirm_second');await click(e,245,'broadcast_repreview');
  expect((await broadcastSession(e,'10001'))!.stage).toBe('preview');expect(await campaign(e)).not.toBe(original);expect(await handleBroadcastAction(e,job(246),second,JSON.parse(second.payload),247,'10001')).toContain('流程已变化');
 });

 it('keeps historical interrupted results visible without granting old confirmations new authority',async()=>{
  const {e,requests}=client();await recipient(e);await textPreview(e,250);await click(e,254,'broadcast_confirm_first');const oldSecond=await latest(e,'broadcast_confirm_second');await click(e,255,'broadcast_confirm_second');const id=await campaign(e);
  const recovered={...e,RECOVERY_EPOCH:'new-broadcast-epoch'};await recoverBroadcasts(recovered);await broadcastPanel(recovered,job(256),'10001');
  expect(JSON.stringify(requests.at(-1)!.params.reply_markup)).toContain(id.slice(0,8));const currentStatus=await latest(recovered,'broadcast_status');expect(JSON.parse(currentStatus.payload).epoch).toBe(recovered.RECOVERY_EPOCH);
  await click(recovered,257,'broadcast_status');const text=String(requests.at(-1)!.params.text);expect(text).toContain('已中断 / 需核查');expect(text).toContain('历史广播（恢复周期已变化）');expect(text).toContain('预计接收人数：1');expect(text).toContain('结果未知：1');expect(text).toContain('已确认成功：0');
  expect(await handleBroadcastAction(recovered,job(258),oldSecond,JSON.parse(oldSecond.payload),259,'10001')).toBe('按钮已失效。');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(1);
 });
 it.each(['attention','quarantined'])('shows a %s broadcast task as paused for investigation with actual unsent counts',async taskState=>{
  const {e,requests}=client();await recipient(e);await textPreview(e,260);await click(e,264,'broadcast_confirm_first');await click(e,265,'broadcast_confirm_second');const id=await campaign(e);
  await run(e,"UPDATE jobs SET state=? WHERE dedupe=?",taskState,`broadcast:${id}`);await broadcastPanel(e,job(266),'10001');expect(JSON.stringify(requests.at(-1)!.params.reply_markup)).toContain('已暂停 / 需核查');await click(e,267,'broadcast_status');
  const text=String(requests.at(-1)!.params.text);expect(text).toContain('状态：已暂停 / 需核查');expect(text).not.toContain('状态：发送中');expect(text).toContain('未发送：1');expect(text).toContain('已确认成功：0');expect(text).toContain('未知结果不自动重发');
  expect(await one(e,'SELECT state FROM broadcasts WHERE id=?',id)).toEqual({state:'sending'});expect(await one(e,'SELECT state FROM jobs WHERE dedupe=?',`broadcast:${id}`)).toEqual({state:taskState});
 });
 it('limits historical listings and read-only status to the same instance, bot and actor',async()=>{
  const {e,requests}=client();await textPreview(e,270);const own=await campaign(e);await run(e,"UPDATE broadcasts SET epoch='old',state='interrupted' WHERE id=?",own);
  const foreign=[['foreign-instance','different-instance',e.EXPECTED_BOT_ID,'10001'],['foreign-bot',e.INSTANCE_ID,'99999','10001'],['foreign-actor',e.INSTANCE_ID,e.EXPECTED_BOT_ID,'10002']];
  for(const [id,instance,bot,actor] of foreign)await run(e,"INSERT INTO broadcasts(id,instance_id,bot_id,actor_id,epoch,text,state,created_at) VALUES(?,?,?,?,'old','private history','interrupted',?)",id,instance,bot,actor,now()+1);
  await broadcastPanel(e,job(274),'10001');const keyboard=JSON.stringify(requests.at(-1)!.params.reply_markup);expect(keyboard).toContain(own.slice(0,8));for(const [id] of foreign)expect(keyboard).not.toContain(id.slice(0,8));
  const action=await latest(e,'broadcast_status'),before=requests.length;
  for(const [id] of foreign)await handleBroadcastAction(e,job(275),action,{...JSON.parse(action.payload),id},276,'10001');expect(requests).toHaveLength(before);expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
 });

 it('issues fresh current-epoch history controls even when restored job ids repeat',async()=>{
  const {e}=client();await broadcastPanel(e,job(280),'10001');const old=await latest(e,'broadcast_new');
  const recovered={...e,RECOVERY_EPOCH:'restored-control-epoch'};await broadcastPanel(recovered,job(280),'10001');const fresh=await latest(recovered,'broadcast_new');
  expect(fresh.token).not.toBe(old.token);expect(JSON.parse(fresh.payload).epoch).toBe(recovered.RECOVERY_EPOCH);expect(await handleBroadcastAction(recovered,job(281),old,JSON.parse(old.payload),282,'10001')).toBe('按钮已失效。');
 });

 it('requires an explicit audience choice before collecting content for a new broadcast',async()=>{
  const {e,requests}=client();await beginAudience(e,300);expect((await broadcastSession(e,'10001'))!.stage).toBe('audience');const controls=JSON.stringify(requests.at(-1)!.params.reply_markup);expect(controls).toContain('全体符合条件用户');expect(controls).toContain('指定多个 TGID');
  await input(e,302,'not a range choice');expect((await broadcastSession(e,'10001'))!.stage).toBe('audience');expect(await all(e,'SELECT id FROM broadcasts')).toHaveLength(0);
  await selectAudience(e,303,'all');expect((await broadcastSession(e,'10001'))!.stage).toBe('content');expect(String(requests.at(-1)!.params.text)).toContain('接收范围：全体符合条件用户');
 });
 it('collects multiple ids, normalizes duplicates and previews exact directed eligibility counts without creating users',async()=>{
  const {e,requests}=client();await recipient(e,'20001');await recipient(e,'20002',1);await beginDirected(e,310,'20003，20001 20002\n20001');
  expect(JSON.parse((await broadcastSession(e,'10001'))!.payload).audience).toEqual({mode:'directed',ids:['20001','20002','20003']});await input(e,312,'directed body');await click(e,313,'broadcast_preview');
  const id=await campaign(e);expect(await one(e,'SELECT scope_json,total FROM broadcasts WHERE id=?',id)).toEqual({scope_json:JSON.stringify({mode:'directed',ids:['20001','20002','20003']}),total:1});expect(await all(e,'SELECT tgid FROM users ORDER BY tgid')).toEqual([{tgid:'20001'},{tgid:'20002'}]);
  const scope=requests.find(r=>r.key.includes('broadcast-scope-preview:'))!;expect(String(scope.params.text)).toContain('20001\n20002\n20003');expect(String(scope.params.text)).toContain('去重后指定：3 个');expect(String(scope.params.text)).toContain('预览时符合接收：1 人');expect(String(scope.params.text)).toContain('预览时排除：2 个');
  expect(requests.find(r=>r.key.includes('broadcast-preview:'))!.params).toEqual({chat_id:e.ADMIN_CHAT_ID,text:'directed body'});
  await click(e,314,'broadcast_confirm_first');expect(String(requests.at(-1)!.params.text)).toContain('去重后指定：3 个');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
  await click(e,315,'broadcast_confirm_second');expect(String(requests.at(-1)!.params.text)).toContain('接收范围：指定多个 TGID');expect(await all(e,'SELECT user_id FROM broadcast_recipients WHERE broadcast_id=?',id)).toEqual([{user_id:'20001'}]);
 });
 it('recovers invalid directed-id input in place and accepts only a new valid list',async()=>{
  const {e,requests}=client();await beginAudience(e,320);await selectAudience(e,321,'directed');
  for(const [index,text] of ['@username','0','-1','01','4503599627370496','20001 bad',Array.from({length:201},(_,i)=>String(i+1)).join(',')].entries()) {await input(e,322+index,text);expect((await broadcastSession(e,'10001'))!.stage).toBe('audience_ids');expect(String(requests.at(-1)!.params.text)).toContain('请输入 1–200 个有效用户 ID');}
  await input(e,330,'20001, 20002\n20001');expect((await broadcastSession(e,'10001'))!.stage).toBe('content');expect(await all(e,'SELECT tgid FROM users')).toHaveLength(0);
 });
 it('keeps the full 200-id scope preview within Telegram limits and separate from long text and picture',async()=>{
  const {e,requests}=client();const ids=Array.from({length:200},(_,i)=>String(4503599627370000+i));await beginDirected(e,340,ids.join('\n'));const text='x'.repeat(3500);await input(e,342,text);await input(e,343,undefined,'directed-picture');
  const scope=requests.find(r=>r.key.includes('broadcast-scope-preview:'))!,scopeText=String(scope.params.text);expect(scopeText.length).toBeLessThanOrEqual(4096);for(const id of ids)expect(scopeText).toContain(id);
  const body=requests.filter(r=>r.key.includes('broadcast-preview:'));expect(body).toHaveLength(2);expect(body[0].params).toEqual({chat_id:e.ADMIN_CHAT_ID,photo:'directed-picture'});expect(body[1].params).toEqual({chat_id:e.ADMIN_CHAT_ID,text});expect(await latest(e,'broadcast_confirm_first')).not.toBeNull();
 });
 it('withholds confirmation if the complete directed scope preview result is unknown',async()=>{
  const {e,requests}=client(r=>r.key.includes('broadcast-scope-preview:')?Response.json({status:'unknown',reason:'response lost'}):undefined);await beginDirected(e,350,'20001');await input(e,352,'body');await click(e,353,'broadcast_preview');
  expect(await latest(e,'broadcast_confirm_first')).toBeNull();expect(String(requests.at(-1)!.params.text)).toContain('完整预览未获确认');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);expect(requests.filter(r=>r.key.includes('broadcast-preview:'))).toHaveLength(0);
 });
 it('changes a confirmed audience while preserving content and invalidates the old second confirmation',async()=>{
  const {e,requests}=client();await recipient(e,'20001');await recipient(e,'20002');await beginDirected(e,360,'20001');await input(e,362,'keep body');await input(e,363,undefined,'keep-photo');const oldId=await campaign(e);await click(e,364,'broadcast_confirm_first');const oldSecond=await latest(e,'broadcast_confirm_second');
  await click(e,365,'broadcast_change_audience');expect((await broadcastSession(e,'10001'))!.stage).toBe('audience');expect(JSON.parse((await broadcastSession(e,'10001'))!.payload)).toMatchObject({text:'keep body',photo:'keep-photo'});expect(await one(e,'SELECT state FROM broadcasts WHERE id=?',oldId)).toEqual({state:'canceled'});
  await selectAudience(e,366,'directed');await input(e,367,'20002');const newId=await campaign(e);expect(newId).not.toBe(oldId);expect((await broadcastSession(e,'10001'))!.stage).toBe('preview');expect(await one(e,'SELECT text,photo,scope_json FROM broadcasts WHERE id=?',newId)).toEqual({text:'keep body',photo:'keep-photo',scope_json:JSON.stringify({mode:'directed',ids:['20002']})});
  expect(await handleBroadcastAction(e,job(368),oldSecond,JSON.parse(oldSecond.payload),369,'10001')).toContain('流程已变化');expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);expect(String(requests.at(-1)!.params.text)).toContain('第一次确认只确认内容与范围');
  await click(e,369,'broadcast_confirm_first');await click(e,370,'broadcast_confirm_second');expect(await all(e,'SELECT user_id FROM broadcast_recipients WHERE broadcast_id=?',newId)).toEqual([{user_id:'20002'}]);
 });
 it('supports directed-to-all range changes and preserves scope during content and picture edits',async()=>{
  const {e}=client();await beginDirected(e,380,'20001');await input(e,382,undefined,'photo',10001,{caption:'caption'});await click(e,383,'broadcast_preview');
  const edits=await all<BroadcastAction>(e,"SELECT * FROM callback_actions WHERE kind='broadcast_edit' ORDER BY rowid DESC"),textEdit=edits.find(a=>JSON.parse(a.payload).stage==='text')!;await handleBroadcastAction(e,job(384),textEdit,JSON.parse(textEdit.payload),385,'10001');await input(e,385,'changed text');expect(JSON.parse((await broadcastSession(e,'10001'))!.payload).audience).toEqual({mode:'directed',ids:['20001']});
  await click(e,386,'broadcast_remove_photo');expect(JSON.parse((await broadcastSession(e,'10001'))!.payload)).toMatchObject({audience:{mode:'directed',ids:['20001']},text:'changed text'});await click(e,387,'broadcast_change_audience');await selectAudience(e,388,'all');expect((await broadcastSession(e,'10001'))!.stage).toBe('preview');const id=await campaign(e);expect(await one(e,'SELECT scope_json,text,photo FROM broadcasts WHERE id=?',id)).toEqual({scope_json:JSON.stringify({mode:'all'}),text:'changed text',photo:null});
 });
 it('keeps directed scope when removing the only picture and choosing new content',async()=>{
  const {e}=client();await beginDirected(e,390,'20001');await input(e,392,undefined,'photo-only');await click(e,393,'broadcast_preview');await click(e,394,'broadcast_remove_photo');expect((await broadcastSession(e,'10001'))!.stage).toBe('content');expect(JSON.parse((await broadcastSession(e,'10001'))!.payload).audience).toEqual({mode:'directed',ids:['20001']});
  await input(e,395,'replacement body');await click(e,396,'broadcast_preview');expect(await one(e,'SELECT scope_json FROM broadcasts WHERE id=?',await campaign(e))).toEqual({scope_json:JSON.stringify({mode:'directed',ids:['20001']})});
 });
 it('intercepts late directed-id input after returning to audience choice and rejects another administrator',async()=>{
  const {e}=client();await beginAudience(e,400);await selectAudience(e,401,'directed');const m=message(e,402,'20001'),admission=await captureBroadcastInput(e,m);const back=await latest(e,'broadcast_audience_back');expect(await handleBroadcastAction(e,job(403),back,JSON.parse(back.payload),404,'10002')).toBe('无权操作。');
  await click(e,404,'broadcast_audience_back');await handleBroadcastInput(e,job(405,admission),{update_id:405,message:m});expect((await broadcastSession(e,'10001'))!.stage).toBe('audience');expect(await all(e,'SELECT id FROM broadcasts')).toHaveLength(0);
 });
 it('continues a legacy content draft without audience as the existing all-users scope',async()=>{
  const {e,requests}=client();await begin(e,410);await run(e,"UPDATE broadcast_sessions SET payload='{}',stage='content' WHERE instance_id=? AND actor_id='10001'",e.INSTANCE_ID);await input(e,412,'legacy text');await click(e,413,'broadcast_preview');expect(await one(e,'SELECT scope_json,text FROM broadcasts')).toEqual({scope_json:JSON.stringify({mode:'all'}),text:'legacy text'});expect(String(requests.at(-1)!.params.text)).toContain('接收范围：全体符合条件用户');
 });
 it('never lets a directed test audience bypass the private test recipient boundary',async()=>{
  const {e,requests}=client();const test={...e,INSTANCE_ID:'test-a',BROADCAST_TEST_RECIPIENT_IDS:'20001'};await recipient(test,'20001');await recipient(test,'20002');await beginDirected(test,420,'20001,20002');await input(test,422,'test body');await click(test,423,'broadcast_preview');
  const id=await campaign(test);expect(await all(test,'SELECT user_id FROM broadcast_recipients WHERE broadcast_id=?',id)).toEqual([{user_id:'20001'}]);expect(String(requests.at(-1)!.params.text)).toContain('预览时排除：1 个');expect(String(requests.at(-1)!.params.text)).toContain('测试账号限定范围');
 });
 it('answers the first confirmation after committing its stage and before sending the next prompt',async()=>{
  const {e,requests}=client();await textPreview(e,430);const a=await latest(e,'broadcast_confirm_first'),p=JSON.parse(a.payload),before=requests.length;
  const update={update_id:434,callback_query:{id:'first-ack',from:{id:10001},data:`a:${a.token}`,message:{...message(e,434),message_id:p.messageId,from:{id:Number(e.EXPECTED_BOT_ID)}}}};await handleAdmin(e,job(434),update);
  const next=requests.slice(before);expect(next[0].method).toBe('answerCallbackQuery');expect(next[0].params.text).toContain('第一次确认已记录');expect(next.slice(1).some(r=>String(r.params.text).includes('第二次确认'))).toBe(true);expect((await broadcastSession(e,'10001'))!.stage).toBe('second_confirm');
 });

 it('recovers a rate-limited directed range preview without replacing the campaign or duplicating content',async()=>{
  let limited=true;const {e,requests}=client(r=>{if(r.key.includes('broadcast-scope-preview:')&&limited){limited=false;return Response.json({due:now()+10},{status:429});}});await beginDirected(e,440,'20001');await input(e,442,'body');const a=await latest(e,'broadcast_preview'),p=JSON.parse(a.payload);
  await expect(handleBroadcastAction(e,job(443),a,p,444,'10001')).rejects.toBeInstanceOf(RetryLater);const id=await campaign(e);await handleBroadcastAction(e,job(443),a,p,444,'10001');expect(await campaign(e)).toBe(id);expect(await all(e,'SELECT id FROM broadcasts')).toHaveLength(1);expect(requests.filter(r=>r.key.includes('broadcast-preview:'))).toHaveLength(1);expect(await latest(e,'broadcast_confirm_first')).not.toBeNull();
 });
 it('reports a corrupted historical scope instead of describing it as an all-users broadcast',async()=>{
  const {e,requests}=client();await textPreview(e,450);const id=await campaign(e);await run(e,"UPDATE broadcasts SET scope_json='',state='canceled' WHERE id=?",id);await broadcastPanel(e,job(454),'10001');await click(e,455,'broadcast_status');const text=String(requests.at(-1)!.params.text);expect(text).toContain('接收范围记录异常');expect(text).not.toContain('接收范围：全体符合条件用户');
 });
 it('acknowledges the second confirmation only after persisting its permission and before the launch work',async()=>{
  const {e}=client();await recipient(e);await textPreview(e,460);await click(e,464,'broadcast_confirm_first');const a=await latest(e,'broadcast_confirm_second'),p=JSON.parse(a.payload),id=await campaign(e);let observed=false;
  await handleBroadcastAction(e,job(465),a,p,466,'10001',undefined,undefined,async text=>{
   observed=true;expect(text).toContain('两次确认已记录');expect((await broadcastSession(e,'10001'))!.stage).toBe('done');expect(await one(e,'SELECT state FROM broadcasts WHERE id=?',id)).toEqual({state:'confirmed'});expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(0);
  });expect(observed).toBe(true);expect(await all(e,"SELECT id FROM jobs WHERE kind='broadcast'")).toHaveLength(1);
 });

});
