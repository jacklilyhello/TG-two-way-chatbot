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
async function begin(e:Env,id=1,actor=10001) {await broadcastPanel(e,job(id),String(actor));await click(e,id+1,'broadcast_new',actor);}
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

});
