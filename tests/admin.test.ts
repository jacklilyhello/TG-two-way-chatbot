import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it} from 'vitest';
import type {Env,Job,Params,TGMessage,TGUpdate} from '../src/types';
import {all,one,run,user,setSetting} from '../src/db';
import {accept} from '../src/ingress';

const api = {requests:[] as {key:string;method:string;params:Record<string,unknown>}[],overrides:new Map<string,unknown>(),sequence:700};
const bot = {
 idFromName:(name:string)=>name,
 get:()=>({fetch:async (_url:string,init:RequestInit)=>{
  const body=JSON.parse(String(init.body)) as {key:string;method:string;params:Params};
  const key=body.key.replace(/^[^:]+:/,''),{method,params}=body;
  api.requests.push({key,method,params});
  let result:unknown=true;
  if(method==='getUserProfilePhotos') result={photos:[]};
  if(method==='getChat') result={id:Number(params.chat_id),first_name:'Updated'};
  if(method==='createForumTopic') result={message_thread_id:++api.sequence};
  if(method==='sendMessage'||method==='sendPhoto') result={message_id:++api.sequence};
  return Response.json(api.overrides.get(key) ?? {status:'success',result});
 }})
};
import {ensureUser,ensureThread,handleAdmin,handleCard,scheduleCard,profileLinks,rangeStart,refreshCardNow as refreshCard,statistics,validUserId} from '../src/admin';
import {RetryLater} from '../src/operations';
const e={...env,BOT:bot} as unknown as Env;
const job=(id:number,user_key='20001'):Job=>({id,dedupe:`test:${id}`,user_key,kind:'update',payload:'{}',state:'running',due_at:0,attempts:0,epoch:e.RECOVERY_EPOCH,created_at:1});
const message=(id:number,text:string,thread?:number):TGMessage=>({message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup',is_forum:true},from:{id:10001},text,...(thread?{message_thread_id:thread,is_topic_message:true}:{})});
async function settingCommand(id:number,text:string,thread?:number) {
 const update={update_id:id,message:message(id,text,thread)};
 await accept(e,update);
 const saved=(await one<Job>(e,'SELECT * FROM jobs WHERE dedupe=?',`u:${id}`))!;
 return handleAdmin(e,saved,update);
}
const callback=(id:number,token:string,messageId:number,thread:number,actor=10001):TGUpdate=>({update_id:id,callback_query:{id:`callback:${id}`,from:{id:actor},data:`a:${token}`,message:{...message(messageId,'',thread),from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true}}}});
async function registered(id='20001',thread=40,version=1,card=500) {
 await run(e,'INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES(?,1,?,?,?)',id,thread,version,card);
}
function prompt() {
 const request=api.requests.findLast(r=>r.method==='sendMessage' && (r.params.text as string).includes('确认切换'))!;
 const buttons=(request.params.reply_markup as {inline_keyboard:{callback_data:string}[][]}).inline_keyboard[0];
 return {token:buttons[0].callback_data.slice(2),cancel:buttons[1].callback_data.slice(2),messageId:api.sequence};
}
beforeEach(()=>{api.requests.length=0;api.overrides.clear();api.sequence=700;});

describe('administrator bindings and profile cards',()=>{
 it('preserves first contact while refreshing genuine profile fields and provides valid username-free links',async()=>{
  const initial=await ensureUser(e,'20001',{id:20001,first_name:'Alice',last_name:'Test',username:'alice_test'});
  const updated=await ensureUser(e,'20001',{id:20001,first_name:'New'});
  expect(updated.first_contact).toBe(initial.first_contact);
  expect(updated.username).toBeNull();
  expect(profileLinks(updated)).toEqual(['tg://user?id=20001','tg://user?id=20001','tg://user?id=20001']);
  expect(profileLinks({tgid:'20001',username:'alice_test'})).toEqual(['https://t.me/alice_test?profile','tg://resolve?domain=alice_test&profile','tg://resolve?domain=alice_test&profile']);
  expect(validUserId('2000000000000')).toBe(true);
  expect(validUserId('4503599627370496')).toBe(false);
 });
 it('records the earliest durable private entry when processing is delayed behind a paused backlog',async()=>{
  const first=Date.parse('2026-10-01T05:00:00Z'),later=Date.parse('2026-10-03T05:00:00Z');
  for(const [id,received] of [[70,first],[71,later]]) await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,private_id,received_at,payload) VALUES('20001','in','20001',?,?,?,'{}')",id,id,received);
  const u=await ensureUser(e,'20001',{id:20001,first_name:'Delayed'});
  expect(u.first_contact).toBe(first);
  const refreshed=await ensureUser(e,'20001',{id:20001,first_name:'Refreshed'},later);
  expect(refreshed.first_contact).toBe(first);
 });
 it('uses administrator registration receipt time before later private arrivals and keeps it on repeat registration',async()=>{
  const registeredAt=Date.parse('2026-10-01T04:00:00Z'),privateAt=Date.parse('2026-10-02T04:00:00Z');
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,private_id,received_at,payload) VALUES('20001','in','20001',72,72,?,'{}')",privateAt);
  await handleAdmin(e,{...job(72),created_at:registeredAt},{update_id:72,message:message(72,'/tj 20001',40)});
  expect((await user(e,'20001'))!.first_contact).toBe(registeredAt);
  await handleAdmin(e,{...job(73),created_at:privateAt+86400000},{update_id:73,message:message(73,'/tj 20001',40)});
  expect((await user(e,'20001'))!.first_contact).toBe(registeredAt);
 });
 it('retrieves available profile data when registering an old topic and retains it on a later unavailable lookup',async()=>{
  await ensureUser(e,'20001');await run(e,"UPDATE users SET first_contact=1 WHERE tgid='20001'");
  api.overrides.set('job:60:profile:20001',{status:'success',result:{id:20001,first_name:'Retrieved',last_name:'Profile',username:'retrieved_user'}});
  await handleAdmin(e,job(60),{update_id:60,message:message(60,'/tj 20001',40)});
  expect(await user(e,'20001')).toMatchObject({first_name:'Retrieved',last_name:'Profile',username:'retrieved_user',first_contact:1,thread_id:40});
  expect(api.requests.filter(r=>r.method==='getChat')).toHaveLength(1);
  api.overrides.set('job:61:profile:20001',{status:'failed',code:400,reason:'chat not found'});
  await handleAdmin(e,job(61),{update_id:61,message:message(61,'/tj 20001',40)});
  expect(await user(e,'20001')).toMatchObject({first_name:'Retrieved',username:'retrieved_user',first_contact:1,thread_id:40});
 });
 it('allows only one user to claim an unoccupied topic under concurrent registration',async()=>{
  await Promise.all([
   handleAdmin(e,job(1,'20001'),{update_id:1,message:message(1,'/tj 20001',50)}),
   handleAdmin(e,job(2,'20002'),{update_id:2,message:message(2,'/tj 20002',50)})
  ]);
  expect((await all(e,'SELECT tgid FROM users WHERE thread_id=50')).length).toBe(1);
  expect((await all(e,'SELECT * FROM thread_history')).length).toBe(1);
  expect((await all(e,'SELECT * FROM users')).length).toBe(2);
 });
 it('rejects stale binding confirmation and a destination claimed after the prompt',async()=>{
  await registered();
  await handleAdmin(e,job(3),{update_id:3,message:message(3,'/tj 20001',41)});
  const stale=prompt();
  await run(e,'UPDATE users SET thread_id=42,binding_version=2 WHERE tgid=?','20001');
  await handleAdmin(e,job(4),callback(4,stale.token,stale.messageId,41));
  expect((await user(e,'20001'))!.thread_id).toBe(42);
  api.requests.length=0;
  await handleAdmin(e,job(5),{update_id:5,message:message(5,'/tj 20001',43)});
  const occupied=prompt();
  await registered('20002',43);
  await handleAdmin(e,job(6),callback(6,occupied.token,occupied.messageId,43));
  expect((await user(e,'20001'))!.thread_id).toBe(42);
  expect((await user(e,'20002'))!.thread_id).toBe(43);
  expect((await all(e,'SELECT * FROM thread_history')).length).toBe(0);
 });
 it('switches only after confirmation, retains history and mappings, and invalidates the old card',async()=>{
  await registered();
  await run(e,"INSERT INTO messages(user_id,direction,source_chat,source_id,admin_id,private_id,thread_id,received_at,payload) VALUES('20001','in','20001',1,600,1,40,1,'{}')");
  await run(e,"INSERT INTO callback_actions VALUES(?,'card_block','20001','*',40,1,?,?,0)",'a'.repeat(32),JSON.stringify({instance:e.INSTANCE_ID}),Number.MAX_SAFE_INTEGER);
  await handleAdmin(e,job(7),{update_id:7,message:message(7,'/tj 20001',41)});
  expect((await user(e,'20001'))!.thread_id).toBe(40);
  const confirm=prompt();
  await handleAdmin(e,job(8),callback(8,confirm.token,confirm.messageId,41));
  expect(await user(e,'20001')).toMatchObject({thread_id:41,binding_version:2,first_contact:1});
  expect(await all(e,'SELECT thread_id,version FROM thread_history')).toEqual([{thread_id:41,version:2}]);
  expect((await all(e,'SELECT * FROM messages')).length).toBe(1);
  await handleAdmin(e,job(9),callback(9,'a'.repeat(32),500,40));
  expect((await user(e,'20001'))!.blocked).toBe(0);
  await handleAdmin(e,job(10),{update_id:10,message:message(10,'/tj 20001',41)});
  expect((await all(e,'SELECT * FROM thread_history')).length).toBe(1);
  expect(api.requests.some(r=>['deleteMessage','deleteForumTopic'].includes(r.method))).toBe(false);
 });
 it('retires old controls even when the current card message is retained',async()=>{
  await registered();
  await refreshCard(e,job(11),'20001');
  const original=(await one<{token:string}>(e,"SELECT token FROM callback_actions WHERE kind='card_block' AND used=0"))!.token;
  await ensureUser(e,'20001',{id:20001,first_name:'Updated'});
  await refreshCard(e,job(12),'20001');
  expect((await one<{used:number}>(e,'SELECT used FROM callback_actions WHERE token=?',original))!.used).toBe(-1);
  await handleAdmin(e,job(13),callback(13,original,500,40));
  expect((await user(e,'20001'))!.blocked).toBe(0);
  const current=(await one<{token:string}>(e,"SELECT token FROM callback_actions WHERE kind='card_block' AND used=0"))!.token;
  await handleAdmin(e,job(14),callback(14,current,500,40));
  expect((await user(e,'20001'))!.blocked).toBe(1);
 });
 it('avoids administrator-group edits and pins when profile and state are unchanged',async()=>{
  await registered();await refreshCard(e,job(18),'20001');
  api.requests.length=0;
  await refreshCard(e,job(19),'20001');
  expect(api.requests).toEqual([]);
 });
 it('refreshes cached avatars after sixty seconds and lets explicit card repair bypass the cache',async()=>{
  await registered();await refreshCard(e,job(80),'20001');
  api.requests.length=0;
  await refreshCard(e,job(81),'20001',true);
  expect(api.requests.filter(r=>r.method==='getUserProfilePhotos')).toHaveLength(1);
  await setSetting(e,'profile_checked:20001',String(Date.now()-60001));
  api.overrides.set('job:82:avatar:20001',{status:'success',result:{photos:[[{file_id:'new-avatar'}]]}});
  await refreshCard(e,job(82),'20001');
  expect((await user(e,'20001'))!.photo_id).toBe('new-avatar');
  expect(api.requests.find(r=>r.method==='sendPhoto')?.params.photo).toBe('new-avatar');
 });
 it('repairs only a confirmed privacy-rejected card using a stable link-free operation',async()=>{
  await registered();await run(e,"UPDATE users SET card_id=NULL WHERE tgid='20001'");
  api.overrides.set('card:20001:1:create',{status:'failed',code:400,reason:'Bad Request: BUTTON_USER_PRIVACY_RESTRICTED'});
  await refreshCard(e,job(83),'20001');
  const sends=api.requests.filter(r=>r.method==='sendMessage');
  expect(sends.map(r=>r.key)).toEqual(['card:20001:1:create','card:20001:1:create:without-private-links']);
  expect(JSON.stringify(sends[0].params.reply_markup)).toContain('tg://user?id=');
  expect(JSON.stringify(sends[1].params.reply_markup)).not.toContain('tg://user?id=');
  expect(JSON.stringify(sends[1].params.reply_markup)).toContain('清空此对话');
  expect((await user(e,'20001'))!.card_id).toBeGreaterThan(0);
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:card:20001:1:create'")).toEqual({phase:'failed'});
  api.requests.length=0;await refreshCard(e,job(84),'20001');
  expect(api.requests).toEqual([]);
 });
 it('never retries a card whose external result is unknown',async()=>{
  await registered();await run(e,"UPDATE users SET card_id=NULL WHERE tgid='20001'");
  api.overrides.set('card:20001:1:create',{status:'unknown',reason:'response lost'});
  await refreshCard(e,job(85),'20001');await refreshCard(e,job(86),'20001');
  expect(api.requests.filter(r=>r.method==='sendMessage').map(r=>r.key)).toEqual(['card:20001:1:create']);
  expect((await user(e,'20001'))!.card_id).toBeNull();
 });
 it('deduplicates background card jobs, yields to business backlog and rejects stale bindings',async()=>{
  await registered();const u=(await user(e,'20001'))!;
  const client={...e,JOBS:{send:async()=>{}}} as unknown as Env;
  await scheduleCard(client,job(87),u);await scheduleCard(client,job(88),u);
  const jobs=await all<Job>(e,"SELECT * FROM jobs WHERE kind='card'");
  expect(jobs).toHaveLength(1);expect(jobs[0].user_key).toBe('card:20001');
  await run(e,"INSERT INTO jobs(dedupe,user_key,kind,payload,state,due_at,epoch,created_at) VALUES('business','20001','update','{}','pending',0,'initial',1)");
  await expect(handleCard(e,jobs[0])).rejects.toBeInstanceOf(RetryLater);
  expect(api.requests).toEqual([]);
  await run(e,"UPDATE jobs SET state='done' WHERE dedupe='business'");
  await run(e,"UPDATE users SET binding_version=2,thread_id=41 WHERE tgid='20001'");
  await handleCard(e,jobs[0]);expect(api.requests).toEqual([]);
 });
 it('creates only the required topic when business delivery defers card maintenance',async()=>{
  const u=await ensureThread(e,job(89),'20001',false);
  expect(u.thread_id).toBeGreaterThan(1);expect(u.card_id).toBeNull();
  expect(api.requests.map(r=>r.method)).toEqual(['getChat','createForumTopic']);
 });
 it('preserves a forced follow-up arriving while the card lane is running',async()=>{
  await registered();const u=(await user(e,'20001'))!;
  const client={...e,JOBS:{send:async()=>{}}} as unknown as Env;
  await scheduleCard(client,job(90),u);
  await run(e,"UPDATE jobs SET state='running' WHERE kind='card'");
  await scheduleCard(client,job(91),u,true);await scheduleCard(client,job(92),u);
  const jobs=await all<{state:string;payload:string}>(e,"SELECT state,payload FROM jobs WHERE kind='card' ORDER BY id");
  expect(jobs).toHaveLength(2);
  expect(jobs.map(j=>j.state)).toEqual(['running','pending']);
  expect(JSON.parse(jobs[1].payload).force).toBe(true);
 });
 it('redirects ordinary messages in a retired topic without forwarding or removing history',async()=>{
  await registered('20001',41,2,500);
  await run(e,"INSERT INTO thread_history(user_id,thread_id,version,created_at) VALUES('20001',40,1,1)");
  expect(await handleAdmin(e,job(20),{update_id:20,message:message(20,'test retired topic',40)})).toBe(true);
  const warning=api.requests.find(r=>r.method==='sendMessage')!;
  expect(warning.params.message_thread_id).toBe(40);
  expect(JSON.stringify(warning.params.reply_markup)).toContain('/41/500');
  expect(api.requests.some(r=>r.method==='copyMessage'||r.method==='deleteMessage')).toBe(false);
 });
 it('marks originals admitted before a queued rebind as suppressed while preserving their business count',async()=>{
  await registered('20001',40,1,500);
  const received={update_id:62,message:message(62,'queued original',40)};
  await accept(e,received);
  await run(e,"UPDATE users SET thread_id=41,binding_version=2 WHERE tgid='20001'");
  await run(e,"INSERT INTO thread_history(user_id,thread_id,version,created_at) VALUES('20001',40,1,1)");
  expect(await handleAdmin(e,job(62),received)).toBe(true);
  expect(await one(e,"SELECT delivery,business FROM messages WHERE source_chat=? AND source_id=62",e.ADMIN_CHAT_ID)).toEqual({delivery:'suppressed',business:1});
  expect(api.requests.some(r=>r.method==='copyMessage')).toBe(false);
 });
 it('keeps an unknown topic creation under one stable ledger key across new-message jobs',async()=>{
  api.overrides.set('thread:create:20001',{status:'unknown',reason:'timeout'});
  await ensureThread(e,job(15),'20001');await ensureThread(e,job(16),'20001');
  await handleAdmin(e,job(17),{update_id:17,message:message(17,'/tj 20001')});
  expect((await user(e,'20001'))!.thread_id).toBeNull();
  expect(api.requests.filter(r=>r.method==='createForumTopic').map(r=>r.key)).toEqual(['thread:create:20001']);
  expect((await all(e,'SELECT * FROM thread_history')).length).toBe(0);
 });
 it('permits explicit /tj repair after an API rejection while preserving the original failed ledger entry',async()=>{
  api.overrides.set('thread:create:20001',{status:'failed',code:400,reason:'not enough rights'});
  await ensureThread(e,job(40),'20001');
  expect((await user(e,'20001'))!.thread_id).toBeNull();
  await handleAdmin(e,job(41),{update_id:41,message:message(41,'/tj 20001')});
  expect((await user(e,'20001'))!.thread_id).toBeGreaterThan(1);
  expect(api.requests.filter(r=>r.method==='createForumTopic').map(r=>r.key)).toEqual(['thread:create:20001','thread:create:20001:retry:41']);
  expect(await one(e,"SELECT phase FROM operations WHERE op_key='initial:thread:create:20001'")).toEqual({phase:'failed'});
 });
 it('uses available avatar with a pinned photo card and preserves existing administrator messages',async()=>{
  await registered('20001',40,1,500);
  api.overrides.set('job:17:avatar:20001',{status:'success',result:{photos:[[{file_id:'small'},{file_id:'large'}]]}});
  await refreshCard(e,job(17),'20001');
  const photo=api.requests.find(r=>r.method==='sendPhoto')!;
  expect(photo.params.photo).toBe('large');
  expect(photo.params.caption).toContain('用户名：未设置');
  expect(api.requests.some(r=>r.method==='pinChatMessage')).toBe(true);
  expect(api.requests.some(r=>r.method==='unpinChatMessage'&&r.params.message_id===500)).toBe(true);
  expect(api.requests.some(r=>r.method==='deleteMessage')).toBe(false);
 });
});

describe('management authorization and clear boundaries',()=>{
 it('consumes unauthorized messages, every slash command, and callback traffic without forwarding',async()=>{
  const unauthorized=message(20,'ordinary message',40);unauthorized.from={id:99999};
  expect(await handleAdmin(e,job(20),{update_id:20,message:unauthorized})).toBe(true);
  expect(await handleAdmin(e,job(21),{update_id:21,message:message(21,'/unknown text',40)})).toBe(true);
  expect(await handleAdmin(e,job(22),callback(22,'a'.repeat(32),500,40,99999))).toBe(true);
  expect(api.requests).toHaveLength(0);
 });
 it('does not authorize buttons from another chat, bot, actor, card, or topic',async()=>{
  await registered();
  const token='b'.repeat(32);
  await run(e,"INSERT INTO callback_actions VALUES(?,'card_block','20001','*',40,1,?,?,0)",token,JSON.stringify({instance:e.INSTANCE_ID}),Number.MAX_SAFE_INTEGER);
  const wrongChat=callback(23,token,500,40);wrongChat.callback_query!.message!.chat.id=-100999999;
  const wrongBot=callback(24,token,500,40);wrongBot.callback_query!.message!.from!.id=99999;
  for(const [index,update] of [wrongChat,wrongBot,callback(25,token,500,40,99999),callback(26,token,501,40),callback(27,token,500,41)].entries()) await handleAdmin(e,job(23+index),update);
  expect((await user(e,'20001'))!.blocked).toBe(0);
 });
 it('uses the ingress snapshot after a later message, binding change, and confirmation expiry',async()=>{
  await registered();
  const token='c'.repeat(32);
  await run(e,"INSERT INTO callback_actions VALUES(?,'clear_confirm','20001','10001',40,1,?,?,0)",token,JSON.stringify({instance:e.INSTANCE_ID,messageId:501}),Date.now()+60000);
  const incoming=(id:number):TGUpdate=>({update_id:id,message:{message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001},text:'Test'}});
  await accept(e,incoming(30));
  const confirmed=callback(31,token,501,40);
  await accept(e,confirmed);await accept(e,incoming(32));
  await run(e,'UPDATE users SET thread_id=42,binding_version=2 WHERE tgid=?','20001');
  await run(e,'UPDATE callback_actions SET expires_at=1 WHERE token=?',token);
  await handleAdmin(e,job(31),confirmed);
  expect(await all(e,'SELECT source_id,tombstone FROM messages ORDER BY id')).toEqual([{source_id:30,tombstone:1},{source_id:32,tombstone:0}]);
  const clear=await one<{payload:string}>(e,"SELECT payload FROM jobs WHERE kind='clear'");
  expect(JSON.parse(clear!.payload)).toEqual({batchId:token});
  expect((await all(e,'SELECT * FROM deletion_items')).length).toBe(1);
 });
 it('never takes a delayed snapshot when a confirmation was not accepted at ingress',async()=>{
  await registered();
  const token='d'.repeat(32);
  await run(e,"INSERT INTO callback_actions VALUES(?,'clear_confirm','20001','10001',40,1,?,?,0)",token,JSON.stringify({instance:e.INSTANCE_ID,messageId:501}),Date.now()+60000);
  await handleAdmin(e,job(33),callback(33,token,501,40));
  expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
  expect(await one(e,"SELECT * FROM jobs WHERE kind='clear'")).toBeNull();
 });
 it('keeps the card clear button reusable after cancellation and refuses the canceled confirmation',async()=>{
  await registered();await refreshCard(e,job(50),'20001');
  const token=(await one<{token:string}>(e,"SELECT token FROM callback_actions WHERE kind='card_clear' AND used=0"))!.token;
  await handleAdmin(e,job(51),callback(51,token,500,40));
  const confirm=(await one<{token:string;payload:string}>(e,"SELECT token,payload FROM callback_actions WHERE kind='clear_confirm'"))!;
  const cancel=(await one<{token:string}>(e,"SELECT token FROM callback_actions WHERE kind='cancel'"))!;
  const promptId=JSON.parse(confirm.payload).messageId as number;
  const canceled=callback(52,cancel.token,promptId,40);
  await accept(e,canceled);await handleAdmin(e,job(52),canceled);
  await accept(e,callback(53,confirm.token,promptId,40));
  expect(await one(e,'SELECT * FROM deletion_batches')).toBeNull();
  await handleAdmin(e,job(54),callback(54,token,500,40));
  expect((await all(e,"SELECT * FROM callback_actions WHERE kind='clear_confirm'")).length).toBe(2);
 });
 it('requires General for settings and rejects reserved or occupied log topics',async()=>{
  await registered();
  await settingCommand(34,'/mb timezone Europe/London',40);
  expect(await one(e,"SELECT value FROM settings WHERE key='timezone'")).toBeNull();
  await settingCommand(35,'/mb logtopic 40');
  expect(await one(e,"SELECT value FROM settings WHERE key='log_thread_id'")).toBeNull();
  await settingCommand(36,'/mb timezone Bad/Timezone');
  expect(await one(e,"SELECT value FROM settings WHERE key='timezone'")).toBeNull();
 });
 it('rejects fixed templates whose supported variables could expand beyond Telegram limits',async()=>{
  const tooLarge='{name}'.repeat(16);
  await settingCommand(63,'/mb start '+tooLarge);
  expect(await one(e,"SELECT value FROM settings WHERE key='start_text'")).toBeNull();
  expect(api.requests.findLast(r=>r.method==='sendMessage')!.params.text).toContain('4096');
  const safe='{name}'.repeat(15);
  await settingCommand(64,'/mb start '+safe);
  expect(await one(e,"SELECT value FROM settings WHERE key='start_text'")).toEqual({value:safe});
  await settingCommand(65,'/mb help '+'{first_name}'.repeat(33));
  expect(await one(e,"SELECT value FROM settings WHERE key='help_text'")).toBeNull();
 });
});

describe('calendar statistics',()=>{
 it('includes today in seven/thirty local calendar days and respects daylight saving boundaries',()=>{
  const time=Date.parse('2026-10-11T05:00:00Z');
  expect(new Date(rangeStart('Asia/Singapore','today',time)).toISOString()).toBe('2026-10-10T16:00:00.000Z');
  expect(new Date(rangeStart('Asia/Singapore','7',time)).toISOString()).toBe('2026-10-04T16:00:00.000Z');
  expect(new Date(rangeStart('Asia/Singapore','30',time)).toISOString()).toBe('2026-09-11T16:00:00.000Z');
  const dst=Date.parse('2026-03-09T12:00:00Z');
  expect(new Date(rangeStart('America/New_York','7',dst)).toISOString()).toBe('2026-03-03T05:00:00.000Z');
 });
 it('starts a day at its first existing instant when daylight saving skips midnight',()=>{
  expect(new Date(rangeStart('Asia/Beirut','today',Date.parse('2026-03-29T12:00:00Z'))).toISOString()).toBe('2026-03-28T22:00:00.000Z');
  expect(new Date(rangeStart('Africa/Cairo','today',Date.parse('2026-04-24T12:00:00Z'))).toISOString()).toBe('2026-04-23T22:00:00.000Z');
 });
 it('counts business originals once and excludes unknown outcomes from the success denominator',async()=>{
  const timestamp=Date.parse('2026-10-11T05:00:00Z');
  await registered();await registered('20002',41);
  const insert=async(source:number,id:string,direction:string,delivery:string,business=1)=>run(e,'INSERT INTO messages(user_id,direction,source_chat,source_id,business,received_at,delivery,payload) VALUES(?,?,?,?,?,?,?,?)',id,direction,direction==='in'?id:e.ADMIN_CHAT_ID,source,business,timestamp-1000,delivery,'{}');
  await insert(1,'20001','in','success');await insert(2,'20001','in','success');await insert(3,'20002','in','success');
  await insert(4,'20001','out','success');await insert(5,'20001','out','failed');await insert(6,'20001','out','unknown');
  await insert(7,'20001','in','success',0);
  await run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES('album-failed','sendMediaGroup','failed',?,?)",timestamp-1000,timestamp-1000);
  await run(e,"INSERT INTO operations(op_key,method,phase,created_at,updated_at) VALUES('reaction-failed','setMessageReaction','failed',?,?)",timestamp-1000,timestamp-1000);
  const stats=await statistics(e,'today',timestamp);
  expect(stats.messages).toMatchObject({total:6,success:1,failed:1,unknown:1,active:2});
  expect(stats.rate).toBe('50.0%');expect(stats.today).toBe(6);
  expect(stats.anomalies).toBe(1);
 });
});
