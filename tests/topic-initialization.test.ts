import {env} from 'cloudflare:test';
import {describe,expect,it,vi} from 'vitest';
import type {Env,Job,Params,TGMessage} from '../src/types';
import {ensureThread,ensureInitialCardReady,refreshCardNow,handleAdmin} from '../src/admin';
import {all,one,run,setting,setSetting,user} from '../src/db';
import {accept} from '../src/ingress';
import {ConversationCoordinator} from '../src/coordinator';
import type {Outcome} from '../src/operations';

const base=env as unknown as Env;
const job=(id:number):Job=>({id,dedupe:`test:${id}`,user_key:'20001',kind:'update',payload:'{}',state:'running',due_at:0,attempts:0,epoch:base.RECOVERY_EPOCH,created_at:1});
type Request={key:string;method:string;params:Params};
function runtime(override?:(r:Request)=>Promise<Outcome|undefined>) {
 const requests:Request[]=[],journal=new Map<string,Promise<Outcome>>();
 let sequence=700;
 const e={...base,JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:(key:string)=>key,get:()=>({fetch:async()=>new Response('ok')})},BOT:{idFromName:()=>base.INSTANCE_ID,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const request=JSON.parse(String(init.body)) as Request;
  if(!journal.has(request.key))journal.set(request.key,(async()=>{
   requests.push(request);
   const replacement=await override?.(request);
   if(replacement)return replacement;
   const result=request.method==='getChat'?{id:20001,first_name:'Synthetic'}:request.method==='createForumTopic'?{message_thread_id:40}:request.method==='getUserProfilePhotos'?{photos:[[{file_id:'synthetic-avatar'}]]}:request.method==='sendMediaGroup'?(request.params.media as unknown[]).map(()=>({message_id:++sequence})):['sendMessage','sendPhoto','copyMessage'].includes(request.method)?{message_id:++sequence}:true;
   return {status:'success',result};
  })());
  return Response.json(await journal.get(request.key));
 }})}} as unknown as Env;
 return {e,requests};
}
const incoming=(id:number,extra:Partial<TGMessage>={}):TGMessage=>({message_id:id,date:1,chat:{id:20001,type:'private'},from:{id:20001,first_name:'Synthetic'},text:`[test init] ${id}`,...extra});
function conversation(e:Env,pending:Promise<unknown>[]) {
 const values=new Map<string,unknown>();
 const state={storage:{get:async(key:string)=>values.get(key),put:async(key:string,value:unknown)=>{values.set(key,value);},setAlarm:async()=>{}},waitUntil:(promise:Promise<unknown>)=>{pending.push(promise);}} as unknown as DurableObjectState;
 const coordinator=new ConversationCoordinator(state,e);
 return ()=>coordinator.fetch(new globalThis.Request('https://conversation/wake',{method:'POST',body:JSON.stringify({key:'20001'})}));
}

describe('new-topic durable initial card gate',()=>{
 it('creates the gate atomically with the binding and pins exactly one panel across overlapping lanes',async()=>{
  const {e,requests}=runtime();
  const u=await ensureThread(e,job(1),'20001',false);
  expect(await setting(e,'card_initial:20001:1')).toBe('pending:40');
  expect(u.card_id).toBeNull();
  expect(await Promise.all([ensureInitialCardReady(e,job(2),'20001'),ensureInitialCardReady(e,job(3),'20001')])).toEqual([true,true]);
  expect(requests.filter(r=>r.method==='sendMessage')).toHaveLength(1);
  expect(requests.filter(r=>r.method==='pinChatMessage')).toHaveLength(1);
  expect(requests.some(r=>r.method==='getUserProfilePhotos')).toBe(false);
  expect(await all(e,"SELECT kind,COUNT(*) n FROM callback_actions GROUP BY kind ORDER BY kind")).toEqual([{kind:'card_block',n:1},{kind:'card_clear',n:1},{kind:'card_profile_android',n:1},{kind:'card_profile_general',n:1},{kind:'card_profile_ios',n:1}]);
  const current=(await user(e,'20001'))!;
  expect(await setting(e,'card_initial:20001:1')).toBe(`ready:40:${current.card_id}`);
  await refreshCardNow(e,job(4),'20001');
  expect((await user(e,'20001'))!.card_id).toBe(current.card_id);
  expect(requests.filter(r=>['sendMessage','sendPhoto'].includes(r.method))).toHaveLength(1);
  expect(requests.some(r=>r.method==='unpinChatMessage')).toBe(false);
 });

 it.each(['create','pin'] as const)('never reissues an unknown initial %s and keeps the gate closed',async(stage)=>{
  const {e,requests}=runtime(async r=>r.method===(stage==='create'?'sendMessage':'pinChatMessage')?{status:'unknown',reason:'Synthetic unknown outcome'}:undefined);
  await ensureThread(e,job(10),'20001',false);
  for(const id of [11,12])await expect(ensureInitialCardReady(e,job(id),'20001')).rejects.toThrow('initial_card_not_confirmed');
  expect(requests.filter(r=>r.method==='sendMessage')).toHaveLength(1);
  expect(requests.filter(r=>r.method==='pinChatMessage')).toHaveLength(stage==='pin'?1:0);
  expect(await setting(e,'card_initial:20001:1')).toBe('pending:40');
  expect(await all(e,"SELECT event FROM audit_logs WHERE event LIKE 'initial_card_%'")).toEqual([{event:`initial_card_${stage}_unavailable`}]);
 });

 it('allows an explicit registration to repair a known pin rejection without resending the panel',async()=>{
  let reject=true;
  const {e,requests}=runtime(async r=>r.method==='pinChatMessage'&&reject?{status:'failed',code:400,reason:'Synthetic permission rejection'}:undefined);
  await ensureThread(e,job(20),'20001',false);
  await expect(ensureInitialCardReady(e,job(21),'20001')).rejects.toThrow('initial_card_not_confirmed');
  reject=false;
  const message:TGMessage={message_id:22,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:10001},text:'/tj 20001'};
  await handleAdmin(e,job(22),{update_id:22,message});
  expect(requests.filter(r=>r.method==='sendMessage'&&r.params.message_thread_id===40)).toHaveLength(1);
  expect(requests.filter(r=>r.method==='pinChatMessage').map(r=>r.key)).toEqual([`${e.RECOVERY_EPOCH}:card:20001:1:initial:pin:701`,`${e.RECOVERY_EPOCH}:card:20001:1:initial:pin:701:retry:22`]);
  expect(await ensureInitialCardReady(e,job(23),'20001')).toBe(true);
 });

 it.each(['text','photo','album'] as const)('pins before first %s delivery, then preserves short-burst order without duplicate forwarding',async(kind)=>{
  const {e,requests}=runtime();
  let clock=100000;
  vi.spyOn(Date,'now').mockImplementation(()=>clock);
  const first=kind==='text'?incoming(30):incoming(30,{text:undefined,photo:[{file_id:'synthetic-1'}],...(kind==='album'?{media_group_id:'synthetic-init-album'}:{})});
  await accept(e,{update_id:30,message:first});
  if(kind==='album')await accept(e,{update_id:31,message:incoming(31,{text:undefined,photo:[{file_id:'synthetic-2'}],media_group_id:'synthetic-init-album'})});
  await accept(e,{update_id:32,message:incoming(32)});
  clock+=10000;
  const pending:Promise<unknown>[]=[],wake=conversation(e,pending);
  await wake();await wake();await Promise.all(pending);
  const methods=requests.map(r=>r.method);
  expect(methods.slice(0,4)).toEqual(['getChat','createForumTopic','sendMessage','pinChatMessage']);
  expect(methods.slice(4)).toEqual(kind==='album'?['sendMediaGroup','copyMessage']:['copyMessage','copyMessage']);
  expect(requests.filter(r=>r.method==='sendMessage')).toHaveLength(1);
  expect(requests.filter(r=>r.method==='copyMessage').map(r=>r.params.message_id)).toEqual(kind==='album'?[32]:[30,32]);
  expect((await all<{delivery:string}>(e,'SELECT delivery FROM messages ORDER BY id')).every(r=>r.delivery==='success')).toBe(true);
  expect(vi.mocked(fetch)).not.toHaveBeenCalled();
 });

 it('keeps a later private message behind a rate-limited initial pin and recovers without duplicate sends',async()=>{
  const {e,requests}=runtime();
  let clock=100000,defer=true;
  vi.spyOn(Date,'now').mockImplementation(()=>clock);
  const original=e.BOT;
  e.BOT={idFromName:(name:string)=>original.idFromName(name),get:(id:DurableObjectId)=>({fetch:async(url:string,init:RequestInit)=>{
   const {method}=JSON.parse(String(init.body));
   if(method==='pinChatMessage'&&defer){defer=false;return Response.json({due:clock+5000},{status:429});}
   return original.get(id).fetch(url,init);
  }})} as unknown as DurableObjectNamespace;
  for(const id of [35,36])await accept(e,{update_id:id,message:incoming(id)});
  const pending:Promise<unknown>[]=[],wake=conversation(e,pending);
  await wake();
  expect(requests.some(r=>r.method==='copyMessage')).toBe(false);
  expect(await all(e,"SELECT state FROM jobs WHERE user_key='20001' ORDER BY id")).toEqual([{state:'retry'},{state:'pending'}]);
  clock+=10000;
  await wake();await Promise.all(pending);
  expect(requests.filter(r=>r.method==='sendMessage')).toHaveLength(1);
  expect(requests.filter(r=>r.method==='pinChatMessage')).toHaveLength(1);
  expect(requests.filter(r=>r.method==='copyMessage').map(r=>r.params.message_id)).toEqual([35,36]);
 });

 it('retries rendering a current ban state if an older card update finishes after unblocking',async()=>{
  await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id,blocked) VALUES('20001',1,40,1,500,1)");
  await setSetting(base,'profile_checked:20001',String(Date.now()));
  let changed=false;
  const {e,requests}=runtime(async r=>{
   if(r.method==='editMessageText'&&!changed){changed=true;await run(base,"UPDATE users SET blocked=0 WHERE tgid='20001'");}
   return undefined;
  });
  await expect(refreshCardNow(e,job(40),'20001')).rejects.toThrow('retry_later');
  await refreshCardNow(e,job(40),'20001');
  const edits=requests.filter(r=>r.method==='editMessageText');
  expect(edits).toHaveLength(2);
  expect(edits[0].params.text).toContain('状态：已封禁');
  expect(edits[1].params.text).toContain('状态：正常');
  const markup=requests.findLast(r=>r.method==='editMessageReplyMarkup');
  expect(JSON.stringify(markup?.params.reply_markup)).toContain('"text":"封禁"');
 });

 it('issues fresh working controls after a block-unblock cycle during one retried card job',async()=>{
  await run(base,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,500)");
  await setSetting(base,'profile_checked:20001',String(Date.now()));
  const {e,requests}=runtime();
  const original=e.BOT;
  let pins=0;
  e.BOT={idFromName:(name:string)=>original.idFromName(name),get:(id:DurableObjectId)=>({fetch:async(url:string,init:RequestInit)=>{
   const {method}=JSON.parse(String(init.body));
   if(method==='pinChatMessage'&&++pins<=2)return Response.json({due:Date.now()+1000},{status:429});
   return original.get(id).fetch(url,init);
  }})} as unknown as DurableObjectNamespace;
  const action=(label:string)=>{
   const markup=requests.findLast(r=>r.method==='editMessageReplyMarkup')!.params.reply_markup as {inline_keyboard:{text:string;callback_data?:string}[][]};
   return markup.inline_keyboard.flat().find(button=>button.text===label)!.callback_data!.slice(2);
  };
  const click=async(id:number,token:string)=>handleAdmin(e,job(id),{update_id:id,callback_query:{id:`test-aba:${id}`,from:{id:10001},data:`a:${token}`,message:{message_id:500,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup'},from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},message_thread_id:40,is_topic_message:true}}});
  const sameJob=job(50);
  await expect(refreshCardNow(e,sameJob,'20001')).rejects.toThrow('retry_later');
  const oldNormal=action('封禁');
  await click(51,oldNormal);
  await expect(refreshCardNow(e,sameJob,'20001')).rejects.toThrow('retry_later');
  const oldBlocked=action('解除封禁');
  await click(52,oldBlocked);
  await refreshCardNow(e,sameJob,'20001');
  const currentNormal=action('封禁'),currentClear=action('清空此对话');
  expect(currentNormal).not.toBe(oldNormal);
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',currentNormal)).toEqual({used:0});
  expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',currentClear)).toEqual({used:0});
  expect(requests.filter(r=>r.method==='editMessageText').map(r=>String(r.params.text).match(/状态：([^\n]+)/)?.[1])).toEqual(['正常','已封禁','正常']);
  expect(await one(e,"SELECT blocked,moderation_version FROM users WHERE tgid='20001'")).toEqual({blocked:0,moderation_version:2});
  await click(53,currentNormal);
  expect(await one(e,"SELECT blocked,moderation_version FROM users WHERE tgid='20001'")).toEqual({blocked:1,moderation_version:3});
 });

 it('renews retired controls when a profile returns to an earlier signature during a retried card job',async()=>{
  await run(base,"INSERT INTO users(tgid,first_name,first_contact,thread_id,binding_version,card_id) VALUES('20001','Synthetic Alpha',1,40,1,500)");
  await setSetting(base,'profile_checked:20001',String(Date.now()));
  const {e,requests}=runtime();
  const original=e.BOT;
  let pins=0;
  e.BOT={idFromName:(name:string)=>original.idFromName(name),get:(id:DurableObjectId)=>({fetch:async(url:string,init:RequestInit)=>{
   const {method}=JSON.parse(String(init.body));
   if(method==='pinChatMessage'&&++pins<=2)return Response.json({due:Date.now()+1000},{status:429});
   return original.get(id).fetch(url,init);
  }})} as unknown as DurableObjectNamespace;
  const currentActions=()=>{
   const markup=requests.findLast(r=>r.method==='editMessageReplyMarkup')!.params.reply_markup as {inline_keyboard:{callback_data?:string}[][]};
   return markup.inline_keyboard.flat().filter(button=>button.callback_data).map(button=>button.callback_data!.slice(2));
  };
  const sameJob=job(60);
  await expect(refreshCardNow(e,sameJob,'20001')).rejects.toThrow('retry_later');
  const originalTokens=currentActions();
  await run(base,"UPDATE users SET first_name='Synthetic Beta' WHERE tgid='20001'");
  await expect(refreshCardNow(e,sameJob,'20001')).rejects.toThrow('retry_later');
  for(const token of originalTokens)expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',token)).toEqual({used:-1});
  await run(base,"UPDATE users SET first_name='Synthetic Alpha' WHERE tgid='20001'");
  await refreshCardNow(e,sameJob,'20001');
  const renewedTokens=currentActions();
  expect(renewedTokens.every(token=>!originalTokens.includes(token))).toBe(true);
  for(const token of renewedTokens)expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',token)).toEqual({used:0});
  expect(requests.filter(r=>r.method==='editMessageText').map(r=>String(r.params.text).split('\n')[0])).toEqual(['姓名：Synthetic Alpha','姓名：Synthetic Beta','姓名：Synthetic Alpha']);
  const edits=requests.filter(r=>r.method==='editMessageReplyMarkup');
  expect(new Set(edits.map(r=>r.key)).size).toBe(3);
 });
});
