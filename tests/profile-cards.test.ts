import {env} from 'cloudflare:test';
import {beforeEach,describe,expect,it} from 'vitest';
import {handleAdmin,refreshCardNow} from '../src/admin';
import {ensureInitialCardReady} from '../src/admin';
import {one,run,setSetting,user} from '../src/db';
import type {Env,Job,Params,TGUpdate} from '../src/types';
import type {ProfileMarkup} from '../src/profile-links';

const api={requests:[] as {key:string;method:string;params:Params}[],sequence:800,privacy:false,unknown:false,helpStatus:null as null|'failed'|'unknown',onPrivacyReject:null as null|(()=>Promise<void>)};
const bot={idFromName:(name:string)=>name,get:()=>({fetch:async(_url:string,init:RequestInit)=>{
 const {key,method,params}=JSON.parse(String(init.body)) as {key:string;method:string;params:Params};
 api.requests.push({key,method,params});
 if(api.helpStatus&&key.endsWith(':profile-help'))return Response.json({status:api.helpStatus,code:400,reason:'profile help unavailable'});
 if(api.unknown&&method==='sendMessage')return Response.json({status:'unknown',reason:'response lost'});
 if(api.privacy&&JSON.stringify(params.reply_markup ?? {}).includes('tg://user?id=')) {
  const hook=api.onPrivacyReject;api.onPrivacyReject=null;if(hook)await hook();
  return Response.json({status:'failed',code:400,reason:'Bad Request: BUTTON_USER_PRIVACY_RESTRICTED'});
 }
 let result:unknown=true;
 if(method==='getUserProfilePhotos')result={photos:[]};
 if(method==='sendMessage'||method==='sendPhoto')result={message_id:++api.sequence};
 return Response.json({status:'success',result});
}})};
const e={...env,BOT:bot} as unknown as Env;
const job=(id:number):Job=>({id,dedupe:`profile-test:${id}`,user_key:'20001',kind:'card',payload:'{}',state:'running',due_at:0,attempts:0,epoch:e.RECOVERY_EPOCH,created_at:1});
async function registered(card:number|null=500) {
 await run(e,"INSERT INTO users(tgid,first_contact,thread_id,binding_version,card_id) VALUES('20001',1,40,1,?)",card);
}
const markupOf=(params:Params)=>params.reply_markup as ProfileMarkup;
function latestMarkup() {return markupOf(api.requests.findLast(request=>request.method==='editMessageReplyMarkup')!.params);}
function callback(id:number,data:string,messageId=500,actor=10001):TGUpdate {
 return {update_id:id,callback_query:{id:`profile-callback:${id}`,from:{id:actor},data,message:{message_id:messageId,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup',is_forum:true},from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true},message_thread_id:40,is_topic_message:true}}};
}
beforeEach(()=>{api.requests.length=0;api.sequence=800;api.privacy=false;api.unknown=false;api.helpStatus=null;api.onPrivacyReject=null;});

describe('profile cards under user privacy restrictions',()=>{
 it('recovers the previous release privacy fallback without sending a duplicate card',async()=>{
  await registered(null);await setSetting(e,'card_initial:20001:1','pending:40');await setSetting(e,'card_no_id_links:20001','1');
  const base=`${e.RECOVERY_EPOCH}:card:20001:1:create`;
  await run(e,"INSERT INTO operations(op_key,method,phase,reason,code,created_at,updated_at) VALUES(?,'sendMessage','failed','BUTTON_USER_PRIVACY_RESTRICTED',400,1,1)",base);
  await run(e,"INSERT INTO operations(op_key,method,phase,result,created_at,updated_at) VALUES(?,'sendMessage','success',?,1,1)",`${base}:without-private-links`,JSON.stringify({message_id:901}));
  expect(await ensureInitialCardReady(e,job(300),'20001')).toBe(true);
  await refreshCardNow(e,job(300),'20001',false,true);
  expect(api.requests.filter(request=>request.method==='sendMessage')).toEqual([]);
  expect((await user(e,'20001'))!.card_id).toBe(901);
  const edit=api.requests.findLast(request=>request.method==='editMessageText')!;
  expect(markupOf(edit.params).inline_keyboard[0].map(button=>button.text)).toEqual(['通用资料','苹果资料','安卓资料']);
  expect(markupOf(edit.params).inline_keyboard[0].every(button=>!!button.callback_data)).toBe(true);
 });
 it('keeps all three entrances and pins the new text card after a confirmed privacy rejection',async()=>{
  await registered(null);await setSetting(e,'card_initial:20001:1','pending:40');api.privacy=true;
  expect(await ensureInitialCardReady(e,job(301),'20001')).toBe(true);
  const sends=api.requests.filter(request=>request.method==='sendMessage');
  expect(sends).toHaveLength(2);
  const buttons=markupOf(sends[1].params).inline_keyboard[0];
  expect(buttons.map(button=>button.text)).toEqual(['通用资料','苹果资料','安卓资料']);
  expect(buttons.every(button=>button.callback_data?.startsWith('a:'))).toBe(true);
  expect(JSON.stringify(sends[1].params.reply_markup)).not.toContain('tg://user?');
  expect(JSON.stringify(sends[1].params.reply_markup)).not.toContain('tg://openmessage?');
  expect(api.requests.findLast(request=>request.method==='pinChatMessage')!.params.message_id).toBe((await user(e,'20001'))!.card_id);
  expect(api.requests.some(request=>request.params.chat_id==='20001')).toBe(false);
 });
 it('repairs cached legacy cards and explains each entrance only in the administrator topic',async()=>{
  await registered();await setSetting(e,'card_no_id_links:20001','1');
  await refreshCardNow(e,job(302),'20001');
  const buttons=latestMarkup().inline_keyboard[0];
  expect(buttons.map(button=>button.text)).toEqual(['通用资料','苹果资料','安卓资料']);
  expect((await user(e,'20001'))!.card_id).toBe(500);
  for(const [index,button] of buttons.entries()) {
   api.requests.length=0;
   await handleAdmin(e,job(303+index),callback(303+index,button.callback_data!));
   const help=api.requests.find(request=>request.method==='sendMessage')!;
   expect(help.params.chat_id).toBe(e.ADMIN_CHAT_ID);expect(help.params.message_thread_id).toBe(40);
   expect(String(help.params.text)).toContain('20001');expect(String(help.params.text)).toContain('隐私');
   expect(markupOf(help.params).inline_keyboard.flat()).toContainEqual({text:'复制 TGID',copy_text:{text:'20001'}});
  }
 });
 it('rejects an unauthorized operator and stale card for profile help',async()=>{
  await registered();await setSetting(e,'card_no_id_links:20001','1');await refreshCardNow(e,job(306),'20001');
  const button=latestMarkup().inline_keyboard[0][0];api.requests.length=0;
  await handleAdmin(e,job(307),callback(307,button.callback_data!,500,99999));
  expect(api.requests).toEqual([]);
  await handleAdmin(e,job(308),callback(308,button.callback_data!,499));
  expect(api.requests.some(request=>request.method==='sendMessage')).toBe(false);
  expect(String(api.requests.find(request=>request.method==='answerCallbackQuery')!.params.text)).toContain('旧资料卡');
 });
 it('restores username links despite an older cached ID restriction',async()=>{
  await registered();await setSetting(e,'card_no_id_links:20001','1');
  await run(e,"UPDATE users SET username='alice_test' WHERE tgid='20001'");await refreshCardNow(e,job(309),'20001');
  expect(latestMarkup().inline_keyboard[0].map(button=>button.url)).toEqual(['https://t.me/alice_test?profile','tg://resolve?domain=alice_test&profile','tg://resolve?domain=alice_test&profile']);
 });
 it('does not convert an unknown card send into another send or pass the initial gate',async()=>{
  await registered(null);await setSetting(e,'card_initial:20001:1','pending:40');api.unknown=true;
  await expect(ensureInitialCardReady(e,job(310),'20001')).rejects.toThrow('initial_card_not_confirmed');
  await expect(ensureInitialCardReady(e,job(311),'20001')).rejects.toThrow('initial_card_not_confirmed');
  expect(api.requests.filter(request=>request.method==='sendMessage')).toHaveLength(1);
  expect(api.requests.some(request=>request.method==='pinChatMessage')).toBe(false);
  expect((await user(e,'20001'))!.card_id).toBeNull();
 });
 it('keeps the rendered fallback tokens active when another render prepares callbacks concurrently',async()=>{
  await registered();api.privacy=true;
  api.onPrivacyReject=async()=>{
   for(const entry of ['general','ios','android'])await run(e,"INSERT INTO callback_actions(token,kind,user_id,actor_id,thread_id,version,payload,expires_at) VALUES(?,?,'20001','*',40,1,?,?)",crypto.randomUUID().replace(/-/g,''),`card_profile_${entry}`,JSON.stringify({instance:e.INSTANCE_ID,context:'concurrent-render'}),Number.MAX_SAFE_INTEGER);
  };
  await refreshCardNow(e,job(312),'20001',false,true);
  const edit=api.requests.findLast(request=>request.method==='editMessageText')!;
  for(const button of markupOf(edit.params).inline_keyboard[0]) {
   expect(button.callback_data).toBeDefined();
   expect(await one(e,'SELECT used FROM callback_actions WHERE token=?',button.callback_data!.slice(2))).toEqual({used:0});
  }
 });
 it.each(['failed','unknown'] as const)('reports %s profile help truthfully and does not repeat a recovered send',async status=>{
  await registered();await setSetting(e,'card_no_id_links:20001','1');await refreshCardNow(e,job(313),'20001');
  const button=latestMarkup().inline_keyboard[0][2];api.requests.length=0;api.helpStatus=status;
  const update=callback(314,button.callback_data!);
  await handleAdmin(e,job(314),update);await handleAdmin(e,job(314),update);
  expect(api.requests.filter(request=>request.method==='sendMessage')).toHaveLength(1);
  expect(String(api.requests.find(request=>request.method==='answerCallbackQuery')!.params.text)).toContain(status==='failed'?'失败':'未知');
  expect(String(api.requests.find(request=>request.method==='answerCallbackQuery')!.params.text)).not.toContain('已记录');
 });
});
