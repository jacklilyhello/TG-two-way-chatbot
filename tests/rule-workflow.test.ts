import {env} from 'cloudflare:test';
import {describe,expect,it} from 'vitest';
import type {Env,Job,Params,TGMessage,TGUpdate} from '../src/types';
import {all,now,one,run} from '../src/db';
import {RetryLater} from '../src/operations';
import {addBlockRule,changeBlockRule,handleRuleAction,handleRuleCommand,handleRuleInput,rulesPanel,type RuleAction} from '../src/block-rules';
import {captureRuleInput,closeRuleSession,ruleSession,type RuleInputAdmission} from '../src/rule-workflow';
import {handleAdmin} from '../src/admin';
import {accept} from '../src/ingress';

const base=env as unknown as Env;
interface Request {key:string;method:string;params:Params}
function client(onRequest?:(request:Request)=>Response) {
 const requests:Request[]=[];let sequence=900;
 const e={...base,ADMIN_IDS:'10001,10002',JOBS:{send:async()=>{}},CONVERSATIONS:{idFromName:()=>'',get:()=>({fetch:async()=>new Response('ok')})},BOT:{idFromName:()=>'',get:()=>({fetch:async(_url:string,init:RequestInit)=>{
  const request=JSON.parse(String(init.body)) as Request;requests.push(request);return onRequest?.(request)??Response.json({status:'success',result:{message_id:++sequence}});
 }})}} as unknown as Env;
 return {e,requests};
}
const job=(id:number,admission?:RuleInputAdmission|null):Job=>({id,dedupe:`rule:${id}`,user_key:'admin',kind:'update',payload:JSON.stringify(admission===undefined?{}:{ruleInputAdmission:admission}),state:'running',due_at:0,attempts:0,epoch:base.RECOVERY_EPOCH,created_at:1});
const message=(e:Env,id:number,text:string,actor=10001):TGMessage=>({message_id:id,date:1,chat:{id:Number(e.ADMIN_CHAT_ID),type:'supergroup',is_forum:true},from:{id:actor},text});
async function latest(e:Env,kind:string,actor=10001):Promise<RuleAction> {
 return (await one<RuleAction>(e,'SELECT * FROM callback_actions WHERE kind=? AND actor_id=? ORDER BY rowid DESC LIMIT 1',kind,String(actor)))!;
}
async function click(e:Env,id:number,kind:string,actor=10001) {
 const action=await latest(e,kind,actor);expect(action).not.toBeNull();
 return handleRuleAction(e,job(id),action,JSON.parse(action.payload),id+1,String(actor));
}
async function input(e:Env,id:number,text:string,actor=10001) {
 const m=message(e,id,text,actor),admission=await captureRuleInput(e,m);
 return handleRuleInput(e,job(id,admission),{update_id:id,message:m});
}
async function begin(e:Env,id=1,kind:'keyword'|'regex'='keyword',actor=10001) {
 await rulesPanel(e,job(id),String(actor));await click(e,id+1,'rule_add',actor);
 const actions=await all<RuleAction>(e,"SELECT * FROM callback_actions WHERE kind='rule_type' AND actor_id=? ORDER BY rowid DESC",String(actor));
 const action=actions.find(action=>JSON.parse(action.payload).kind===kind)!;
 await handleRuleAction(e,job(id+2),action,JSON.parse(action.payload),id+3,String(actor));
}

describe('interactive rule management',()=>{
 it('lists only enabled effective rules, paginates five at a time and exposes complete button navigation',async()=>{
  const {e,requests}=client();
  for(let i=0;i<8;i++)await addBlockRule(e,job(100+i),'10001','keyword',`visible-${i}`);
  await changeBlockRule(e,job(110),'10001',1,'off');
  await run(e,"UPDATE block_rules SET pattern=' ' WHERE id=2");
  await rulesPanel(e,job(111),'10001');
  const first=String(requests.at(-1)!.params.text),keyboard=JSON.stringify(requests.at(-1)!.params.reply_markup);
  expect(first).toContain('当前有效 6 条');expect(first).not.toContain('visible-0');expect(first).toContain('#3');expect(first).not.toContain('#8');
  expect(keyboard).toContain('添加规则');expect(keyboard).toContain('删除规则');expect(keyboard).toContain('返回主面板');
  await click(e,112,'rule_page');expect(String(requests.at(-1)!.params.text)).toContain('#8');
 });
 it('selects type, previews exact content, saves only on confirmation and ends input collection',async()=>{
  const {e,requests}=client();await begin(e);
  expect(await input(e,4,'same  words\nnext')).toBe(true);
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(0);
  expect(String(requests.at(-1)!.params.text)).toContain('same  words\nnext');
  const confirmation=await latest(e,'rule_add_confirm');
  await click(e,5,'rule_add_confirm');
  expect(await one(e,'SELECT pattern,enabled FROM block_rules')).toEqual({pattern:'same  words\nnext',enabled:1});
  expect((await ruleSession(e,'10001'))!.stage).toBe('done');
  expect(await input(e,6,'ordinary message')).toBe(false);
  // Even an incorrectly duplicated dispatcher call reuses the token mutation.
  await handleRuleAction(e,job(7),confirmation,JSON.parse(confirmation.payload),8,'10001');
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(1);
  expect(await all(e,"SELECT id FROM audit_logs WHERE event='block_rule_created'")).toHaveLength(1);
  expect(JSON.stringify(await all(e,'SELECT detail FROM audit_logs'))).not.toContain('same  words');
 });
 it('gives precise invalid-regex feedback and accepts replacement without a management command',async()=>{
  const {e,requests}=client();await begin(e,10,'regex');
  await input(e,13,'(?=ad)ad');expect((await ruleSession(e,'10001'))!.stage).toBe('pattern');
  expect(String(requests.at(-1)!.params.text)).toContain('不支持 (?...)');
  await input(e,14,'[z-a]');expect(String(requests.at(-1)!.params.text)).toContain('起点不能大于终点');
  await input(e,15,'.*');expect(String(requests.at(-1)!.params.text)).toContain('不能匹配空字符串');
  await input(e,16,'(优惠|推广)\\s*[0-9]+');await click(e,17,'rule_add_confirm');
  expect(await one(e,'SELECT kind,pattern FROM block_rules')).toEqual({kind:'regex',pattern:'(优惠|推广)\\s*[0-9]+'});
 });
 it('asks for a rule number, recovers input errors and confirms a soft deletion',async()=>{
  const {e,requests}=client();await addBlockRule(e,job(20),'10001','keyword','delete-me');await rulesPanel(e,job(21),'10001');await click(e,22,'rule_delete');
  await input(e,23,'wrong');expect(String(requests.at(-1)!.params.text)).toContain('编号格式有误');
  await input(e,24,'999');expect(String(requests.at(-1)!.params.text)).toContain('找不到这个编号');
  await input(e,25,'#1');expect(String(requests.at(-1)!.params.text)).toContain('delete-me');
  expect(await one(e,'SELECT deleted FROM block_rules')).toEqual({deleted:0});
  await click(e,26,'rule_delete_confirm');expect(await one(e,'SELECT deleted,enabled FROM block_rules')).toEqual({deleted:1,enabled:0});
  expect(String(requests.at(-1)!.params.text)).toContain('当前没有已启用且有效的规则');
  expect(await input(e,27,'later')).toBe(false);
 });
 it('revalidates the selected rule version at deletion confirmation',async()=>{
  const {e}=client();await addBlockRule(e,job(30),'10001','keyword','preserved');await rulesPanel(e,job(31),'10001');await click(e,32,'rule_delete');await input(e,33,'1');
  await changeBlockRule(e,job(34),'10002',1,'off');expect(await click(e,35,'rule_delete_confirm')).toContain('规则已变化');
  expect(await one(e,'SELECT deleted,enabled FROM block_rules')).toEqual({deleted:0,enabled:0});
 });
 it('isolates each administrator and rejects another actor using a draft button',async()=>{
  const {e}=client();await begin(e,40,'keyword',10001);await begin(e,50,'regex',10002);
  await input(e,43,'first-admin',10001);await input(e,53,'^second-admin$',10002);
  const first=await latest(e,'rule_add_confirm',10001);
  expect(await handleRuleAction(e,job(54),first,JSON.parse(first.payload),55,'10002')).toBe('无权操作。');
  await click(e,55,'rule_add_confirm',10001);expect((await ruleSession(e,'10002'))!.stage).toBe('confirm_add');
  await click(e,56,'rule_add_confirm',10002);expect((await all<{pattern:string}>(e,'SELECT pattern FROM block_rules ORDER BY id')).map(rule=>rule.pattern)).toEqual(['first-admin','^second-admin$']);
 });
 it('scopes drafts to General, the instance, the recovery epoch and an authorized human',async()=>{
  const {e}=client();await begin(e,60);
  const normal=message(e,63,'ignored');
  for(const m of [{...normal,message_thread_id:40,is_topic_message:true},{...normal,from:{id:20001}},{...normal,from:{id:10001,is_bot:true}},{...normal,sender_chat:{id:Number(e.ADMIN_CHAT_ID)}},{...normal,forward_origin:{type:'user' as const,sender_user:{id:10002},date:1}}])expect(await captureRuleInput(e,m)).toBeNull();
  expect(await captureRuleInput({...e,INSTANCE_ID:'other-instance'},normal)).toBeNull();
  expect(await captureRuleInput({...e,RECOVERY_EPOCH:'other-epoch'},normal)).toBeNull();
  const admission=(await captureRuleInput(e,normal))!;
  expect(await handleRuleInput({...e,INSTANCE_ID:'other-instance'},job(63,admission),{update_id:63,message:normal})).toBe(true);
  expect((await ruleSession(e,'10001'))!.stage).toBe('pattern');
 });
 it('supports returning to earlier steps and cancellation invalidates old confirmations',async()=>{
  const {e}=client();await begin(e,70);await input(e,73,'not-saved');
  const confirm=await latest(e,'rule_add_confirm');await click(e,74,'rule_back');
  expect((await ruleSession(e,'10001'))!.stage).toBe('pattern');await input(e,75,'changed');
  await click(e,76,'rule_cancel');expect((await ruleSession(e,'10001'))!.stage).toBe('done');
  expect(await handleRuleAction(e,job(77),confirm,JSON.parse(confirm.payload),78,'10001')).toContain('流程已变化或结束');
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(0);
 });
 it('intercepts late admitted input after cancellation but does not capture messages from before a new flow',async()=>{
  const {e,requests}=client();await begin(e,80);
  const m=message(e,83,'late management text'),snapshot=(await captureRuleInput(e,m))!;
  await closeRuleSession(e,'10001');expect(await handleRuleInput(e,job(83,snapshot),{update_id:83,message:m})).toBe(true);
  expect(String(requests.at(-1)!.params.text)).toContain('已结束或过期');
  await begin(e,84);
  expect(await handleRuleInput(e,job(87,null),{update_id:87,message:message(e,87,'admitted before flow')})).toBe(false);
  expect(await handleRuleInput(e,job(88,snapshot),{update_id:88,message:message(e,88,'belongs to old flow')})).toBe(true);
  expect((await ruleSession(e,'10001'))!.stage).toBe('pattern');
 });
 it('rejects a second queued input with the old revision and never edits from message edits',async()=>{
  const {e,requests}=client();await begin(e,90);
  const m=message(e,93,'first'),snapshot=(await captureRuleInput(e,m))!;
  await handleRuleInput(e,job(93,snapshot),{update_id:93,message:m});
  await handleRuleInput(e,job(94,snapshot),{update_id:94,message:message(e,94,'second')});
  expect(String(requests.at(-1)!.params.text)).toContain('旧输入未保存');
  expect(JSON.parse((await ruleSession(e,'10001'))!.payload).pattern).toBe('first');
  await click(e,95,'rule_back');const edit=message(e,96,'edited');
  await handleRuleInput(e,job(96,(await captureRuleInput(e,edit))!),{update_id:96,edited_message:edit});
  expect(String(requests.at(-1)!.params.text)).toContain('编辑旧消息不会修改规则');
  expect((await ruleSession(e,'10001'))!.stage).toBe('pattern');
 });
 it('expires drafts without saving and can leave by ordinary cancellation or /mb',async()=>{
  const {e}=client();await begin(e,100);const m=message(e,103,'expired'),snapshot=(await captureRuleInput(e,m))!;
  await run(e,'UPDATE rule_sessions SET expires_at=?',now()-1);
  expect(await handleRuleInput(e,job(103,snapshot),{update_id:103,message:m})).toBe(true);expect((await ruleSession(e,'10001'))!.stage).toBe('done');
  await begin(e,104);await input(e,107,'取消');expect((await ruleSession(e,'10001'))!.stage).toBe('done');
  await begin(e,108);expect(await input(e,111,'/mb')).toBe(false);expect((await ruleSession(e,'10001'))!.stage).toBe('done');
 });
 it('recovers a prompt rate limit after the draft transition without double transitions',async()=>{
  let limited=true;const {e}=client(request=>{
   if(request.key.includes('job:123:rules:draft:3')&&limited){limited=false;return Response.json({due:now()+1},{status:429});}
   return Response.json({status:'success',result:{message_id:901}});
  });await begin(e,120);const m=message(e,123,'recoverable'),snapshot=(await captureRuleInput(e,m))!,j=job(123,snapshot),update={update_id:123,message:m};
  await expect(handleRuleInput(e,j,update)).rejects.toBeInstanceOf(RetryLater);
  expect((await ruleSession(e,'10001'))!.revision).toBe(3);await handleRuleInput(e,j,update);
  expect((await ruleSession(e,'10001'))!.revision).toBe(3);await click(e,124,'rule_add_confirm');expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(1);
 });
 it('recovers a rate limited save receipt from its committed mutation and never duplicates the rule',async()=>{
  let limited=true;const {e}=client(request=>{
   if(request.key.endsWith('job:134:rules:add')&&limited){limited=false;return Response.json({due:now()+1},{status:429});}
   return Response.json({status:'success',result:{message_id:901}});
  });await begin(e,130);await input(e,133,'committed');const confirm=await latest(e,'rule_add_confirm'),payload=JSON.parse(confirm.payload);
  await expect(handleRuleAction(e,job(134),confirm,payload,135,'10001')).rejects.toBeInstanceOf(RetryLater);
  expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(1);
  await handleRuleAction(e,job(134),confirm,payload,135,'10001');expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(1);
  expect(await all(e,"SELECT id FROM audit_logs WHERE event='block_rule_created'")).toHaveLength(1);
 });
 it('offers the add wizard through the compatible short command and returns to the main panel callback',async()=>{
  const {e}=client();await handleRuleCommand(e,job(140),'10001','add');expect((await ruleSession(e,'10001'))!.stage).toBe('type');
  let main=0;const action=await latest(e,'rule_main');await handleRuleAction(e,job(141),action,JSON.parse(action.payload),142,'10001',undefined,async()=>{main++;});
  expect(main).toBe(1);expect((await ruleSession(e,'10001'))!.stage).toBe('done');
 });
 it('accepts slash-prefixed keyword content and reports slash-delimited regex errors without leaving the step',async()=>{
  const {e,requests}=client();await begin(e,150);await input(e,153,'/promo');
  expect(JSON.parse((await ruleSession(e,'10001'))!.payload).pattern).toBe('/promo');await click(e,154,'rule_add_confirm');
  await begin(e,155,'regex');await input(e,158,'/advert/i');
  expect((await ruleSession(e,'10001'))!.stage).toBe('pattern');expect(String(requests.at(-1)!.params.text)).toContain('不使用 / 分隔符或 flags');
  await input(e,159,'advert');expect((await ruleSession(e,'10001'))!.stage).toBe('confirm_add');
 });
 it('uses the durable ingress snapshot in the real dispatcher and never writes workflow input to business messages',async()=>{
  const {e}=client();await begin(e,160);const m=message(e,163,'dispatcher-rule'),update:TGUpdate={update_id:163,message:m};
  expect(await accept(e,update)).toBe('admin:10001');const admitted=(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:163'"))!;
  expect(JSON.parse(admitted.payload).ruleInputAdmission).toMatchObject({instance:e.INSTANCE_ID,actor:'10001',stage:'pattern'});
  expect(await handleAdmin(e,admitted,update)).toBe(true);expect(JSON.parse((await ruleSession(e,'10001'))!.payload).pattern).toBe('dispatcher-rule');
  expect(await all(e,'SELECT id FROM messages')).toHaveLength(0);
 });
 it.each(['rule_cancel','rule_main'])('recovers %s after closing its draft before a rate-limited return page',async kind=>{
  let limited=true;const {e,requests}=client(request=>{
   if(request.key.includes('job:174:')&&request.method==='sendMessage'&&limited){limited=false;return Response.json({due:now()+1},{status:429});}
   return Response.json({status:'success',result:{message_id:901}});
  });await begin(e,170);
  const action=await latest(e,kind),callback:TGUpdate={update_id:174,callback_query:{id:'return-174',from:{id:10001},data:`a:${action.token}`,message:{...message(e,JSON.parse(action.payload).messageId,''),from:{id:Number(e.EXPECTED_BOT_ID),is_bot:true}}}};
  await expect(handleAdmin(e,job(174),callback)).rejects.toBeInstanceOf(RetryLater);
  expect(await ruleSession(e,'10001')).toMatchObject({stage:'done',transition_key:`action:${action.token}`});
  await handleAdmin(e,job(174),callback);
  expect(String(requests.at(-1)!.params.text)).toContain(kind==='rule_cancel'?'已取消':'已返回主面板');
  const returnSends=requests.filter(request=>request.key.includes('job:174:')&&request.method==='sendMessage');expect(returnSends).toHaveLength(2);
  // A new callback update has a different stamp and cannot borrow this UI replay.
  await handleAdmin(e,job(175),{...callback,update_id:175,callback_query:{...callback.callback_query!,id:'return-175'}});
  expect(String(requests.at(-1)!.params.text)).toContain('按钮已失效');
  expect(await ruleSession(e,'10001')).toMatchObject({stage:'done'});expect(await all(e,'SELECT id FROM block_rules')).toHaveLength(0);
 });
 it.each(['取消','返回'])('recovers plain %s from its original admitted job without treating later input as a replay',async text=>{
  let limited=true;const {e,requests}=client(request=>{
   if(request.key.includes('job:184:rules:0')&&limited){limited=false;return Response.json({due:now()+1},{status:429});}
   return Response.json({status:'success',result:{message_id:901}});
  });await begin(e,180);
  const m=message(e,184,text),admission=(await captureRuleInput(e,m))!,j=job(184,admission),update={update_id:184,message:m};
  await expect(handleRuleInput(e,j,update)).rejects.toBeInstanceOf(RetryLater);
  expect(await ruleSession(e,'10001')).toMatchObject({stage:'done',transition_key:'input:184'});
  expect(await handleRuleInput(e,j,update)).toBe(true);
  expect(requests.filter(request=>request.key.includes('job:184:rules:0'))).toHaveLength(2);
  expect(await handleRuleInput(e,job(185,admission),{update_id:185,message:message(e,185,text)})).toBe(true);
  expect(String(requests.at(-1)!.params.text)).toContain('已结束或过期');
  expect(await input(e,186,'ordinary message')).toBe(false);
 });
 it('keeps the short add-command draft open when its initial prompt is rate limited',async()=>{
  let limited=true,targetJob=0;const {e,requests}=client(request=>{
   if(request.key.includes(`job:${targetJob}:rules:draft:1`)&&limited){limited=false;return Response.json({due:now()+1},{status:429});}
   return Response.json({status:'success',result:{message_id:901}});
  });const m=message(e,190,'/mb rules add'),update={update_id:190,message:m};await accept(e,update);const j=(await one<Job>(e,"SELECT * FROM jobs WHERE dedupe='u:190'"))!;targetJob=j.id;
  await expect(handleAdmin(e,j,update)).rejects.toBeInstanceOf(RetryLater);
  expect(await ruleSession(e,'10001')).toMatchObject({stage:'type',revision:1});
  await handleAdmin(e,j,update);expect(await ruleSession(e,'10001')).toMatchObject({stage:'type',revision:1});
  expect(requests.filter(request=>request.key.includes(`job:${targetJob}:rules:draft:1`))).toHaveLength(2);
  expect(String(requests.at(-1)!.params.text)).toContain('请选择规则类型');
 });
});
