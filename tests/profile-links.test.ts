import {describe,expect,it} from 'vitest';
import {profileButtons,profileFallbackDetails,profileLinks,withProfileFallback} from '../src/profile-links';

const callbacks={general:'a:general',ios:'a:ios',android:'a:android'};
describe('profile entrances and privacy fallback',()=>{
 it('keeps three ID-based entrances without requiring a username or avatar',()=>{
  const u={tgid:'20001',username:null};
  expect(profileLinks(u)).toEqual(['tg://user?id=20001','tg://user?id=20001','tg://openmessage?user_id=20001']);
  expect(profileButtons(u).map(button=>button.text)).toEqual(['通用资料','苹果资料','安卓资料']);
 });
 it('uses documented public profile links when a username is available',()=>{
  expect(profileLinks({tgid:'20001',username:'alice_test'})).toEqual(['https://t.me/alice_test?profile','tg://resolve?domain=alice_test&profile','tg://resolve?domain=alice_test&profile']);
  expect(profileLinks({tgid:'20001',username:'invalid&profile=x'})).toEqual(profileLinks({tgid:'20001',username:null}));
 });
 it('rejects invalid IDs instead of inserting arbitrary URL parameters',()=>{
  for(const tgid of ['0','-20001','20001&x=1','4503599627370496']) expect(()=>profileLinks({tgid,username:null})).toThrow('invalid_profile_user_id');
 });
 it('replaces all three ID links and the duplicate home entrance with callbacks',()=>{
  const markup={inline_keyboard:[profileButtons({tgid:'20001',username:null}),[{text:'封禁',callback_data:'a:block'},{text:'直达对方主页',url:'tg://user?id=20001'}],[{text:'清空此对话',callback_data:'a:clear'}]]};
  const fallback=withProfileFallback(markup,callbacks);
  expect(fallback.inline_keyboard[0]).toEqual([{text:'通用资料',callback_data:'a:general'},{text:'苹果资料',callback_data:'a:ios'},{text:'安卓资料',callback_data:'a:android'}]);
  expect(fallback.inline_keyboard[1]).toEqual([{text:'封禁',callback_data:'a:block'},{text:'直达对方主页',callback_data:'a:general'}]);
  expect(fallback.inline_keyboard[2]).toEqual(markup.inline_keyboard[2]);
  expect(markup.inline_keyboard[0][0].url).toBe('tg://user?id=20001');
 });
 it('retains restored public username links even if the ID restriction is cached',()=>{
  const markup={inline_keyboard:[profileButtons({tgid:'20001',username:'alice_test'})]};
  expect(withProfileFallback(markup,callbacks)).toEqual(markup);
 });
 it('offers copyable TGID and honest client limits without retrying restricted mentions',()=>{
  for(const entry of ['general','ios','android'] as const) {
   const help=profileFallbackDetails({tgid:'20001',username:null},entry);
   expect(help.text).toContain('不能绕过隐私设置');
   expect(help.reply_markup.inline_keyboard.flat()).toContainEqual({text:'复制 TGID',copy_text:{text:'20001'}});
   expect(JSON.stringify(help.reply_markup)).not.toContain('tg://user?');
   expect(help.reply_markup.inline_keyboard.flat().some(button=>button.url)).toBe(false);
   expect(help.text.includes('tg://openmessage?user_id=20001')).toBe(entry==='android');
  }
 });
 it('recovers a public profile route in help after the username changes',()=>{
  const help=profileFallbackDetails({tgid:'20001',username:'alice_test'},'ios');
  expect(help.reply_markup.inline_keyboard[0]).toEqual([{text:'打开公开资料',url:'tg://resolve?domain=alice_test&profile'}]);
 });
});
