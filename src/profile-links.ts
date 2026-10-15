import type {UserRow} from './types';

type ProfileUser = Pick<UserRow,'tgid'|'username'>;
export type ProfileEntry = 'general'|'ios'|'android';
export type ProfileCallbacks = Record<ProfileEntry,string>;
export type ProfileButton = {text:string;url?:string;callback_data?:string;copy_text?:{text:string}};
export type ProfileMarkup = {inline_keyboard:ProfileButton[][]};
export const PROFILE_ENTRIES: readonly {entry:ProfileEntry;label:string}[] = [
 {entry:'general',label:'通用资料'},
 {entry:'ios',label:'苹果资料'},
 {entry:'android',label:'安卓资料'}
];

const publicUsername = (u:ProfileUser) => u.username && /^[A-Za-z0-9_]{5,32}$/.test(u.username) ? u.username : null;
function checkedId(u:ProfileUser) {
 if (!/^[1-9]\d{0,15}$/.test(u.tgid) || Number(u.tgid)>4503599627370495) throw new Error('invalid_profile_user_id');
 return u.tgid;
}

/** The Bot API converts tg://user into a native user-profile button, subject
 * to privacy. It is not a universal standalone deep link. Android also has an
 * openmessage handler; that compatibility route can open a chat, not a profile. */
export function profileLinks(u:ProfileUser):[string,string,string] {
 const id=checkedId(u),username=publicUsername(u);
 if (username) return [`https://t.me/${username}?profile`,`tg://resolve?domain=${username}&profile`,`tg://resolve?domain=${username}&profile`];
 return [`tg://user?id=${id}`,`tg://user?id=${id}`,`tg://openmessage?user_id=${id}`];
}

export function profileButtons(u:ProfileUser):ProfileButton[] {
 const links=profileLinks(u);
 return PROFILE_ENTRIES.map(({label},index)=>({text:label,url:links[index]}));
}

/** Replace restricted ID routes with authenticated callback help, preserving
 * all three entrances and the remaining card controls. Public username links
 * are retained when an account later acquires a username. */
export function withProfileFallback(markup:ProfileMarkup,callbacks:ProfileCallbacks):ProfileMarkup {
 return {inline_keyboard:markup.inline_keyboard.map(row=>row.map(button=>{
  if (!button.url || !/^tg:\/\/(?:user\?id=|openmessage\?user_id=)/.test(button.url)) return {...button};
  const entry=PROFILE_ENTRIES.find(item=>item.label===button.text)?.entry ?? 'general';
  return {text:button.text,callback_data:callbacks[entry]};
 }))};
}

/** Plain text deliberately avoids another privacy-dependent ID mention. */
export function profileFallbackDetails(u:ProfileUser,entry:ProfileEntry):{text:string;reply_markup:ProfileMarkup} {
 const id=checkedId(u),username=publicUsername(u);
 const label=PROFILE_ENTRIES.find(item=>item.entry===entry)!.label;
 const text=[
  `${label}（兼容说明）`,
  `TGID：${id}`,
  username ? `公开用户名：@${username}` : '该用户未设置公开用户名。',
  'Telegram 已限制数字 ID 资料链接，或当前客户端无法打开该入口。数字 TGID 不能绕过隐私设置。',
  entry==='android' ? `安卓兼容链接（复制后尝试）：tg://openmessage?user_id=${id}\n可能打开已有对话；仍受客户端是否已认识该用户、版本及隐私限制。` : '可复制 TGID 核对目标，并从已有私聊或共同群中的用户资料进入。',
  '无法打开时仍可在当前话题继续双向聊天。'
 ].join('\n');
 const buttons:ProfileButton[][]=[[{text:'复制 TGID',copy_text:{text:id}}]];
 if (username) buttons.unshift([{text:'打开公开资料',url:profileLinks(u)[entry==='general'?0:entry==='ios'?1:2]}]);
 return {text,reply_markup:{inline_keyboard:buttons}};
}
