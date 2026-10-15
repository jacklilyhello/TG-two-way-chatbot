export interface TGUser { id: number; is_bot?: boolean; first_name?: string; last_name?: string; username?: string }
export type TGMessageOrigin =
 | {type:'user';sender_user:TGUser;date:number}
 | {type:'hidden_user';sender_user_name:string;date:number}
 | {type:'chat';sender_chat:{id:number};date:number}
 | {type:'channel';chat:{id:number};message_id:number;date:number};
export interface TGMessage {
  message_id: number; date: number; edit_date?: number; chat: {id: number; type: string; is_forum?: boolean};
  from?: TGUser; sender_chat?: {id: number}; message_thread_id?: number; is_topic_message?: boolean;
  text?: string; entities?: unknown[]; caption?: string; caption_entities?: unknown[];
  photo?: {file_id: string}[]; video?: {file_id: string}; audio?: {file_id: string}; voice?: {file_id: string};
  document?: {file_id: string}; sticker?: {file_id: string}; animation?: {file_id: string};
  media_group_id?: string; reply_to_message?: TGMessage;
  forum_topic_created?: {name:string;icon_color:number;icon_custom_emoji_id?:string};
  forward_origin?: TGMessageOrigin;
}
export interface TGUpdate {update_id: number; message?: TGMessage; edited_message?: TGMessage; callback_query?: {id: string; from: TGUser; data?: string; message?: TGMessage}}
export interface Env {
  DB: D1Database; JOBS: Queue<{key: string}>; CONVERSATIONS: DurableObjectNamespace; BOT: DurableObjectNamespace;
  TELEGRAM_BOT_TOKEN: string; TELEGRAM_WEBHOOK_SECRET: string;
  INSTANCE_ID: string; EXPECTED_BOT_ID: string; ADMIN_CHAT_ID: string; ADMIN_IDS: string;
  VERSION: string; PAUSED?: string; RECOVERY_EPOCH: string;
}
export interface UserRow {tgid: string; first_name: string; last_name: string; username: string | null; first_contact: number; blocked: number; moderation_version?: number; thread_id: number | null; binding_version: number; card_id: number | null; photo_id: string | null}
export interface Job {id: number; dedupe: string; user_key: string; kind: string; payload: string; state: string; due_at: number; attempts: number; epoch: string; created_at: number}
export interface MessageRow {id: number; user_id: string; direction: string; source_chat: string; source_id: number; admin_id: number | null; private_id: number | null; thread_id: number | null; business: number; media_group: string | null; received_at: number; version: number; delivery: string; deletion: string; tombstone: number; payload: string; epoch: string}
export type Params = Record<string, unknown>;
