# -*- coding: utf-8 -*-

from bridge.context import ContextType, Context
from bridge.reply import Reply, ReplyType
from channel.chat_message import ChatMessage
import plugins
import json
import os
import re
from threading import Timer
import time
from datetime import datetime
import shutil
import uuid
from plugins import *
from common.log import logger
from config import conf
import copy
import traceback

try:
    from channel.gewechat.gewechat_channel import GeWeChatChannel
except ImportError:
    logger.warning("[RevocationAndLogger] 未找到 gewechat channel，防撤回功能可能受限。")
    GeWeChatChannel = None

@plugins.register(
    name="RevocationAndLogger",
    desire_priority=100,
    hidden=False,
    namecn="防撤回与群聊记录",
    desc="防撤回(修复群聊通知v1.0)、群聊记录txt、最后发言查询(文本)",
    version="1.0", # Incremented version for command fix and hint optimize
    author="Misk00",
)
class RevocationAndLogger(Plugin):
    def __init__(self):
        super().__init__()
        self.config = super().load_config()
        if not self.config:
            self.config = self._load_config_template()

        self.handlers[Event.ON_RECEIVE_MESSAGE] = self.on_receive_message
        self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context

        logger.info("[RevocationAndLogger] 插件初始化 (V1.0 - 修复命令回复及提示优化)")

        self.msg_dict = {}
        self.target_friend = None
        self.group_info_cache = {}
        self.cache_expiry_time = self.config.get("group_cache_expiry", 3600)

        self.tmp_dir = os.path.join(os.getcwd(), 'tmp')
        if not os.path.exists(self.tmp_dir): os.makedirs(self.tmp_dir)
        self.log_dir = os.path.join(os.getcwd(), self.config.get("chat_log_dir", "chat_logs"))
        if not os.path.exists(self.log_dir):
            try:
                os.makedirs(self.log_dir)
                logger.info(f"[RevocationAndLogger] 创建聊天记录目录: {self.log_dir}")
            except Exception as e: logger.error(f"[RevocationAndLogger] 创建聊天记录目录失败: {e}")
        self.last_spoken_dir = os.path.join(self.log_dir, "last_spoken")
        if not os.path.exists(self.last_spoken_dir):
            try:
                os.makedirs(self.last_spoken_dir)
                logger.info(f"[RevocationAndLogger] 创建最后发言记录目录: {self.last_spoken_dir}")
            except Exception as e: logger.error(f"[RevocationAndLogger] 创建最后发言记录目录失败: {e}")

        self.start_cleanup_timer()

        self.gewechat_channel = None
        if GeWeChatChannel:
            try:
                self.gewechat_channel = GeWeChatChannel()
                logger.info("[RevocationAndLogger] 已初始化gewechat支持")
            except Exception as e:
                logger.error(f"[RevocationAndLogger] 初始化gewechat失败: {str(e)}")
        else:
                logger.warning("[RevocationAndLogger] GeWeChatChannel 未导入或初始化失败，无法使用gewechat特定功能 (如获取用户名/群名)。")

        self.quote_pattern = re.compile(r"^「(.+?)\s*:\s*<msg>.*?</msg>\s*」\s*[-—]+\s*(.*)$", re.DOTALL)

        self.command_trigger = self.config.get("last_spoken_command", "最后信息").strip()
        if not self.command_trigger:
            logger.warning("[RevocationAndLogger] 未配置最后发言查询命令 (last_spoken_command)，将使用默认值 '最后信息'")
            self.command_trigger = "最后信息"
        logger.info(f"[RevocationAndLogger] 最后发言记录查询命令: '{self.command_trigger}'")

    def _load_config_template(self):
        logger.debug("[RevocationAndLogger] 未找到配置文件，使用模板")
        default_conf = {
            "receiver": {"type": "wxid", "name": "filehelper"},
            "message_expire_time": 120,
            "cleanup_interval": 60,
            "chat_log_dir": "chat_logs",
            "last_spoken_command": "最后信息",
            "group_cache_expiry": 3600,
            "download_timeout": 20
        }
        try:
            plugin_config_path = os.path.join(self.path, "config.json.template")
            if os.path.exists(plugin_config_path):
                with open(plugin_config_path, "r", encoding="utf-8") as f: plugin_conf = json.load(f)
                for key, value in default_conf.items(): plugin_conf.setdefault(key, value)
                return plugin_conf
            else:
                logger.warning("[RevocationAndLogger] 模板配置文件 (config.json.template) 也不存在，生成默认配置")
                return default_conf
        except Exception as e:
            logger.exception(f"[RevocationAndLogger] 加载模板配置失败: {e}")
            return default_conf

    def get_help_text(self, **kwargs):
        help_text = f"防撤回与群聊记录插件 V{self.version} 说明:\n"
        help_text += "1. 自动保存最近消息并在检测到撤回时转发给指定接收者。\n"
        help_text += "2. 将接收到的群聊消息按 群聊ID.txt 格式保存文件。\n"
        help_text += "3. 记录每个群成员的最后发言时间。\n"
        help_text += f"4. 在群聊中发送 '{self.command_trigger}' 可获取该群成员最后发言时间的文本记录。\n"
        help_text += "5. 首次记录某群聊时，会在文件开头写入群名和ID。\n"
        help_text += f"6. 聊天记录默认保存在: '{self.config.get('chat_log_dir', 'chat_logs')}' 文件夹。\n"
        help_text += f"7. 最后发言记录在上述目录下的 'last_spoken' 子文件夹中。\n"
        help_text += "8. 优化了引用消息的记录格式。\n"
        help_text += "9. 防撤回功能目前仅明确支持gewechat协议。\n"
        return help_text

    def sanitize_filename(self, name):
        if not name: return f"unknown_{str(uuid.uuid4())[:8]}"
        name = re.sub(r'[\\/*?:"<>|]+', '_', name)
        name = name.strip('. ')
        name = re.sub(r'[\s_]+', '_', name)
        if not name: return f"unknown_{str(uuid.uuid4())[:8]}"
        return name

    def log_group_message(self, msg: ChatMessage):
        file_name = ""
        group_id_str = msg.from_user_id if msg else 'N/A'
        try:
            if msg.ctype == ContextType.REVOKE: return
            group_id = msg.from_user_id
            if not group_id: logger.warning("[RevocationAndLogger] 无法获取群聊ID，跳过记录"); return

            safe_group_id_filename = self.sanitize_filename(group_id)
            file_name = f"{safe_group_id_filename}.txt"
            log_file_path = os.path.join(self.log_dir, file_name)

            write_header = not os.path.exists(log_file_path)
            header_content = ""
            if write_header:
                try:
                    group_name, _ = self.get_group_info(group_id)
                    header_content += f"# 群聊名称: {group_name if group_name else '未能获取'}\n"
                    header_content += f"# 群聊 ID: {group_id}\n"
                    header_content += f"# 文件创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    header_content += "---\n"
                except Exception as header_e:
                    logger.error(f"[RevocationAndLogger] 获取群名或生成文件头失败 (GroupID: {group_id}): {header_e}")
                    write_header = False

            sender_nickname = msg.actual_user_nickname or msg.actual_user_id or "未知成员"
            try:
                ts = msg.create_time
                timestamp_str = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M") \
                                if isinstance(ts, (int, float)) else \
                                ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, datetime) else \
                                datetime.now().strftime("%Y-%m-%d %H:%M")
            except Exception: timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M")

            content_to_log = ""
            if msg.ctype == ContextType.TEXT:
                match = self.quote_pattern.match(msg.content)
                if match:
                    quoted_sender = match.group(1).strip()
                    reply_text = match.group(2).strip().replace('\n', ' ')
                    content_to_log = f"[引用消息 「{quoted_sender}」] {reply_text}"
                else: content_to_log = msg.content.replace('\n', ' ')
            elif msg.ctype == ContextType.IMAGE: content_to_log = "[图片]"
            elif msg.ctype == ContextType.VIDEO: content_to_log = "[视频]"
            elif msg.ctype == ContextType.VOICE: content_to_log = "[语音]"
            elif msg.ctype == ContextType.FILE:
                fname = os.path.basename(msg.content) if msg.content else "未知文件"
                content_to_log = f"[文件: {fname}]"
            elif msg.ctype == ContextType.SHARING:
                title = "未知链接"
                try:
                    if hasattr(msg, 'link_title') and msg.link_title: title = msg.link_title
                    elif isinstance(msg.content, str) and '<title>' in msg.content:
                        match = re.search(r'<title>(.*?)</title>', msg.content, re.DOTALL); title = match.group(1).strip() if match else title
                except Exception: pass
                content_to_log = f"[链接/卡片: {title}]"
            elif msg.ctype == ContextType.CARD:
                name = "未知用户"
                try:
                    if isinstance(msg.content, str) and 'nickname="' in msg.content:
                        match = re.search(r'nickname="([^"]*)"', msg.content); name = match.group(1) if match else name
                except Exception: pass
                content_to_log = f"[名片: {name}]"
            elif msg.ctype == ContextType.PATPAT: content_to_log = "[拍了拍]"
            elif msg.ctype == ContextType.ACCEPT_FRIEND: content_to_log = "[接受好友请求]"
            elif msg.ctype == ContextType.JOIN_GROUP: content_to_log = f"[入群通知: {sender_nickname}]"
            elif msg.ctype == ContextType.EXIT_GROUP: content_to_log = f"[退群通知: {sender_nickname}]"
            elif msg.ctype == ContextType.SYSTEM:
                content = msg.content.strip().replace('\n',' ')[:50] if isinstance(msg.content, str) else ""; content_to_log = f"[系统消息: {content}...]" if content else "[系统消息]"
            else: content_to_log = f"[未知类型: {msg.ctype.name}]"

            log_line = f"{timestamp_str} 【{sender_nickname}】{content_to_log}\n"
            try:
                mode = 'w' if write_header else 'a'
                with open(log_file_path, mode, encoding='utf-8') as f:
                    if write_header: f.write(header_content)
                    f.write(log_line)
            except IOError as io_err: logger.error(f"[RevocationAndLogger] 写入日志文件IO错误 (File: {file_name}): {io_err}")
        except Exception as e:
            logger.error(f"[RevocationAndLogger] 记录群聊消息失败 (GroupID: {group_id_str}, FileName: {file_name}): {e}")
            logger.error(f"[RevocationAndLogger] 错误详情: {traceback.format_exc()}")

    def update_last_spoken_time(self, group_id: str, nickname: str, timestamp_str: str):
        if not group_id or not nickname: logger.warning(f"[RevocationAndLogger] update_last_spoken_time: 无效的 group_id 或 nickname ({group_id}, {nickname})"); return
        if not hasattr(self, 'last_spoken_dir') or not self.last_spoken_dir: logger.error("[RevocationAndLogger] last_spoken_dir 未初始化"); return
        os.makedirs(self.last_spoken_dir, exist_ok=True)

        file_name = f"{self.sanitize_filename(group_id)}-最后发言.txt"
        file_path = os.path.join(self.last_spoken_dir, file_name)
        search_prefix = f"【{nickname}】"; new_line = f"{search_prefix}{timestamp_str}\n"
        lines = []; found = False; content_changed = False
        try:
            try:
                with open(file_path, 'r', encoding='utf-8') as f: lines = f.readlines()
            except FileNotFoundError: content_changed = True; pass
            output_lines = []
            for line in lines:
                if line.startswith(search_prefix):
                    found = True
                    if line.strip() != new_line.strip(): output_lines.append(new_line); content_changed = True
                    else: output_lines.append(line)
                else: output_lines.append(line)
            if not found: output_lines.append(new_line); content_changed = True
            if content_changed:
                with open(file_path, 'w', encoding='utf-8') as f: f.writelines(output_lines)
        except IOError as e: logger.error(f"[RevocationAndLogger] 读写最后发言文件失败 (File: {file_path}): {e}")
        except Exception as e:
            logger.error(f"[RevocationAndLogger] 更新最后发言时间时发生未知错误 (File: {file_path}): {e}")
            logger.error(f"[RevocationAndLogger] 错误详情: {traceback.format_exc()}")

    def get_revoke_msg_receiver(self):
        if self.target_friend is None:
            receiver_config = self.config.get("receiver", {}); match_name = receiver_config.get("name", "filehelper")
            logger.info(f"[RevocationAndLogger] 防撤回消息接收者: {match_name}")
            self.target_friend = { 'UserName': match_name, 'protocol': 'gewechat' }
        return self.target_friend

    def start_cleanup_timer(self):
        def delete_out_date_msg():
            try:
                current_time = time.time(); expire_time_secs = self.config.get("message_expire_time", 120); expired_ids = []
                for msg_id, msg_info in list(self.msg_dict.items()):
                    stored_msg = msg_info[0] if isinstance(msg_info, tuple) else msg_info
                    if not hasattr(stored_msg, 'create_time'): continue
                    try:
                        ts = stored_msg.create_time; msg_timestamp = ts.timestamp() if isinstance(ts, datetime) else float(ts)
                        if (current_time - msg_timestamp) > expire_time_secs: expired_ids.append(msg_id)
                    except Exception as ts_err: logger.warning(f"[RevocationAndLogger] 清理缓存时处理时间戳错误 for msg_id {msg_id}: {ts_err}"); continue
                removed_count = 0
                for msg_id in expired_ids:
                    if msg_id in self.msg_dict:
                        msg_info = self.msg_dict.pop(msg_id); removed_count += 1
                        if isinstance(msg_info, tuple):
                            _, file_path = msg_info
                            if file_path and os.path.exists(file_path):
                                try: os.remove(file_path)
                                except Exception as e: logger.error(f"[RevocationAndLogger] 删除过期临时文件失败: {file_path}, Error: {e}")
            except Exception as e: logger.error(f"[RevocationAndLogger] 缓存清理任务出错: {e}"); logger.error(f"[RevocationAndLogger] 错误详情: {traceback.format_exc()}")
            finally:
                interval = self.config.get("cleanup_interval", 60); cleanup_timer = Timer(interval, delete_out_date_msg); cleanup_timer.daemon = True; cleanup_timer.start()
        logger.info("[RevocationAndLogger] 启动消息缓存清理定时器..."); initial_timer = Timer(1, delete_out_date_msg); initial_timer.daemon = True; initial_timer.start()

    def copy_to_tmp(self, file_path):
        try:
            if not file_path or not os.path.exists(file_path): logger.warning(f"[RevocationAndLogger] File not found for copying to tmp: {file_path}"); return None
            ext = os.path.splitext(file_path)[1]; base_name = os.path.basename(file_path)
            unique_name = f"{os.path.splitext(base_name)[0]}_{str(uuid.uuid4())[:8]}{ext}"; target_path = os.path.join(self.tmp_dir, unique_name)
            shutil.copy2(file_path, target_path); return target_path
        except Exception as e: logger.error(f"[RevocationAndLogger] 复制文件到tmp失败: {e}"); return None

    def download_files(self, msg: ChatMessage):
        try:
            file_path = msg.content
            if isinstance(file_path, str) and file_path and os.path.exists(file_path): return file_path
            if hasattr(msg, 'url') and msg.url and isinstance(msg.url, str):
                logger.info(f"[RevocationAndLogger] Attempting to download file from URL: {msg.url}")
                try: import requests
                except ImportError: logger.error("[RevocationAndLogger] 下载文件需要 'requests' 库"); return None
                try:
                    response = requests.get(msg.url, timeout=self.config.get("download_timeout", 20)); response.raise_for_status()
                    url_ext=os.path.splitext(msg.url)[1]; orig_path_ext=os.path.splitext(file_path if isinstance(file_path, str) else "")[1]; ext = url_ext or orig_path_ext or ".dat"
                    orig_basename = os.path.basename(file_path) if isinstance(file_path, str) and file_path else None; name = orig_basename or f"dl_{str(uuid.uuid4())[:8]}{ext}"; safe_name = self.sanitize_filename(name)
                    os.makedirs(self.tmp_dir, exist_ok=True); target_path = os.path.join(self.tmp_dir, safe_name); counter=0; base_target = target_path
                    while os.path.exists(target_path): counter+=1; fn, fext = os.path.splitext(base_target); target_path = f"{fn}_{counter}{fext}"
                    with open(target_path, 'wb') as f: f.write(response.content); logger.info(f"[RevocationAndLogger] 文件下载成功: {target_path}")
                    msg.content = target_path; return target_path
                except requests.exceptions.RequestException as e: logger.error(f"[RevocationAndLogger] 下载文件失败 (URL: {msg.url}): {e}"); return None
                except Exception as e: logger.error(f"[RevocationAndLogger] 下载或保存文件时出错 (URL: {msg.url}): {e}"); return None
            else: return None
        except Exception as e: logger.error(f"[RevocationAndLogger] download_files 处理异常: {e}"); return None

    def get_user_info(self, user_id):
        try:
            if not self.gewechat_channel or not hasattr(self.gewechat_channel, 'client') or not self.gewechat_channel.client: return user_id
            client = self.gewechat_channel.client; app_id = self.gewechat_channel.app_id
            if not app_id: return user_id
            method_name = 'getBriefInfo' if hasattr(client,'getBriefInfo') else 'get_brief_info'
            if not hasattr(client, method_name): logger.warning(f"[RevocationAndLogger] gewechat client missing user info method: {method_name}"); return user_id
            api_method = getattr(client, method_name); res = api_method(app_id, [user_id])
            if res and res.get('ret') == 200 and res.get('data') and isinstance(res['data'], list) and len(res['data']) > 0:
                info = res['data'][0]; name = info.get('remark') or info.get('nickName')
                if name: return name
            return user_id
        except Exception as e: logger.error(f"[RevocationAndLogger] 获取用户信息 API 调用失败 for {user_id}: {e}"); return user_id

    def handle_revoke(self, msg: ChatMessage, is_group=False):
        logger.info(f"[RevocationAndLogger V1.0] Processing revoke message (Group: {is_group})...")
        revoke_xml_content = msg.content if isinstance(msg.content, str) else ""

        revoked_msg_id_attr = str(getattr(msg, 'revoked_msg_id', ''))
        msgid_xml = None
        newmsgid_xml = None
        possible_ids = []

        try:
            if revoke_xml_content:
                msgid_match = re.search(r"<msgid>(.*?)</msgid>", revoke_xml_content)
                if msgid_match: msgid_xml = str(msgid_match.group(1))
                newmsgid_match = re.search(r"<newmsgid>(.*?)</newmsgid>", revoke_xml_content)
                if newmsgid_match: newmsgid_xml = str(newmsgid_match.group(1))

            if revoked_msg_id_attr: possible_ids.append(revoked_msg_id_attr)
            if newmsgid_xml and newmsgid_xml not in possible_ids: possible_ids.append(newmsgid_xml)
            if msgid_xml and msgid_xml not in possible_ids: possible_ids.append(msgid_xml)

        except Exception as e:
            logger.error(f"[RevocationAndLogger V1.0] Error parsing revoke message IDs from XML: {e}")

        if not possible_ids:
            logger.error("[RevocationAndLogger V1.0] Failed to extract any potential revoked message ID.")
            return

        logger.info(f"[RevocationAndLogger V1.0] Possible revoked IDs: {possible_ids}. Cache keys: {list(self.msg_dict.keys())}")

        found_msg_info = None
        found_id = None
        for try_id in possible_ids:
            if try_id in self.msg_dict:
                found_msg_info = self.msg_dict[try_id]
                found_id = try_id
                logger.info(f"[RevocationAndLogger V1.0] Found original message in cache using ID: {found_id}")
                break

        if not found_msg_info:
            logger.warning(f"[RevocationAndLogger V1.0] Original message not found in cache for IDs: {possible_ids}")
            return

        original_msg, tmp_file_path = found_msg_info if isinstance(found_msg_info, tuple) else (found_msg_info, None)
        target = self.get_revoke_msg_receiver()
        if not target:
            logger.error("[RevocationAndLogger V1.0] Cannot get revoke message receiver config.")
            return

        try:
            if not self.gewechat_channel or not self.gewechat_channel.client:
                logger.error("[RevocationAndLogger V1.0] gewechat client not initialized.")
                return
            client = self.gewechat_channel.client
            app_id = self.gewechat_channel.app_id
            receiver = target.get('UserName')
            if not client or not app_id or not receiver:
                logger.error("[RevocationAndLogger V1.0] gewechat client, app_id or receiver is invalid.")
                return

            prefix = ""
            if is_group:
                group_id = original_msg.from_user_id
                group_name, _ = self.get_group_info(group_id)
                from_name = group_name or f"群聊({group_id})"

                actual_name = "未知成员"
                revoker_id = None

                if hasattr(original_msg, 'actual_user_nickname') and original_msg.actual_user_nickname:
                    actual_name = original_msg.actual_user_nickname
                    revoker_id = getattr(original_msg, 'actual_user_id', None)
                    logger.info(f"[RevocationAndLogger V1.0] Got revoker from original_msg: {actual_name} (ID: {revoker_id})")
                elif hasattr(original_msg, 'actual_user_id') and original_msg.actual_user_id:
                    revoker_id = original_msg.actual_user_id
                    logger.info(f"[RevocationAndLogger V1.0] Got revoker ID from original_msg: {revoker_id}, looking up name...")
                    actual_name_lookup = self.get_user_info(revoker_id)
                    if actual_name_lookup != revoker_id:
                        actual_name = actual_name_lookup
                        logger.info(f"[RevocationAndLogger V1.0] Looked up revoker name: {actual_name}")
                    else:
                        logger.warning(f"[RevocationAndLogger V1.0] Lookup for {revoker_id} failed, name unknown.")
                        actual_name = "未知成员"

                if (actual_name == "未知成员" or not actual_name) and revoke_xml_content:
                    logger.info("[RevocationAndLogger V1.0] Trying fallback: Parsing nickname from revoke XML <replacemsg>...")
                    try:
                        replacemsg_match = re.search(r'<!\[CDATA\["([^"]+)"\s+撤回了一条消息\]\]>', revoke_xml_content, re.IGNORECASE)
                        if replacemsg_match:
                            parsed_name = replacemsg_match.group(1)
                            actual_name = parsed_name
                            logger.info(f"[RevocationAndLogger V1.0] Parsed nickname from <replacemsg>: {actual_name}")
                        else:
                            logger.warning("[RevocationAndLogger V1.0] Failed to parse nickname from <replacemsg> CDATA.")
                    except Exception as parse_e:
                        logger.error(f"[RevocationAndLogger V1.0] Error parsing <replacemsg>: {parse_e}")

                if not actual_name or actual_name == "未知成员": actual_name = f"用户({revoker_id or '未知ID'})"
                prefix = f"群「{from_name}」的成员「{actual_name}」"

            else:
                sender_id = original_msg.from_user_id
                from_name = self.get_user_info(sender_id)
                prefix = f"好友「{from_name}」"

            logger.info(f"[RevocationAndLogger V1.0] Constructed prefix: {prefix}")

            if original_msg.ctype == ContextType.TEXT:
                logger.info(f"[RevocationAndLogger V1.0] Sending TEXT revoke notification to {receiver}...")
                client.post_text(app_id, receiver, f"{prefix} 撤回了一条消息:\n---\n{original_msg.content}", "")
                logger.info(f"[RevocationAndLogger V1.0] TEXT notification sent.")
            elif tmp_file_path and os.path.exists(tmp_file_path) and original_msg.ctype in [ContextType.IMAGE, ContextType.VIDEO, ContextType.FILE, ContextType.VOICE]:
                type_str_map = { ContextType.IMAGE: "图片", ContextType.VIDEO: "视频", ContextType.FILE: "文件", ContextType.VOICE: "语音" }
                type_str = type_str_map.get(original_msg.ctype, "媒体文件")
                logger.info(f"[RevocationAndLogger V1.0] Sending {type_str} revoke notification to {receiver}...")
                client.post_text(app_id, receiver, f"{prefix} 撤回了一个{type_str}👇", "")

                callback_url = conf().get("gewechat_callback_url", "").rstrip('/')
                if callback_url:
                    try:
                        rel_path = os.path.relpath(tmp_file_path, os.getcwd()).replace(os.sep, '/')
                        if not rel_path.startswith('tmp/'): rel_path = 'tmp/' + os.path.basename(tmp_file_path)
                        file_url = f"{callback_url}?file={rel_path}"
                        logger.info(f"[RevocationAndLogger V1.0] Sending file via URL: {file_url}")
                        if original_msg.ctype == ContextType.IMAGE: client.post_image(app_id, receiver, file_url)
                        elif original_msg.ctype == ContextType.VIDEO: client.post_file(app_id, receiver, file_url, os.path.basename(tmp_file_path))
                        elif original_msg.ctype == ContextType.VOICE: client.post_file(app_id, receiver, file_url, os.path.basename(tmp_file_path))
                        else: client.post_file(app_id, receiver, file_url, os.path.basename(tmp_file_path))
                        logger.info(f"[RevocationAndLogger V1.0] File notification sent.")
                    except Exception as send_e:
                        logger.error(f"[RevocationAndLogger V1.0] Failed to send file via callback URL: {send_e}")
                        client.post_text(app_id, receiver, f"（无法发送被撤回的{type_str}文件，请检查回调配置或临时文件）", "")
                else:
                    logger.error("[RevocationAndLogger V1.0] gewechat_callback_url not configured, cannot send file content.")
                    client.post_text(app_id, receiver, f"（无法发送被撤回的{type_str}文件，回调URL未配置）", "")
            elif original_msg.ctype not in [ContextType.TEXT]:
                type_name = original_msg.ctype.name
                logger.info(f"[RevocationAndLogger V1.0] Sending {type_name} type revoke notification to {receiver}...")
                client.post_text(app_id, receiver, f"{prefix} 撤回了一条 {type_name} 类型的消息。", "")
                logger.info(f"[RevocationAndLogger V1.0] Type notification sent.")
            else: logger.warning(f"[RevocationAndLogger V1.0] Unhandled original message type for revoke: {original_msg.ctype}, tmp_file_path: {tmp_file_path}")

            logger.info(f"[RevocationAndLogger V1.0] Revoke handling appears complete for ID {found_id}.")
        except Exception as e:
            logger.error(f"[RevocationAndLogger V1.0] Exception during revoke notification sending: {e}")
            logger.error(f"[RevocationAndLogger V1.0] Traceback: {traceback.format_exc()}")

    def handle_msg(self, msg: ChatMessage, is_group=False):
        try:
            if msg.ctype == ContextType.REVOKE: self.handle_revoke(msg, is_group); return
            expire_duration=self.config.get("message_expire_time",120); current_time=time.time()
            try:
                ts=msg.create_time; msg_timestamp=ts.timestamp() if isinstance(ts,datetime) else float(ts)
                if msg_timestamp < (current_time - expire_duration): return
            except Exception as time_err: logger.warning(f"[RevocationAndLogger] 无法处理消息时间戳 {msg.msg_id}: {time_err}. 跳过缓存."); return

            msg_id_str = str(msg.msg_id); cached_data = None
            if msg.ctype == ContextType.TEXT: cached_data = msg
            elif msg.ctype in [ContextType.IMAGE, ContextType.VIDEO, ContextType.FILE, ContextType.VOICE]:
                local_path = self.download_files(msg)
                if not local_path: logger.warning(f"[RevocationAndLogger] 无法获取文件路径，跳过缓存: {msg_id_str} ({msg.ctype.name})"); return
                tmp_path = self.copy_to_tmp(local_path)
                if not tmp_path: logger.warning(f"[RevocationAndLogger] 无法复制文件到tmp，跳过缓存: {msg_id_str} ({msg.ctype.name})"); return
                msg_copy = copy.copy(msg); msg_copy.content = tmp_path; cached_data = (msg_copy, tmp_path)
            elif msg.ctype in [ContextType.SHARING, ContextType.CARD, ContextType.PATPAT]: cached_data = msg

            if cached_data:
                self.msg_dict[msg_id_str] = cached_data
                if hasattr(msg, 'msg_data') and isinstance(msg.msg_data, dict) and 'MsgId' in msg.msg_data:
                    internal_id_str = str(msg.msg_data['MsgId'])
                    if internal_id_str != msg_id_str and internal_id_str not in self.msg_dict: self.msg_dict[internal_id_str] = cached_data
        except Exception as e:
            logger.error(f"[RevocationAndLogger] 缓存消息失败 ({msg.msg_id if hasattr(msg, 'msg_id') else 'N/A'}): {e}")
            logger.error(f"[RevocationAndLogger] 错误详情: {traceback.format_exc()}")

    def get_group_info(self, group_id, force_refresh=False):
        current_time = time.time()
        if not force_refresh and group_id in self.group_info_cache:
            cached_name, expiry = self.group_info_cache[group_id]
            if current_time < expiry: return cached_name, {}
        group_name = None; member_dict = {}
        try:
            if not self.gewechat_channel or not self.gewechat_channel.client: return group_id, {}
            client = self.gewechat_channel.client; app_id = self.gewechat_channel.app_id
            if not app_id: return group_id, {}
            method_name = 'getChatroomInfo' if hasattr(client, 'getChatroomInfo') else 'get_chatroom_info'
            if hasattr(client, method_name):
                api_method = getattr(client, method_name); res = api_method(app_id, group_id)
                if res and res.get('ret') == 200 and res.get('data'): group_name = res['data'].get('nickName') or res['data'].get('remark')
            else: logger.warning(f"[RevocationAndLogger] gewechat client missing group info method: {method_name}")
            final_name = group_name or group_id; self.group_info_cache[group_id] = (final_name, current_time + self.cache_expiry_time)
            return final_name, member_dict
        except Exception as e:
            logger.error(f"[RevocationAndLogger] 获取群信息 API 调用失败 for {group_id}: {e}")
            self.group_info_cache[group_id] = (group_id, current_time + 60); return group_id, {}

    def on_receive_message(self, e_context: EventContext):
        try:
            context: Context = e_context['context']; cmsg: ChatMessage = context.get('msg')
            if not cmsg: return
            if cmsg.is_group: self.handle_group_msg(cmsg)
            else: self.handle_single_msg(cmsg)
        except Exception as e:
            logger.error(f"[RevocationAndLogger] on_receive_message 处理失败: {e}")
            logger.error(f"[RevocationAndLogger] Traceback: {traceback.format_exc()}")

    def handle_single_msg(self, msg: ChatMessage):
        self.handle_msg(msg, is_group=False)

    def handle_group_msg(self, msg: ChatMessage):
        self.log_group_message(msg)
        if msg.ctype != ContextType.REVOKE:
            try:
                group_id = msg.from_user_id; sender_nickname = msg.actual_user_nickname or msg.actual_user_id or "未知成员"; ts = msg.create_time
                timestamp_str = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M") if isinstance(ts, (int, float)) else ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, datetime) else datetime.now().strftime("%Y-%m-%d %H:%M")
                if group_id and sender_nickname: self.update_last_spoken_time(group_id, sender_nickname, timestamp_str)
            except Exception as e: logger.error(f"[RevocationAndLogger] 调用 update_last_spoken_time 失败: {e}")
        self.handle_msg(msg, is_group=True)

    def on_handle_context(self, e_context: EventContext):
        context: Context = e_context['context']
        msg: ChatMessage = context.get('msg')
        channel = e_context['channel']

        if context.type == ContextType.TEXT and msg and msg.is_group:
            content = context.content.strip()
            if content == self.command_trigger:
                logger.info(f"[RevocationAndLogger] 收到命令 '{self.command_trigger}' 来自群聊 {msg.from_user_id}")
                e_context.action = EventAction.BREAK_PASS

                group_id = msg.from_user_id
                reply_text = ""

                if not group_id:
                    reply_text = "无法获取当前群聊ID"
                elif not hasattr(self, 'last_spoken_dir') or not self.last_spoken_dir:
                    reply_text = "内部错误：记录目录未初始化"
                else:
                    file_name = f"{self.sanitize_filename(group_id)}-最后发言.txt"
                    file_path = os.path.join(self.last_spoken_dir, file_name)

                    if os.path.exists(file_path):
                        logger.info(f"[RevocationAndLogger] 找到文件: {file_path}")
                        try:
                            with open(file_path, 'r', encoding='utf-8') as f:
                                file_content = f.read()
                            if file_content and file_content.strip():
                                reply_text = file_content
                                logger.info("[RevocationAndLogger] 准备发送文件内容")
                            else:
                                reply_text = "记录文件为空。"
                                logger.info("[RevocationAndLogger] 文件为空")
                        except Exception as e:
                            logger.error(f"[RevocationAndLogger] 读取文件失败: {file_path}, Error: {e}")
                            reply_text = "读取记录文件出错"
                    else:
                        logger.warning(f"[RevocationAndLogger] 未找到文件: {file_path}")
                        group_name, _ = self.get_group_info(group_id)
                        display_name = group_name if group_name != group_id else f"本群"
                        reply_text = f"{display_name} 尚无发言记录。"
                        logger.info("[RevocationAndLogger] 已设置未找到文件回复")

                if reply_text:
                    try:
                        reply = Reply(ReplyType.TEXT, reply_text)
                        channel.send(reply, context)
                        logger.info(f"[RevocationAndLogger] 已通过 channel.send 发送回复至群聊 {group_id}")
                        e_context['reply'] = None
                    except Exception as send_e:
                         logger.error(f"[RevocationAndLogger] 使用 channel.send 发送回复失败: {send_e}")
                         e_context['reply'] = None
                else:
                     e_context['reply'] = None
                     logger.warning("[RevocationAndLogger] reply_text 为空，未发送任何回复。")

                return

        return
