# -*- coding: utf-8 -*-

# --- Imports ---
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
from plugins import * # Event, EventContext, EventAction etc. are expected here
from common.log import logger
from config import conf
import copy # Needed for handle_msg copy.copy()
import traceback # For detailed error logging

try:
    # Attempt to import GeWeChatChannel for specific functionalities
    from channel.gewechat.gewechat_channel import GeWeChatChannel
except ImportError:
    logger.warning("[RevocationAndLogger] 未找到 gewechat channel，防撤回功能可能受限。")
    GeWeChatChannel = None
# --- End Imports ---

@plugins.register(
    name="RevocationAndLogger",
    desire_priority=100,
    hidden=False, # Make it visible in help
    namecn="防撤回与群聊记录",
    desc="防撤回、群聊记录txt、群成员最后发言时间记录与查询(发送'最后信息'以文本形式)", # Updated description
    version="1.0", # Incremented version
    author="sineom & Gemini",
)
class RevocationAndLogger(Plugin):
    def __init__(self):
        """
        Initializes the RevocationAndLogger plugin.
        Sets up configuration, event handlers, directories, and timers.
        """
        super().__init__()
        self.config = super().load_config()
        if not self.config:
            self.config = self._load_config_template()

        # --- Event Handlers ---
        self.handlers[Event.ON_RECEIVE_MESSAGE] = self.on_receive_message # For logging and revoke cache
        self.handlers[Event.ON_HANDLE_CONTEXT] = self.on_handle_context   # For handling user commands
        # --- End Event Handlers ---

        logger.info("[RevocationAndLogger] 插件初始化 (V1.9 - 发送最后信息为文本)")

        # --- Internal State ---
        self.msg_dict = {} # Cache for anti-revoke {msg_id: msg_object or (msg_object, temp_file_path)}
        self.target_friend = None # Cache for the revoke notification receiver config
        self.group_info_cache = {} # Cache for group names {group_id: (name, expiry_timestamp)}
        self.cache_expiry_time = self.config.get("group_cache_expiry", 3600) # Default 1 hour cache for group info
        # --- End Internal State ---

        # --- Directories ---
        # Temporary directory for file operations (e.g., copied files for revoke)
        self.tmp_dir = os.path.join(os.getcwd(), 'tmp')
        if not os.path.exists(self.tmp_dir): os.makedirs(self.tmp_dir)

        # Main chat log directory
        self.log_dir = os.path.join(os.getcwd(), self.config.get("chat_log_dir", "chat_logs"))
        if not os.path.exists(self.log_dir):
            try:
                os.makedirs(self.log_dir)
                logger.info(f"[RevocationAndLogger] 创建聊天记录目录: {self.log_dir}")
            except Exception as e: logger.error(f"[RevocationAndLogger] 创建聊天记录目录失败: {e}")

        # Directory for last spoken time files (within the main log dir)
        self.last_spoken_dir = os.path.join(self.log_dir, "last_spoken")
        if not os.path.exists(self.last_spoken_dir):
            try:
                os.makedirs(self.last_spoken_dir)
                logger.info(f"[RevocationAndLogger] 创建最后发言记录目录: {self.last_spoken_dir}")
            except Exception as e: logger.error(f"[RevocationAndLogger] 创建最后发言记录目录失败: {e}")
        # --- End Directories ---

        # Start the cleanup timer for the message cache
        self.start_cleanup_timer()

        # --- GeWeChat Integration ---
        self.gewechat_channel = None
        if GeWeChatChannel:
            try:
                # Instantiate the channel if available (needed for API calls like get_user_info)
                self.gewechat_channel = GeWeChatChannel()
                logger.info("[RevocationAndLogger] 已初始化gewechat支持")
            except Exception as e:
                logger.error(f"[RevocationAndLogger] 初始化gewechat失败: {str(e)}")
        else:
             logger.warning("[RevocationAndLogger] GeWeChatChannel 未导入或初始化失败，无法使用gewechat特定功能 (如获取用户名/群名)。")
        # --- End GeWeChat Integration ---

        # Regex for parsing quoted messages in WeChat format
        self.quote_pattern = re.compile(r"^「(.+?)\s*:\s*<msg>.*?</msg>\s*」\s*[-—]+\s*(.*)$", re.DOTALL)

        # --- Command Configuration ---
        # Load the command trigger word from config, default to "最后信息"
        self.command_trigger = self.config.get("last_spoken_command", "最后信息").strip()
        if not self.command_trigger:
            logger.warning("[RevocationAndLogger] 未配置最后发言查询命令 (last_spoken_command)，将使用默认值 '最后信息'")
            self.command_trigger = "最后信息"
        logger.info(f"[RevocationAndLogger] 最后发言记录查询命令: '{self.command_trigger}'")
        # --- End Command Configuration ---

    def _load_config_template(self):
        """Loads the default configuration template if config.json is missing."""
        logger.debug("[RevocationAndLogger] 未找到配置文件，使用模板")
        default_conf = {
            "receiver": {"type": "wxid", "name": "filehelper"}, # Who receives revoke notifications
            "message_expire_time": 120, # Seconds to keep messages in cache for revoke
            "cleanup_interval": 60,     # Seconds between cache cleanup runs
            "chat_log_dir": "chat_logs", # Directory for main chat logs
            "last_spoken_command": "最后信息", # Command to trigger sending last spoken info
            "group_cache_expiry": 3600 # Seconds to cache group names
        }
        try:
            plugin_config_path = os.path.join(self.path, "config.json.template")
            if os.path.exists(plugin_config_path):
                with open(plugin_config_path, "r", encoding="utf-8") as f:
                    plugin_conf = json.load(f)
                # Ensure all default keys exist, merge with defaults
                for key, value in default_conf.items():
                    plugin_conf.setdefault(key, value)
                return plugin_conf
            else:
                logger.warning("[RevocationAndLogger] 模板配置文件 (config.json.template) 也不存在，生成默认配置")
                return default_conf
        except Exception as e:
            logger.exception(f"[RevocationAndLogger] 加载模板配置失败: {e}")
            return default_conf # Return defaults on error

    def get_help_text(self, **kwargs):
        """Provides help text explaining the plugin's functionality."""
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
        """Removes or replaces characters that are invalid in filenames."""
        if not name: return f"unknown_{str(uuid.uuid4())[:8]}" # Handle empty names
        # Remove/replace invalid characters
        name = re.sub(r'[\\/*?:"<>|]+', '_', name)
        # Remove leading/trailing dots and spaces
        name = name.strip('. ')
        # Replace whitespace sequences with single underscore
        name = re.sub(r'[\s_]+', '_', name)
        # If name becomes empty after sanitization, generate a unique one
        if not name: return f"unknown_{str(uuid.uuid4())[:8]}"
        return name

    def log_group_message(self, msg: ChatMessage):
        """Logs a received group message to its corresponding .txt file."""
        file_name = "" # Initialize for logging scope
        group_id_str = msg.from_user_id if msg else 'N/A' # For logging

        try:
            # Do not log the revoke message itself (it's handled elsewhere)
            if msg.ctype == ContextType.REVOKE: return

            group_id = msg.from_user_id
            if not group_id:
                logger.warning("[RevocationAndLogger] 无法获取群聊ID，跳过记录")
                return

            # Filename is based solely on the group ID
            safe_group_id_filename = self.sanitize_filename(group_id)
            file_name = f"{safe_group_id_filename}.txt"
            log_file_path = os.path.join(self.log_dir, file_name)

            # --- File Header Handling (Write only if file doesn't exist) ---
            write_header = not os.path.exists(log_file_path)
            header_content = ""
            if write_header:
                try:
                    # Fetch group name only when creating the file
                    group_name, _ = self.get_group_info(group_id) # Ignore member dict for now
                    header_content += f"# 群聊名称: {group_name if group_name else '未能获取'}\n"
                    header_content += f"# 群聊 ID: {group_id}\n"
                    header_content += f"# 文件创建时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    header_content += "---\n"
                except Exception as header_e:
                    logger.error(f"[RevocationAndLogger] 获取群名或生成文件头失败 (GroupID: {group_id}): {header_e}")
                    write_header = False # Don't write potentially incorrect header
            # --- End File Header Handling ---

            # Determine sender nickname (use actual nickname, then ID, then fallback)
            sender_nickname = msg.actual_user_nickname or msg.actual_user_id or "未知成员"

            # Format timestamp consistently
            try:
                ts = msg.create_time
                # Handle both timestamp (int/float) and datetime objects
                timestamp_str = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M") \
                                if isinstance(ts, (int, float)) else \
                                ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, datetime) else \
                                datetime.now().strftime("%Y-%m-%d %H:%M") # Fallback to now
            except Exception:
                timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M") # Fallback on error

            # --- Determine content to log based on message type ---
            content_to_log = ""
            if msg.ctype == ContextType.TEXT:
                # Handle quoted messages specifically for better readability
                match = self.quote_pattern.match(msg.content)
                if match:
                    quoted_sender = match.group(1).strip()
                    reply_text = match.group(2).strip().replace('\n', ' ') # Clean up reply
                    content_to_log = f"[引用消息 「{quoted_sender}」] {reply_text}"
                else:
                    # Regular text message, replace newlines with spaces for single-line log entry
                    content_to_log = msg.content.replace('\n', ' ')
            elif msg.ctype == ContextType.IMAGE: content_to_log = "[图片]"
            elif msg.ctype == ContextType.VIDEO: content_to_log = "[视频]"
            elif msg.ctype == ContextType.VOICE: content_to_log = "[语音]"
            elif msg.ctype == ContextType.FILE:
                fname = os.path.basename(msg.content) if msg.content else "未知文件"
                content_to_log = f"[文件: {fname}]"
            elif msg.ctype == ContextType.SHARING:
                title = "未知链接"
                try:
                    # Prefer link_title if available
                    if hasattr(msg, 'link_title') and msg.link_title:
                         title = msg.link_title
                    # Fallback to parsing XML-like content
                    elif isinstance(msg.content, str) and '<title>' in msg.content:
                        match = re.search(r'<title>(.*?)</title>', msg.content, re.DOTALL)
                        if match: title = match.group(1).strip()
                except Exception: pass # Ignore parsing errors
                content_to_log = f"[链接/卡片: {title}]"
            elif msg.ctype == ContextType.CARD: # Contact card
                name = "未知用户"
                try:
                    # Parse XML-like content for nickname
                    if isinstance(msg.content, str) and 'nickname="' in msg.content:
                        match = re.search(r'nickname="([^"]*)"', msg.content)
                        if match: name = match.group(1)
                except Exception: pass # Ignore parsing errors
                content_to_log = f"[名片: {name}]"
            elif msg.ctype == ContextType.PATPAT: content_to_log = "[拍了拍]"
            elif msg.ctype == ContextType.ACCEPT_FRIEND: content_to_log = "[接受好友请求]" # Usually not in group?
            elif msg.ctype == ContextType.JOIN_GROUP: content_to_log = f"[入群通知: {sender_nickname}]"
            elif msg.ctype == ContextType.EXIT_GROUP: content_to_log = f"[退群通知: {sender_nickname}]"
            elif msg.ctype == ContextType.SYSTEM:
                # Log a snippet of system messages
                content = msg.content.strip().replace('\n',' ')[:50] if isinstance(msg.content, str) else ""
                content_to_log = f"[系统消息: {content}...]" if content else "[系统消息]"
            else:
                # Log unknown types for debugging
                content_to_log = f"[未知类型: {msg.ctype.name}]"
            # --- End Content Determination ---

            # Assemble the final log line
            log_line = f"{timestamp_str} 【{sender_nickname}】{content_to_log}\n"

            # --- Write to file ---
            # Use 'w' only if writing header (creates/overwrites file)
            # Use 'a' (append) otherwise
            try:
                mode = 'w' if write_header else 'a'
                with open(log_file_path, mode, encoding='utf-8') as f:
                    if write_header:
                        f.write(header_content)
                    f.write(log_line)
            except IOError as io_err:
               logger.error(f"[RevocationAndLogger] 写入日志文件IO错误 (File: {file_name}): {io_err}")
            # --- End Write to file ---

        except Exception as e:
            logger.error(f"[RevocationAndLogger] 记录群聊消息失败 (GroupID: {group_id_str}, FileName: {file_name}): {e}")
            logger.error(f"[RevocationAndLogger] 错误详情: {traceback.format_exc()}")

    def update_last_spoken_time(self, group_id: str, nickname: str, timestamp_str: str):
        """
        Updates the last spoken time file for a given user in a specific group.
        File format: {GroupID}-最后发言.txt, each line: 【Nickname】YYYY-MM-DD HH:MM
        """
        if not group_id or not nickname:
            logger.warning(f"[RevocationAndLogger] update_last_spoken_time: 无效的 group_id 或 nickname ({group_id}, {nickname})")
            return

        # Ensure the target directory exists and is initialized
        if not hasattr(self, 'last_spoken_dir') or not self.last_spoken_dir:
             logger.error("[RevocationAndLogger] last_spoken_dir 未初始化，无法更新最后发言时间。")
             return
        os.makedirs(self.last_spoken_dir, exist_ok=True) # Ensure directory exists just in case

        # Construct file path
        file_name = f"{self.sanitize_filename(group_id)}-最后发言.txt"
        file_path = os.path.join(self.last_spoken_dir, file_name)

        search_prefix = f"【{nickname}】"
        new_line = f"{search_prefix}{timestamp_str}\n"

        lines = []
        found = False
        content_changed = False # Flag to track if file needs rewriting

        try:
            # 1. Read existing file content (if it exists)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
            except FileNotFoundError:
                # File doesn't exist yet, so it will definitely change
                content_changed = True
                pass

            # 2. Process lines in memory: update existing or keep others
            output_lines = []
            for line in lines:
                if line.startswith(search_prefix):
                    found = True
                    # Only update if the timestamp is actually different
                    if line.strip() != new_line.strip():
                        output_lines.append(new_line) # Add updated line
                        content_changed = True
                        # logger.debug(f"[RevocationAndLogger] 更新最后发言时间: {nickname} -> {timestamp_str} in {file_name}")
                    else:
                        output_lines.append(line) # Keep original line if time hasn't changed
                        # logger.debug(f"[RevocationAndLogger] 最后发言时间未变，跳过更新: {nickname}")
                else:
                    # Keep lines for other users
                    output_lines.append(line)

            # 3. If user was not found, add their new line
            if not found:
                output_lines.append(new_line)
                content_changed = True # Added a new line
                # logger.debug(f"[RevocationAndLogger] 新增最后发言记录: {nickname} -> {timestamp_str} in {file_name}")

            # 4. Write back to the file only if content actually changed
            if content_changed:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.writelines(output_lines)
                # logger.debug(f"[RevocationAndLogger] 文件已写入: {file_path}")

        except IOError as e:
            logger.error(f"[RevocationAndLogger] 读写最后发言文件失败 (File: {file_path}): {e}")
        except Exception as e:
            logger.error(f"[RevocationAndLogger] 更新最后发言时间时发生未知错误 (File: {file_path}): {e}")
            logger.error(f"[RevocationAndLogger] 错误详情: {traceback.format_exc()}")

    def get_revoke_msg_receiver(self):
        """Gets the configured receiver for revoke notifications."""
        if self.target_friend is None: # Load and cache the receiver info
            receiver_config = self.config.get("receiver", {})
            match_name = receiver_config.get("name", "filehelper") # Default to filehelper
            logger.info(f"[RevocationAndLogger] 防撤回消息接收者: {match_name}")
            # Assuming gewechat protocol for now, adjust if other protocols need support
            self.target_friend = { 'UserName': match_name, 'protocol': 'gewechat' }
        return self.target_friend

    def start_cleanup_timer(self):
        """Starts a periodic timer to clean up expired messages from the cache."""
        def delete_out_date_msg():
            """The actual cleanup task run by the timer."""
            try:
                current_time = time.time()
                expire_time_secs = self.config.get("message_expire_time", 120) # Cache duration
                expired_ids = []

                # Iterate over a copy for safe deletion during iteration
                for msg_id, msg_info in list(self.msg_dict.items()):
                    # msg_info can be the message object or a tuple (msg, file_path)
                    stored_msg = msg_info[0] if isinstance(msg_info, tuple) else msg_info
                    if not hasattr(stored_msg, 'create_time'): continue # Skip if no timestamp

                    try:
                        ts = stored_msg.create_time
                        # Convert create_time (datetime or timestamp) to float timestamp
                        msg_timestamp = ts.timestamp() if isinstance(ts, datetime) else float(ts)

                        if (current_time - msg_timestamp) > expire_time_secs:
                            expired_ids.append(msg_id)
                    except Exception as ts_err:
                        logger.warning(f"[RevocationAndLogger] 清理缓存时处理时间戳错误 for msg_id {msg_id}: {ts_err}")
                        continue # Ignore messages with invalid timestamps for cleanup check

                # Remove expired messages and associated temp files
                removed_count = 0
                for msg_id in expired_ids:
                    if msg_id in self.msg_dict:
                        msg_info = self.msg_dict.pop(msg_id)
                        removed_count += 1
                        # If it was a file/media message, remove the temporary copy
                        if isinstance(msg_info, tuple):
                            _, file_path = msg_info
                            if file_path and os.path.exists(file_path):
                                try:
                                    os.remove(file_path)
                                    # logger.debug(f"[RevocationAndLogger] Deleted expired temp file: {file_path}")
                                except Exception as e:
                                    logger.error(f"[RevocationAndLogger] 删除过期临时文件失败: {file_path}, Error: {e}")
                # if removed_count > 0: logger.debug(f"[RevocationAndLogger] 清理了 {removed_count} 条过期消息缓存.")

            except Exception as e:
                logger.error(f"[RevocationAndLogger] 缓存清理任务出错: {e}")
                logger.error(f"[RevocationAndLogger] 错误详情: {traceback.format_exc()}")
            finally:
                # Schedule the next cleanup regardless of errors in this run
                interval = self.config.get("cleanup_interval", 60) # Check interval seconds
                cleanup_timer = Timer(interval, delete_out_date_msg)
                cleanup_timer.daemon = True # Allow program to exit even if timer is running
                cleanup_timer.start()

        # Start the first cleanup immediately
        logger.info("[RevocationAndLogger] 启动消息缓存清理定时器...")
        initial_timer = Timer(1, delete_out_date_msg) # Start after 1 second
        initial_timer.daemon = True
        initial_timer.start()


    def copy_to_tmp(self, file_path):
        """Copies a file to the temporary directory with a unique name."""
        try:
            if not file_path or not os.path.exists(file_path):
                logger.warning(f"[RevocationAndLogger] File not found for copying to tmp: {file_path}")
                return None

            # Create a unique name to avoid collisions in the tmp directory
            ext = os.path.splitext(file_path)[1]
            base_name = os.path.basename(file_path)
            # Add UUID part to ensure uniqueness
            unique_name = f"{os.path.splitext(base_name)[0]}_{str(uuid.uuid4())[:8]}{ext}"
            target_path = os.path.join(self.tmp_dir, unique_name)

            # Copy the file, preserving metadata
            shutil.copy2(file_path, target_path)
            # logger.debug(f"[RevocationAndLogger] Copied file to tmp: {target_path}")
            return target_path
        except Exception as e:
            logger.error(f"[RevocationAndLogger] 复制文件到tmp失败: {e}")
            return None

    def download_files(self, msg: ChatMessage):
        """
        Downloads files associated with a message if necessary.
        Checks if content is already a path, otherwise tries to download from msg.url.
        Stores downloaded files in the tmp directory.
        Updates msg.content to the local path.
        Returns the local file path or None if failed.
        """
        try:
            # 1. Check if content is already a valid local path
            file_path = msg.content
            if isinstance(file_path, str) and file_path and os.path.exists(file_path):
                # logger.debug(f"[RevocationAndLogger] File already exists locally: {file_path}")
                return file_path # Already downloaded or was local

            # 2. Check if there's a URL attribute to download from
            if hasattr(msg, 'url') and msg.url and isinstance(msg.url, str):
                logger.info(f"[RevocationAndLogger] Attempting to download file from URL: {msg.url}")
                try:
                    # Dynamically import requests only when needed
                    import requests
                except ImportError:
                    logger.error("[RevocationAndLogger] 下载文件需要 'requests' 库，请安装： pip install requests")
                    return None

                try:
                    # Perform the download with a timeout
                    response = requests.get(msg.url, timeout=self.config.get("download_timeout", 20))
                    response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)

                    # Determine file extension and name
                    # Prefer extension from URL, fallback to original content path or generate one
                    url_ext = os.path.splitext(msg.url)[1]
                    orig_path_ext = os.path.splitext(file_path if isinstance(file_path, str) else "")[1]
                    ext = url_ext or orig_path_ext or ".dat" # Default extension if none found

                    # Base name from original path if available, otherwise generate
                    orig_basename = os.path.basename(file_path) if isinstance(file_path, str) and file_path else None
                    name = orig_basename or f"dl_{str(uuid.uuid4())[:8]}{ext}"
                    safe_name = self.sanitize_filename(name)

                    # Ensure target directory exists
                    os.makedirs(self.tmp_dir, exist_ok=True)
                    target_path = os.path.join(self.tmp_dir, safe_name)

                    # Handle potential filename collisions (append counter)
                    counter=0
                    base_target = target_path
                    while os.path.exists(target_path):
                        counter+=1
                        fn, fext = os.path.splitext(base_target)
                        target_path = f"{fn}_{counter}{fext}"

                    # Write downloaded content to file
                    with open(target_path, 'wb') as f:
                        f.write(response.content)
                    logger.info(f"[RevocationAndLogger] 文件下载成功: {target_path}")

                    # IMPORTANT: Update the message content to point to the downloaded file path
                    msg.content = target_path
                    return target_path # Return the path to the downloaded file

                except requests.exceptions.RequestException as e:
                    logger.error(f"[RevocationAndLogger] 下载文件失败 (URL: {msg.url}): {e}")
                    return None
                except Exception as e:
                    logger.error(f"[RevocationAndLogger] 下载或保存文件时出错 (URL: {msg.url}): {e}")
                    return None
            else:
                # No local path and no URL provided
                # logger.warning(f"[RevocationAndLogger] No local path or URL found for message content: {msg.msg_id}")
                return None # Cannot get the file
        except Exception as e:
            logger.error(f"[RevocationAndLogger] download_files 处理异常: {e}")
            return None

    def get_user_info(self, user_id):
        """
        Attempts to get user nickname/remark using gewechat API.
        Returns the name or the original user_id if failed.
        Requires gewechat channel to be initialized.
        """
        try:
            # Check if gewechat channel and client are available
            if not self.gewechat_channel or not hasattr(self.gewechat_channel, 'client') or not self.gewechat_channel.client:
                 # logger.debug("[RevocationAndLogger] gewechat client not available for get_user_info")
                 return user_id # Return ID if client not ready

            client = self.gewechat_channel.client
            app_id = self.gewechat_channel.app_id
            if not app_id:
                # logger.debug("[RevocationAndLogger] gewechat app_id not available for get_user_info")
                return user_id # Return ID if app_id missing

            # Determine the correct API method name for compatibility
            method_name = 'getBriefInfo' if hasattr(client,'getBriefInfo') else 'get_brief_info'
            if not hasattr(client, method_name):
                 logger.warning(f"[RevocationAndLogger] gewechat client missing user info method: {method_name}")
                 return user_id # Return ID if method not found

            # Call the API
            api_method = getattr(client, method_name)
            res = api_method(app_id, [user_id]) # API expects a list of IDs

            # Parse the result
            if res and res.get('ret') == 200 and res.get('data') and isinstance(res['data'], list) and len(res['data']) > 0:
                info = res['data'][0]
                # Prefer remark name over nickname
                name = info.get('remark') or info.get('nickName')
                if name:
                    # logger.debug(f"[RevocationAndLogger] User info found for {user_id}: {name}")
                    return name
            # else: logger.debug(f"[RevocationAndLogger] Failed to get user info or empty data for {user_id}. Response: {res}")

            return user_id # Return original ID if lookup failed or no name found
        except Exception as e:
            logger.error(f"[RevocationAndLogger] 获取用户信息 API 调用失败 for {user_id}: {e}")
            return user_id # Return ID on error

    def handle_revoke(self, msg: ChatMessage, is_group=False):
         """Handles a revoke notification message."""
         logger.info(f"[RevocationAndLogger] 处理撤回消息 (Group: {is_group})...")
         old_msg_id_str = None # The ID of the message that was revoked

         # --- Extract revoked message ID ---
         # Method 1: Check dedicated attribute if framework provides it
         if hasattr(msg, 'revoked_msg_id') and msg.revoked_msg_id:
             old_msg_id_str = str(msg.revoked_msg_id)
         # Method 2: Parse from revoke message content (common in WeChat XML format)
         elif isinstance(msg.content, str):
             try:
                 # Look for <newmsgid> or <msgid> within the revoke XML/text
                 match = re.search(r"<(?:new)?msgid>(.*?)</(?:new)?msgid>", msg.content)
                 if match:
                    old_msg_id_str = str(match.group(1))
             except Exception as parse_err:
                 logger.error(f"[RevocationAndLogger] 解析撤回消息内容中的ID失败: {parse_err}")
         # --- End ID Extraction ---

         if not old_msg_id_str:
             logger.error("[RevocationAndLogger] 未能提取撤回消息的原始ID")
             return

         # --- Find original message in cache ---
         # Look up using the extracted ID
         found_msg_info = self.msg_dict.get(old_msg_id_str)
         # Fallback: Sometimes the revoke message's own ID might match the original (less common)
         if not found_msg_info and hasattr(msg, 'msg_id'):
              found_msg_info = self.msg_dict.get(str(msg.msg_id))

         if not found_msg_info:
             logger.warning(f"[RevocationAndLogger] 缓存中未找到被撤回的消息: {old_msg_id_str}")
             # TODO: Optionally notify receiver that a message was revoked but couldn't be recovered?
             return
         # --- End Cache Lookup ---

         # Unpack cached info: original message and potentially temp file path
         original_msg, tmp_file_path = found_msg_info if isinstance(found_msg_info, tuple) else (found_msg_info, None)

         # Get the configured receiver for the notification
         target = self.get_revoke_msg_receiver()
         if not target:
             logger.error("[RevocationAndLogger] 无法获取防撤回消息接收者配置")
             return

         # --- Send notification via gewechat ---
         # This part currently assumes gewechat protocol for sending
         try:
             # Check if gewechat is available and configured
             if not self.gewechat_channel or not self.gewechat_channel.client:
                 logger.error("[RevocationAndLogger] gewechat client 未初始化，无法发送撤回通知")
                 return
             client = self.gewechat_channel.client
             app_id = self.gewechat_channel.app_id
             receiver = target.get('UserName') # Get receiver ID from config
             if not client or not app_id or not receiver:
                 logger.error("[RevocationAndLogger] gewechat client, app_id 或 receiver 无效")
                 return

             # --- Construct prefix indicating who/where the revoke happened ---
             prefix = ""
             if is_group:
                 group_id = original_msg.from_user_id
                 # Get group name (use cache or API)
                 group_name, _ = self.get_group_info(group_id)
                 from_name = group_name or f"群聊({group_id})" # Fallback to ID if name not found
                 # Get the actual user who sent the original message
                 revoker_id = original_msg.actual_user_id
                 actual_name = original_msg.actual_user_nickname or (self.get_user_info(revoker_id) if revoker_id else "未知成员")
                 prefix = f"群「{from_name}」的成员「{actual_name}」"
             else: # Single chat revoke
                 sender_id = original_msg.from_user_id
                 # Get sender's name (use cache or API)
                 from_name = self.get_user_info(sender_id) # Might just return ID
                 prefix = f"好友「{from_name}」"
             # --- End Prefix Construction ---

             # --- Send based on original message type ---
             if original_msg.ctype == ContextType.TEXT:
                 # Send text content directly
                 client.post_text(app_id, receiver, f"{prefix} 撤回了一条消息:\n---\n{original_msg.content}", "")
                 logger.info(f"[RevocationAndLogger] 已向 {receiver} 发送撤回的文本消息")

             # Check if it was a file/media type AND we have the temporary file path
             elif tmp_file_path and os.path.exists(tmp_file_path) and original_msg.ctype in [ContextType.IMAGE, ContextType.VIDEO, ContextType.FILE, ContextType.VOICE]:
                 type_str_map = { ContextType.IMAGE: "图片", ContextType.VIDEO: "视频", ContextType.FILE: "文件", ContextType.VOICE: "语音" }
                 type_str = type_str_map.get(original_msg.ctype, "媒体文件")
                 # Send a preceding text message indicating the type
                 client.post_text(app_id, receiver, f"{prefix} 撤回了一个{type_str}👇", "")

                 # Attempt to send the file using the callback URL method (requires config)
                 callback_url = conf().get("gewechat_callback_url", "").rstrip('/')
                 if callback_url:
                     try:
                         # Construct the relative path for the URL
                         rel_path = os.path.relpath(tmp_file_path, os.getcwd()).replace(os.sep, '/')
                         # Ensure it's relative to the expected web server root (often starts with tmp/)
                         if not rel_path.startswith('tmp/'):
                            rel_path = 'tmp/' + os.path.basename(tmp_file_path) # Basic fallback if relpath fails
                         file_url = f"{callback_url}?file={rel_path}" # Assumes simple ?file= query param
                         logger.info(f"[RevocationAndLogger] 准备发送撤回的文件 URL: {file_url}")

                         # Use appropriate gewechat API method based on original type
                         if original_msg.ctype == ContextType.IMAGE: client.post_image(app_id, receiver, file_url)
                         elif original_msg.ctype == ContextType.VIDEO: client.post_file(app_id, receiver, file_url, os.path.basename(tmp_file_path))
                         elif original_msg.ctype == ContextType.VOICE: client.post_file(app_id, receiver, file_url, os.path.basename(tmp_file_path)) # Send voice as file
                         else: client.post_file(app_id, receiver, file_url, os.path.basename(tmp_file_path)) # Other files
                         logger.info(f"[RevocationAndLogger] 已向 {receiver} 发送撤回的 {type_str} (via URL)")

                     except Exception as send_e:
                         logger.error(f"[RevocationAndLogger] 通过回调URL发送撤回文件失败: {send_e}")
                         client.post_text(app_id, receiver, f"（无法发送被撤回的{type_str}文件，请检查回调配置或临时文件）", "")
                 else:
                     # Callback URL not configured, cannot send the file content
                     logger.error("[RevocationAndLogger] gewechat 回调URL (gewechat_callback_url) 未配置，无法发送撤回的文件内容")
                     client.post_text(app_id, receiver, f"（无法发送被撤回的{type_str}文件，回调URL未配置）", "")

             # Handle other message types (like Sharing, Card, Patpat) that were cached
             elif original_msg.ctype not in [ContextType.TEXT]:
                  type_name = original_msg.ctype.name # Get the type name string
                  # Send a simple notification about the type that was revoked
                  client.post_text(app_id, receiver, f"{prefix} 撤回了一条 {type_name} 类型的消息。", "")
                  logger.info(f"[RevocationAndLogger] 已向 {receiver} 发送类型为 {type_name} 的撤回通知")
             # --- End Sending Logic ---

         except Exception as e:
             logger.error(f"[RevocationAndLogger] 处理和发送撤回通知时发生异常: {e}")
             logger.error(f"[RevocationAndLogger] Traceback: {traceback.format_exc()}")
         # --- End Send Notification ---

    def handle_msg(self, msg: ChatMessage, is_group=False):
        """
        Handles caching of received messages for potential revoke detection.
        Also triggers the actual revoke handling if the message type is REVOKE.
        """
        try:
            # If it's a revoke message itself, trigger the revoke handler directly
            if msg.ctype == ContextType.REVOKE:
                self.handle_revoke(msg, is_group)
                return # Don't cache the revoke message itself after handling

            # --- Check message age: Only cache recent messages ---
            expire_duration = self.config.get("message_expire_time", 120)
            current_time = time.time()
            try:
                ts = msg.create_time
                msg_timestamp = ts.timestamp() if isinstance(ts, datetime) else float(ts)
                # Ignore messages older than the cache duration (prevents caching very old messages on startup)
                if msg_timestamp < (current_time - expire_duration):
                    # logger.debug(f"[RevocationAndLogger] Ignoring old message {msg.msg_id} for caching.")
                    return
            except Exception as time_err:
                logger.warning(f"[RevocationAndLogger] 无法处理消息时间戳 {msg.msg_id}: {time_err}. 跳过缓存.")
                return # Skip caching if timestamp is invalid
            # --- End Age Check ---

            msg_id_str = str(msg.msg_id) # Use string representation for dictionary key

            # --- Cache based on message type ---
            cached_data = None
            if msg.ctype == ContextType.TEXT:
                # Cache text messages directly
                cached_data = msg
                # logger.debug(f"[RevocationAndLogger] Cached TEXT message: {msg_id_str}")

            elif msg.ctype in [ContextType.IMAGE, ContextType.VIDEO, ContextType.FILE, ContextType.VOICE]:
                # For media/files, ensure we have a local copy
                local_path = self.download_files(msg) # Downloads if needed, returns path
                if not local_path:
                    logger.warning(f"[RevocationAndLogger] 无法获取文件路径，跳过缓存: {msg_id_str} ({msg.ctype.name})")
                    return

                # Copy the file to a temporary location managed by this plugin
                # This prevents the original file from being deleted by other processes before revoke handling
                tmp_path = self.copy_to_tmp(local_path)
                if not tmp_path:
                    logger.warning(f"[RevocationAndLogger] 无法复制文件到tmp，跳过缓存: {msg_id_str} ({msg.ctype.name})")
                    return

                # IMPORTANT: Create a shallow copy of the message object
                # We need to modify its 'content' to point to our temporary path (tmp_path)
                # without altering the original message object passed elsewhere in the system.
                msg_copy = copy.copy(msg) # Shallow copy is sufficient
                msg_copy.content = tmp_path # Update content in the copy to the temp file path

                # Cache the modified message copy and the path to the temp file
                cached_data = (msg_copy, tmp_path)
                # logger.debug(f"[RevocationAndLogger] Cached {msg.ctype.name} message: {msg_id_str} at {tmp_path}")

            elif msg.ctype in [ContextType.SHARING, ContextType.CARD, ContextType.PATPAT]:
                # Cache these simpler types directly
                cached_data = msg
                # logger.debug(f"[RevocationAndLogger] Cached {msg.ctype.name} message: {msg_id_str}")

            # else: logger.debug(f"[RevocationAndLogger] Message type {msg.ctype.name} not cached for revoke: {msg_id_str}")

            # --- Store in cache dictionary ---
            if cached_data:
                self.msg_dict[msg_id_str] = cached_data

                # Handle potential duplicate message IDs (e.g., gewechat's internal MsgId vs framework msg_id)
                # If raw message data is available and contains a different MsgId, link it too.
                if hasattr(msg, 'msg_data') and isinstance(msg.msg_data, dict) and 'MsgId' in msg.msg_data:
                    internal_id_str = str(msg.msg_data['MsgId'])
                    # If the internal ID is different and not already cached, link it to the same cached entry
                    if internal_id_str != msg_id_str and internal_id_str not in self.msg_dict:
                        self.msg_dict[internal_id_str] = cached_data
                        # logger.debug(f"[RevocationAndLogger] Linked internal ID {internal_id_str} to cached message {msg_id_str}")

        except Exception as e:
            logger.error(f"[RevocationAndLogger] 缓存消息失败 ({msg.msg_id if hasattr(msg, 'msg_id') else 'N/A'}): {e}")
            logger.error(f"[RevocationAndLogger] 错误详情: {traceback.format_exc()}")


    def get_group_info(self, group_id, force_refresh=False):
        """
        Gets group name using cache or gewechat API.
        Returns (group_name, member_dict). member_dict is currently always empty.
        """
        current_time = time.time()

        # 1. Check cache first (unless forced refresh)
        if not force_refresh and group_id in self.group_info_cache:
            cached_name, expiry = self.group_info_cache[group_id]
            if current_time < expiry:
                # logger.debug(f"[RevocationAndLogger] Cache hit for group info: {group_id} -> {cached_name}")
                return cached_name, {} # Return cached name, member dict not used here currently

        # 2. Cache miss, expired, or forced refresh: Fetch from API
        # logger.debug(f"[RevocationAndLogger] Cache miss or refresh for group info: {group_id}")
        group_name = None
        member_dict = {} # Placeholder for potential future member info fetching

        try:
            # Ensure gewechat client is ready
            if not self.gewechat_channel or not self.gewechat_channel.client:
                 # logger.debug("[RevocationAndLogger] gewechat client not available for get_group_info")
                 return group_id, {} # Return ID if client not ready

            client = self.gewechat_channel.client
            app_id = self.gewechat_channel.app_id
            if not app_id:
                # logger.debug("[RevocationAndLogger] gewechat app_id not available for get_group_info")
                return group_id, {}

            # Determine the correct API method name for compatibility
            method_name = 'getChatroomInfo' if hasattr(client, 'getChatroomInfo') else 'get_chatroom_info'
            if hasattr(client, method_name):
                api_method = getattr(client, method_name)
                res = api_method(app_id, group_id)

                # Parse the response
                if res and res.get('ret') == 200 and res.get('data'):
                    # Prefer nickName (often the actual group name) over remark
                    group_name = res['data'].get('nickName') or res['data'].get('remark')
                    # logger.debug(f"[RevocationAndLogger] API success for group info {group_id}: Name={group_name}")
                # else: logger.warning(f"[RevocationAndLogger] API call for group info {group_id} failed or returned no data. Response: {res}")
            else:
                 logger.warning(f"[RevocationAndLogger] gewechat client missing group info method: {method_name}")

            # Use group_id as fallback if name couldn't be fetched
            final_name = group_name or group_id
            # Update cache with fetched name (or ID fallback) and new expiry time
            self.group_info_cache[group_id] = (final_name, current_time + self.cache_expiry_time)
            # logger.debug(f"[RevocationAndLogger] Updated cache for group info: {group_id} -> {final_name}")
            return final_name, member_dict

        except Exception as e:
            logger.error(f"[RevocationAndLogger] 获取群信息 API 调用失败 for {group_id}: {e}")
            # Cache the ID as name with a shorter expiry on error to avoid repeated failures
            self.group_info_cache[group_id] = (group_id, current_time + 60) # Cache failure for 1 min
            return group_id, {} # Return ID on error

    # --- Main Event Handlers ---

    def on_receive_message(self, e_context: EventContext):
        """
        Primary handler called when a message is received.
        Differentiates between group and single messages and passes to specific handlers.
        """
        try:
            context: Context = e_context['context']
            cmsg: ChatMessage = context.get('msg')
            if not cmsg:
                # logger.debug("[RevocationAndLogger] No message object in context")
                return

            # Differentiate between group and single chat messages
            if cmsg.is_group:
                # logger.debug(f"[RevocationAndLogger] Handling group message: {cmsg.msg_id}")
                self.handle_group_msg(cmsg)
            else:
                # logger.debug(f"[RevocationAndLogger] Handling single message: {cmsg.msg_id}")
                self.handle_single_msg(cmsg)

            # Let the event pass through to other plugins/handlers
            # e_context.action = EventAction.CONTINUE # Default behavior

        except Exception as e:
            logger.error(f"[RevocationAndLogger] on_receive_message 处理失败: {e}")
            logger.error(f"[RevocationAndLogger] Traceback: {traceback.format_exc()}")

    def handle_single_msg(self, msg: ChatMessage):
        """Handles messages received in a single (private) chat."""
        # Currently, only caches messages for revoke detection
        self.handle_msg(msg, is_group=False)
        # No logging or last spoken time for single chats in this plugin

    def handle_group_msg(self, msg: ChatMessage):
        """Handles messages received in a group chat."""
        # 1. Log the message to the group's main log file (skips REVOKE type)
        self.log_group_message(msg)

        # 2. Update last spoken time (skips REVOKE type)
        if msg.ctype != ContextType.REVOKE:
            try:
                 group_id = msg.from_user_id
                 # Use consistent nickname logic
                 sender_nickname = msg.actual_user_nickname or msg.actual_user_id or "未知成员"
                 # Use consistent timestamp formatting
                 ts = msg.create_time
                 timestamp_str = datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M") \
                                if isinstance(ts, (int, float)) else \
                                ts.strftime("%Y-%m-%d %H:%M") if isinstance(ts, datetime) else \
                                datetime.now().strftime("%Y-%m-%d %H:%M") # Fallback

                 if group_id and sender_nickname: # Ensure valid ID and nickname
                     # Call the function to update the last spoken file
                     self.update_last_spoken_time(group_id, sender_nickname, timestamp_str)
                 # else: logger.warning(...) # Optional: Log if ID or nickname is missing

            except Exception as e:
                 logger.error(f"[RevocationAndLogger] 调用 update_last_spoken_time 失败: {e}")
                 # Log error but continue caching

        # 3. Cache the message for potential revoke handling (handles REVOKE type internally)
        self.handle_msg(msg, is_group=True)

    def on_handle_context(self, e_context: EventContext):
        """
        Handler called when context is being processed (e.g., for commands).
        Listens for the configured command to send last spoken info.
        """
        context: Context = e_context['context']
        msg: ChatMessage = context.get('msg')

        # Check if it's a text message in a group chat
        if context.type == ContextType.TEXT and msg and msg.is_group:
            content = context.content.strip()

            # Check if the content matches the configured command trigger
            if content == self.command_trigger:
                logger.info(f"[RevocationAndLogger] 收到命令 '{self.command_trigger}' 来自群聊 {msg.from_user_id}")

                # *** IMPORTANT: Stop further processing ***
                # Prevent other plugins/bots from handling this command
                # Prevent the default AI reply if this is a command for the bot
                e_context.action = EventAction.BREAK_PASS

                group_id = msg.from_user_id
                if not group_id:
                    logger.warning("[RevocationAndLogger] 无法获取群聊ID，无法发送最后发言文件内容")
                    reply = Reply(ReplyType.TEXT, "无法获取当前群聊ID，无法完成操作。")
                    e_context['reply'] = reply # Set reply for the framework to send
                    return

                # Construct the path to the last spoken file
                if not hasattr(self, 'last_spoken_dir') or not self.last_spoken_dir:
                     logger.error("[RevocationAndLogger] last_spoken_dir 未初始化，无法发送最后发言文件内容。")
                     reply = Reply(ReplyType.TEXT, "内部错误：无法找到最后发言记录目录。")
                     e_context['reply'] = reply
                     return

                file_name = f"{self.sanitize_filename(group_id)}-最后发言.txt"
                file_path = os.path.join(self.last_spoken_dir, file_name)

                # Check if the file exists
                if os.path.exists(file_path):
                    logger.info(f"[RevocationAndLogger] 找到最后发言文件: {file_path}，准备读取内容发送...")
                    try:
                        # Read the entire file content
                        with open(file_path, 'r', encoding='utf-8') as f:
                            file_content = f.read()

                        # Check if the file actually contains non-whitespace content
                        if file_content and file_content.strip():
                            # Create a TEXT reply with the file content
                            reply = Reply()
                            reply.type = ReplyType.TEXT
                            reply.content = file_content # Assign file string as content
                            e_context['reply'] = reply # Set reply for the framework
                            logger.info(f"[RevocationAndLogger] 已设置最后发言内容的文本回复 (来自 {file_path})")
                        else:
                            # File exists but is empty
                            logger.info(f"[RevocationAndLogger] 最后发言文件为空: {file_path}")
                            reply = Reply(ReplyType.TEXT, f"最后发言记录文件为空。")
                            e_context['reply'] = reply

                    except Exception as e:
                        # Handle errors during file reading
                        logger.error(f"[RevocationAndLogger] 读取最后发言文件失败: {file_path}, Error: {e}")
                        reply = Reply(ReplyType.TEXT, f"读取最后发言记录文件时出错，请检查日志。")
                        e_context['reply'] = reply

                else:
                    # File does not exist
                    logger.warning(f"[RevocationAndLogger] 未找到最后发言文件: {file_path}")
                    reply = Reply()
                    reply.type = ReplyType.TEXT
                    reply.content = f"当前群聊({group_id})还没有生成最后发言记录文件。"
                    e_context['reply'] = reply
                    logger.info("[RevocationAndLogger] 已设置未找到文件的文本回复")

        # If it's not the target command, do nothing and let the event continue
        return

# --- End Class RevocationAndLogger ---