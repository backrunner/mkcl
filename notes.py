from aix import generate_id
from connection import RedisConnection
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Any, Optional
import threading
from multiprocessing import cpu_count
from psycopg.rows import dict_row
from psycopg import sql

class NoteManager:
    """
    Note管理类
    """

    def __init__(self, db_connection):
        # 使用psycopg的dict_row作为row_factory
        self.db_cursor = db_connection.cursor(row_factory=dict_row)
        self.db_conn = db_connection
        # 预编译常用SQL语句
        self._prepare_statements()

    def _prepare_statements(self):
        """预编译常用SQL语句已不再使用，留空以保持兼容性"""
        # 不再使用预编译语句，避免类型推断问题
        pass

    def get_notes_list(self, start_date, end_date):
        """
        获取在一段时间内所有的note id列表
        """
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))

        # 不使用预编译语句，直接使用参数化查询
        self.db_cursor.execute(
            """SELECT id FROM note WHERE "id" < %s AND "id" > %s""",
            [end_id, start_id]
        )
        results = self.db_cursor.fetchall()
        return [result["id"] for result in results]

    def get_pinned_notes(self, note_ids):
        """
        批量获取置顶note列表
        Args:
            note_ids (list): note ID列表
        Returns:
            set: 置顶note ID集合
        """
        if not note_ids:
            return set()

        # 确保note_ids是列表类型
        note_ids = list(note_ids)
        
        # 不使用预编译语句，直接使用参数化查询
        self.db_cursor.execute(
            """SELECT "noteId" FROM user_note_pining WHERE "noteId" = ANY(%s)""",
            [note_ids]
        )
        results = self.db_cursor.fetchall()
        return {str(result["noteId"]) for result in results}

    def get_notes_batch(self, note_ids):
        """
        批量获取note信息
        Args:
            note_ids: note ID列表
        Returns:
            dict: note信息字典，如果查询失败则返回空字典
        """
        if not note_ids:
            return {}

        try:
            # 检查连接状态
            if self.db_cursor.closed:
                self.db_cursor = self.db_conn.cursor()

            # 确保 note_ids 是列表类型
            note_ids = list(note_ids)

            # 当ID数量超过阈值时，使用临时表优化
            if len(note_ids) > 100:
                # 创建临时表
                self.db_cursor.execute("CREATE TEMP TABLE temp_note_ids (id text) ON COMMIT DROP")

                # 批量插入ID到临时表
                values = [(id,) for id in note_ids]
                query = sql.SQL("INSERT INTO temp_note_ids (id) VALUES {}").format(
                    sql.SQL(',').join(sql.SQL('(%s)') for _ in values)
                )
                self.db_cursor.execute(query, [item for sublist in values for item in sublist])

                # 使用JOIN代替IN查询
                self.db_cursor.execute(
                    """
                    WITH flag_status AS (
                        SELECT DISTINCT "noteId", TRUE as is_flagged
                        FROM (
                            SELECT nr."noteId" FROM note_reaction nr
                            JOIN temp_note_ids t ON nr."noteId" = t.id
                            UNION ALL
                            SELECT nf."noteId" FROM note_favorite nf
                            JOIN temp_note_ids t ON nf."noteId" = t.id
                            UNION ALL
                            SELECT cn."noteId" FROM clip_note cn
                            JOIN temp_note_ids t ON cn."noteId" = t.id
                            UNION ALL
                            SELECT nu."noteId" FROM note_unread nu
                            JOIN temp_note_ids t ON nu."noteId" = t.id
                            UNION ALL
                            SELECT nw."noteId" FROM note_watching nw
                            JOIN temp_note_ids t ON nw."noteId" = t.id
                        ) combined_flags
                    )
                    SELECT
                        n.id,
                        n."userId",
                        n."userHost",
                        n.mentions,
                        n."renoteId",
                        n."replyId",
                        n."fileIds",
                        n."hasPoll",
                        COALESCE(f.is_flagged, FALSE) as "isFlagged"
                    FROM note n
                    JOIN temp_note_ids t ON n.id = t.id
                    LEFT JOIN flag_status f ON n.id = f."noteId"
                    """
                )
            else:
                # 对于少量ID，使用原来的IN查询
                self.db_cursor.execute(
                    """
                    WITH flag_status AS (
                        SELECT DISTINCT "noteId", TRUE as is_flagged
                        FROM (
                            SELECT "noteId" FROM note_reaction WHERE "noteId" = ANY(%s::text[])
                            UNION ALL
                            SELECT "noteId" FROM note_favorite WHERE "noteId" = ANY(%s::text[])
                            UNION ALL
                            SELECT "noteId" FROM clip_note WHERE "noteId" = ANY(%s::text[])
                            UNION ALL
                            SELECT "noteId" FROM note_unread WHERE "noteId" = ANY(%s::text[])
                            UNION ALL
                            SELECT "noteId" FROM note_watching WHERE "noteId" = ANY(%s::text[])
                        ) combined_flags
                    )
                    SELECT
                        n.id,
                        n."userId",
                        n."userHost",
                        n.mentions,
                        n."renoteId",
                        n."replyId",
                        n."fileIds",
                        n."hasPoll",
                        COALESCE(f.is_flagged, FALSE) as "isFlagged"
                    FROM note n
                    LEFT JOIN flag_status f ON n.id = f."noteId"
                    WHERE n.id = ANY(%s::text[])
                    """,
                    [note_ids, note_ids, note_ids, note_ids, note_ids, note_ids]
                )

            # 检查查询是否成功执行
            if self.db_cursor.description is None:
                return {}

            results = self.db_cursor.fetchall()
            if not results:
                return {}

            notes_info = {}
            for row in results:
                notes_info[row["id"]] = {
                    "id": row["id"],
                    "userId": row["userId"],
                    "host": row["userHost"],
                    "mentions": row["mentions"],
                    "renoteId": row["renoteId"],
                    "replyId": row["replyId"],
                    "fileIds": row["fileIds"] if row["fileIds"] is not None else [],
                    "hasPoll": row["hasPoll"],
                    "isFlagged": row["isFlagged"]
                }

            return notes_info

        except Exception:
            return {}

    def analyze_notes_batch(self, note_ids, end_id, redis_conn: RedisConnection, file_manager, batch_size=100):
        """
        批量分析note
        """
        # 获取所有相关note信息
        all_related_notes = {}
        to_process = set(note_ids)
        processed = set()

        while to_process:
            current_batch = list(to_process)[:batch_size]
            notes_info = self.get_notes_batch(current_batch)

            for note_id, info in notes_info.items():
                all_related_notes[note_id] = info
                processed.add(note_id)

                # 添加关联note到处理队列
                if info["renoteId"] and info["renoteId"] not in processed:
                    to_process.add(info["renoteId"])
                if info["replyId"] and info["replyId"] not in processed:
                    to_process.add(info["replyId"])

            to_process = to_process - processed

        # 分析处理结果
        notes_to_delete = set()
        files_to_process = set()
        all_user_ids = set()

        for note_id, note_info in all_related_notes.items():
            should_keep = (
                note_info["hasPoll"] or
                note_info["isFlagged"] or
                note_info["id"] > end_id
            )

            if not should_keep:
                notes_to_delete.add(note_id)
                files_to_process.update(note_info["fileIds"])

            all_user_ids.add(note_info["userId"])

        # 批量检查用户状态
        pipeline = redis_conn.pipeline()
        for user_id in all_user_ids:
            pipeline.hget('user_cache', user_id)

        try:
            # 执行Redis管道命令
            user_results = dict(zip(all_user_ids, redis_conn.execute(
                lambda: pipeline.execute()
            )))
        except (ConnectionError, TimeoutError):
            # 如果执行失败，重试整个批处理
            pipeline = redis_conn.pipeline()
            for user_id in all_user_ids:
                pipeline.hget('user_cache', user_id)
            user_results = dict(zip(all_user_ids, redis_conn.execute(
                lambda: pipeline.execute()
            )))

        # 处理未缓存的用户
        uncached_users = {
            user_id for user_id, info in user_results.items()
            if info is None
        }

        if uncached_users:
            # 不使用预编译语句，直接使用参数化查询
            self.db_cursor.execute(
                """SELECT id, host, "followersCount", "followingCount"
                   FROM public.user
                   WHERE id = ANY(%s)""",
                [list(uncached_users)]
            )

            pipeline = redis_conn.pipeline()
            for row in self.db_cursor.fetchall():
                user_id, host, followers, following = row["id"], row["host"], row["followersCount"], row["followingCount"]
                is_important = (host is None) or (followers + following > 0)
                user_results[user_id] = str(is_important)
                pipeline.hset('user_cache', user_id, str(is_important))
            redis_conn.execute(lambda: pipeline.execute())

        # 根据用户状态过滤需要保留的note
        for note_id in list(notes_to_delete):
            note_info = all_related_notes[note_id]
            if user_results.get(note_info["userId"]) == 'True':
                notes_to_delete.remove(note_id)

        # 处理文件
        if files_to_process:
            file_refs = file_manager.get_file_references_batch(list(files_to_process))
            files_info = file_manager.get_files_info_batch(list(files_to_process))
            used_as_avatar_banner = file_manager.check_user_avatar_banner_batch(list(files_to_process))

            pipeline = redis_conn.pipeline()
            # 批量操作文件处理结果
            files_to_keep = []
            files_to_delete = []

            for file_id in files_to_process:
                if (file_id in used_as_avatar_banner or
                    file_refs.get(file_id, 0) > 1 or
                    not files_info.get(file_id, {}).get("isLink", False)):
                    files_to_keep.append(file_id)
                else:
                    files_to_delete.append(file_id)

            # 批量添加到Redis集合
            if files_to_keep:
                pipeline.sadd('files_to_keep', *files_to_keep)
            if files_to_delete:
                pipeline.sadd('files_to_delete', *files_to_delete)

            redis_conn.execute(lambda: pipeline.execute())

        # 更新Redis - 使用批量操作
        pipeline = redis_conn.pipeline()
        if notes_to_delete:
            pipeline.sadd('notes_to_delete', *notes_to_delete)
        if note_ids:
            pipeline.srem('note_list', *note_ids)
        redis_conn.execute(lambda: pipeline.execute())

        return len(notes_to_delete)

    def delete_notes_batch(self, note_ids: list[str], batch_size: int = 1000) -> None:
        """
        批量删除note及其相关历史记录，采用分批处理方式
        Args:
            note_ids: 要删除的noteID列表
        """
        if not note_ids:
            return

        # 确保note_ids是列表类型
        note_ids = list(note_ids)

        # 先检查note_history表是否存在，添加安全检查
        try:
            # 检查游标状态，如果已关闭则重新创建
            if self.db_cursor.closed:
                self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
            
            self.db_cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = 'note_history'
                )
                """
            )
            result = self.db_cursor.fetchone()
            # 安全检查：确保查询返回了结果
            note_history_exists = result["exists"] if result and "exists" in result else False
        except Exception as e:
            print(f"检查note_history表存在性失败: {str(e)}")
            # 回滚事务并重新创建游标
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
            except Exception:
                pass
            # 默认假设表不存在，避免程序崩溃
            note_history_exists = False

        # 如果note_history表存在，先删除note_history表中的关联记录
        if note_history_exists:
            try:
                # 不使用预编译语句，直接使用参数化查询
                self.db_cursor.execute(
                    """DELETE FROM note_history nh
                       WHERE nh."targetId" = ANY(%s)""",
                    [note_ids]
                )
            except Exception as e:
                print(f"删除note_history记录失败: {str(e)}")
                # 回滚事务并重新创建游标
                try:
                    self.db_conn.rollback()
                    self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
                except Exception:
                    pass
                # 继续执行，不中断整个删除流程

        # 再删除note表中的记录
        try:
            self.db_cursor.execute(
                """DELETE FROM note WHERE id = ANY(%s)""",
                [note_ids]
            )
            self.db_conn.commit()
        except Exception as e:
            print(f"删除note记录失败: {str(e)}")
            # 回滚事务
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
            except Exception:
                pass
            # 重新抛出异常，让上层处理
            raise

    def analyze_notes_batch_parallel(self, note_ids: List[str], end_id: str,
                                   redis_conn: RedisConnection, file_manager,
                                   batch_size: int = 100,
                                   max_workers: Optional[int] = None) -> int:
        """
        并行批量分析note
        Args:
            note_ids: 要处理的note ID列表
            end_id: 结束ID
            redis_conn: Redis连接
            file_manager: 文件管理器
            batch_size: 批处理大小
            max_workers: 最大工作线程数，默认为 (CPU核心数)
        """
        # 如果未指定worker数量，则使用 CPU核心数
        if max_workers is None:
            max_workers = cpu_count()

        # 线程安全的数据结构
        self.lock = threading.Lock()
        self.all_related_notes: Dict[str, Any] = {}
        self.notes_to_delete: Set[str] = set()
        self.files_to_process: Set[str] = set()
        self.all_user_ids: Set[str] = set()

        def process_batch(batch_ids: List[str]) -> int:
            # 获取note信息和关联note
            local_related_notes = {}
            to_process = set(batch_ids)
            processed = set()

            while to_process:
                current_batch = list(to_process)[:batch_size]
                notes_info = self.get_notes_batch(current_batch)

                for note_id, info in notes_info.items():
                    local_related_notes[note_id] = info
                    processed.add(note_id)

                    # 安全地检查和添加关联note
                    if info.get("renoteId") and info["renoteId"] not in processed:
                        to_process.add(info["renoteId"])
                    if info.get("replyId") and info["replyId"] not in processed:
                        to_process.add(info["replyId"])

                to_process = to_process - processed

            # 分析处理结果
            local_notes_to_delete = set()
            local_files_to_process = set()
            local_user_ids = set()

            for note_id, note_info in local_related_notes.items():
                should_keep = (
                    note_info["hasPoll"] or
                    note_info["isFlagged"] or
                    note_info["id"] > end_id
                )

                if not should_keep:
                    local_notes_to_delete.add(note_id)
                    local_files_to_process.update(note_info["fileIds"])

                local_user_ids.add(note_info["userId"])

            # 合并结果到共享数据结构
            with self.lock:
                self.all_related_notes.update(local_related_notes)
                self.notes_to_delete.update(local_notes_to_delete)
                self.files_to_process.update(local_files_to_process)
                self.all_user_ids.update(local_user_ids)

            return len(local_notes_to_delete)

        # 将note_ids分成多个批次
        batches = [note_ids[i:i + batch_size]
                  for i in range(0, len(note_ids), batch_size)]

        total_deleted = 0
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_batch = {
                executor.submit(process_batch, batch): batch
                for batch in batches
            }

            for future in as_completed(future_to_batch):
                total_deleted += future.result()

        # 批量获取用户状态
        pipeline = redis_conn.pipeline()
        for user_id in self.all_user_ids:
            pipeline.hget('user_cache', user_id)

        try:
            # 批量执行Redis命令
            user_result_list = redis_conn.execute(lambda: pipeline.execute())
            user_results = dict(zip(self.all_user_ids, user_result_list))
        except (ConnectionError, TimeoutError):
            pipeline = redis_conn.pipeline()
            for user_id in self.all_user_ids:
                pipeline.hget('user_cache', user_id)
            user_result_list = redis_conn.execute(lambda: pipeline.execute())
            user_results = dict(zip(self.all_user_ids, user_result_list))

        # 处理未缓存的用户
        uncached_users = {
            user_id for user_id, info in user_results.items()
            if info is None
        }

        if uncached_users:
            # 不使用预编译语句，直接使用参数化查询
            self.db_cursor.execute(
                """SELECT id, host, "followersCount", "followingCount"
                   FROM public.user
                   WHERE id = ANY(%s)""",
                [list(uncached_users)]
            )

            pipeline = redis_conn.pipeline()
            for row in self.db_cursor.fetchall():
                user_id, host, followers, following = row["id"], row["host"], row["followersCount"], row["followingCount"]
                is_important = (host is None) or (followers + following > 0)
                user_results[user_id] = str(is_important)
                pipeline.hset('user_cache', user_id, str(is_important))
            redis_conn.execute(lambda: pipeline.execute())

        # 根据用户状态过滤需要保留的note
        notes_to_keep = set()
        for note_id in self.notes_to_delete:
            note_info = self.all_related_notes[note_id]
            if user_results.get(note_info["userId"]) == 'True':
                notes_to_keep.add(note_id)

        # 批量移除需要保留的note
        if notes_to_keep:
            self.notes_to_delete -= notes_to_keep

        # 处理文件
        if self.files_to_process:
            file_refs = file_manager.get_file_references_batch(
                list(self.files_to_process))
            files_info = file_manager.get_files_info_batch(
                list(self.files_to_process))
            used_as_avatar_banner = file_manager.check_user_avatar_banner_batch(
                list(self.files_to_process))

            # 批量处理文件分类
            files_to_keep = []
            files_to_delete = []

            for file_id in self.files_to_process:
                if (file_id in used_as_avatar_banner or
                    file_refs.get(file_id, 0) > 1 or
                    not files_info.get(file_id, {}).get("isLink", False)):
                    files_to_keep.append(file_id)
                else:
                    files_to_delete.append(file_id)

            # 批量更新Redis
            pipeline = redis_conn.pipeline()
            if files_to_keep:
                pipeline.sadd('files_to_keep', *files_to_keep)
            if files_to_delete:
                pipeline.sadd('files_to_delete', *files_to_delete)
            redis_conn.execute(lambda: pipeline.execute())

        # 更新Redis - 批量操作
        pipeline = redis_conn.pipeline()
        # 批量添加到notes_to_delete集合
        if self.notes_to_delete:
            pipeline.sadd('notes_to_delete', *self.notes_to_delete)
        # 批量从note_list集合移除
        if note_ids:
            pipeline.srem('note_list', *note_ids)
        redis_conn.execute(lambda: pipeline.execute())

        return len(self.notes_to_delete)
