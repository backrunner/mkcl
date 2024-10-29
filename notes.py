from aix import generate_id
from connection import RedisConnection
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Any, Optional
import threading
from multiprocessing import cpu_count


class NoteManager:
    """
    笔记管理类
    """

    def __init__(self, db_connection):
        self.db_cursor = db_connection.cursor()
        self.db_conn = db_connection

    def get_notes_list(self, start_date, end_date):
        """
        获取在一段时间内所有的笔记id列表
        """
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))
        self.db_cursor.execute(
            """SELECT id FROM note WHERE "id" < %s AND "id" > %s;""", [end_id, start_id])
        results = self.db_cursor.fetchall()
        return [result[0] for result in results]

    def get_pinned_notes(self, note_ids):
        """
        批量获取置顶帖子列表
        Args:
            note_ids (list): 帖子ID列表
        Returns:
            set: 置顶帖子ID集合
        """
        if not note_ids:
            return set()

        # PostgreSQL中使用 = ANY 语法替代 IN，这样可以正确处理参数化查询
        query = """
            SELECT "noteId"
            FROM user_note_pining
            WHERE "noteId" = ANY(%s)
        """
        # 将note_ids列表作为一个数组参数传递
        self.db_cursor.execute(query, (note_ids,))
        results = self.db_cursor.fetchall()
        return {str(result[0]) for result in results}

    def get_notes_batch(self, note_ids):
        """
        批量获取帖子信息
        Args:
            note_ids: 帖子ID列表
        Returns:
            dict: 帖子信息字典，如果查询失败则返回空字典
        """
        if not note_ids:
            return {}

        try:
            # 检查连接状态
            if self.db_cursor.closed:
                self.db_cursor = self.db_conn.cursor()
            
            # 确保 note_ids 是列表类型
            note_ids = list(note_ids)
            
            # 使用CTE优化查询
            self.db_cursor.execute(
                """
                WITH flag_status AS (
                    SELECT DISTINCT "noteId", TRUE as is_flagged
                    FROM (
                        SELECT "noteId" FROM note_reaction WHERE "noteId" = ANY(%s)
                        UNION ALL
                        SELECT "noteId" FROM note_favorite WHERE "noteId" = ANY(%s)
                        UNION ALL
                        SELECT "noteId" FROM clip_note WHERE "noteId" = ANY(%s)
                        UNION ALL
                        SELECT "noteId" FROM note_unread WHERE "noteId" = ANY(%s)
                        UNION ALL
                        SELECT "noteId" FROM note_watching WHERE "noteId" = ANY(%s)
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
                WHERE n.id = ANY(%s)
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
                notes_info[row[0]] = {
                    "id": row[0],
                    "userId": row[1],
                    "host": row[2],
                    "mentions": row[3],
                    "renoteId": row[4],
                    "replyId": row[5],
                    "fileIds": row[6] if row[6] is not None else [],
                    "hasPoll": row[7],
                    "isFlagged": row[8]
                }

            return notes_info

        except Exception:
            return {}

    def analyze_notes_batch(self, note_ids, end_id, redis_conn: RedisConnection, file_manager, batch_size=100):
        """
        批量分析帖子
        """
        # 获取所有相关帖子信息
        all_related_notes = {}
        to_process = set(note_ids)
        processed = set()

        while to_process:
            current_batch = list(to_process)[:batch_size]
            notes_info = self.get_notes_batch(current_batch)

            for note_id, info in notes_info.items():
                all_related_notes[note_id] = info
                processed.add(note_id)

                # 添加关联帖子到处理队列
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
            self.db_cursor.execute(
                """
                SELECT id, host, "followersCount", "followingCount"
                FROM public.user
                WHERE id = ANY(%s)
                """,
                [list(uncached_users)]
            )

            pipeline = redis_conn.pipeline()
            for user_id, host, followers, following in self.db_cursor.fetchall():
                is_important = (host is None) or (followers + following > 0)
                user_results[user_id] = str(is_important)
                pipeline.hset('user_cache', user_id, str(is_important))
            pipeline.execute()

        # 根据用户状态过滤需要保留的帖子
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
            for file_id in files_to_process:
                if (file_id in used_as_avatar_banner or
                    file_refs.get(file_id, 0) > 1 or
                    not files_info.get(file_id, {}).get("isLink", False)):
                    pipeline.sadd('files_to_keep', file_id)
                else:
                    pipeline.sadd('files_to_delete', file_id)

            redis_conn.execute(lambda: pipeline.execute())

        # 更新Redis
        pipeline = redis_conn.pipeline()
        for note_id in notes_to_delete:
            pipeline.sadd('notes_to_delete', note_id)
        for note_id in note_ids:
            pipeline.srem('note_list', note_id)
        redis_conn.execute(lambda: pipeline.execute())

        return len(notes_to_delete)

    def delete_notes_batch(self, note_ids: list[str], batch_size: int = 1000) -> None:
        """
        批量删除帖子，采用分批处理方式
        Args:
            note_ids: 要删除的帖子ID列表
        """
        if not note_ids:
            return

        # 使用 WITH 和 JOIN 来优化删除操作
        self.db_cursor.execute(
            """
            WITH batch_ids AS (
                SELECT unnest(%s::text[]) AS id
            )
            DELETE FROM note n
            USING batch_ids b
            WHERE n.id = b.id
            """,
            [note_ids]
        )

        self.db_conn.commit()

    def analyze_notes_batch_parallel(self, note_ids: List[str], end_id: str,
                                   redis_conn: RedisConnection, file_manager,
                                   batch_size: int = 100,
                                   max_workers: Optional[int] = None) -> int:
        """
        并行批量分析帖子
        Args:
            note_ids: 要处理的帖子ID列表
            end_id: 结束ID
            redis_conn: Redis连接
            file_manager: 文件管理器
            batch_size: 批处理大小
            max_workers: 最大工作线程数，默认为 (CPU核心数 * 2)
        """
        # 如果未指定worker数量，则使用 CPU核心数 * 2
        if max_workers is None:
            max_workers = cpu_count() * 2

        # 线程安全的数据结构
        self.lock = threading.Lock()
        self.all_related_notes: Dict[str, Any] = {}
        self.notes_to_delete: Set[str] = set()
        self.files_to_process: Set[str] = set()
        self.all_user_ids: Set[str] = set()

        def process_batch(batch_ids: List[str]) -> int:
            # 获取帖子信息和关联帖子
            local_related_notes = {}
            to_process = set(batch_ids)
            processed = set()

            while to_process:
                current_batch = list(to_process)[:batch_size]
                notes_info = self.get_notes_batch(current_batch)

                for note_id, info in notes_info.items():
                    local_related_notes[note_id] = info
                    processed.add(note_id)

                    # 安全地检查和添加关联笔记
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

        # 处理用户状态
        pipeline = redis_conn.pipeline()
        for user_id in self.all_user_ids:
            pipeline.hget('user_cache', user_id)

        try:
            user_results = dict(zip(self.all_user_ids, redis_conn.execute(
                lambda: pipeline.execute()
            )))
        except (ConnectionError, TimeoutError):
            pipeline = redis_conn.pipeline()
            for user_id in self.all_user_ids:
                pipeline.hget('user_cache', user_id)
            user_results = dict(zip(self.all_user_ids, redis_conn.execute(
                lambda: pipeline.execute()
            )))

        # 处理未缓存的用户
        uncached_users = {
            user_id for user_id, info in user_results.items()
            if info is None
        }

        if uncached_users:
            self.db_cursor.execute(
                """
                SELECT id, host, "followersCount", "followingCount"
                FROM public.user
                WHERE id = ANY(%s)
                """,
                [list(uncached_users)]
            )

            pipeline = redis_conn.pipeline()
            for user_id, host, followers, following in self.db_cursor.fetchall():
                is_important = (host is None) or (followers + following > 0)
                user_results[user_id] = str(is_important)
                pipeline.hset('user_cache', user_id, str(is_important))
            pipeline.execute()

        # 根据用户状态过滤需要保留的帖子
        for note_id in list(self.notes_to_delete):
            note_info = self.all_related_notes[note_id]
            if user_results.get(note_info["userId"]) == 'True':
                self.notes_to_delete.remove(note_id)

        # 处理文件
        if self.files_to_process:
            file_refs = file_manager.get_file_references_batch(
                list(self.files_to_process))
            files_info = file_manager.get_files_info_batch(
                list(self.files_to_process))
            used_as_avatar_banner = file_manager.check_user_avatar_banner_batch(
                list(self.files_to_process))

            pipeline = redis_conn.pipeline()
            for file_id in self.files_to_process:
                if (file_id in used_as_avatar_banner or
                    file_refs.get(file_id, 0) > 1 or
                    not files_info.get(file_id, {}).get("isLink", False)):
                    pipeline.sadd('files_to_keep', file_id)
                else:
                    pipeline.sadd('files_to_delete', file_id)

            redis_conn.execute(lambda: pipeline.execute())

        # 更新Redis
        pipeline = redis_conn.pipeline()
        for note_id in self.notes_to_delete:
            pipeline.sadd('notes_to_delete', note_id)
        for note_id in note_ids:
            pipeline.srem('note_list', note_id)
        redis_conn.execute(lambda: pipeline.execute())

        return len(self.notes_to_delete)
