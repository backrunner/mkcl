from aix import generate_id
from connection import RedisConnection
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Set, Any, Optional
import threading
from multiprocessing import cpu_count
from psycopg.rows import dict_row
from psycopg import sql
from tqdm import tqdm
import psycopg
import time

class NoteManager:
    """
    Note管理类
    """

    def __init__(self, db_connection, verbose=False):
        # 使用psycopg的dict_row作为row_factory
        self.db_cursor = db_connection.cursor(row_factory=dict_row)
        self.db_conn = db_connection
        self.verbose = verbose
        # 预编译常用SQL语句
        self._prepare_statements()

    def _prepare_statements(self):
        """预编译常用SQL语句已不再使用，留空以保持兼容性"""
        # 不再使用预编译语句，避免类型推断问题
        pass

    def get_notes_list(self, start_date, end_date, batch_size=10000):
        """
        获取在一段时间内所有的note id列表，使用高效的游标分页
        """
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))

        if self.verbose:
            print(f"扫描Note范围: {start_id} 到 {end_id}")

        # 首先获取总数估计
        try:
            self.db_cursor.execute(
                """SELECT reltuples::bigint as estimate
                FROM pg_class
                WHERE relname = 'note'""")
            result = self.db_cursor.fetchone()
            total_estimate = result["estimate"] if result else 0
            if self.verbose:
                print(f"Note表估计总数: {total_estimate:,}")
        except Exception as e:
            if self.verbose:
                print(f"获取表大小估计失败: {str(e)}")

        all_notes = []
        last_id = start_id
        processed_count = 0

        # 使用游标分页，避免OFFSET性能问题
        with tqdm(desc="收集Note ID", unit="个") as pbar:
            while True:
                # 使用索引友好的范围查询
                self.db_cursor.execute(
                    """SELECT id FROM note
                    WHERE id > %s AND id < %s
                    ORDER BY id
                    LIMIT %s""",
                    [last_id, end_id, batch_size]
                )

                results = self.db_cursor.fetchall()
                if not results:
                    break

                batch_notes = [result["id"] for result in results]
                all_notes.extend(batch_notes)

                # 更新进度
                processed_count += len(batch_notes)
                pbar.update(len(batch_notes))
                pbar.set_postfix({'已收集': processed_count})

                # 更新last_id为当前批次的最后一个ID
                last_id = batch_notes[-1]

                # 如果返回的结果少于batch_size，说明已经到达末尾
                if len(batch_notes) < batch_size:
                    break

        if self.verbose:
            print(f"共收集到 {len(all_notes):,} 个Note ID")

        # 添加调试信息：显示一些note ID样本
        if all_notes and self.verbose:
            print(f"Note ID样本: {all_notes[:5]}")
            # 检查ID长度和格式
            sample_id = all_notes[0]
            print(f"样本ID长度: {len(sample_id)}, 类型: {type(sample_id)}")

        return all_notes

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

            # 添加调试信息：首先检查这些note是否存在
            if self.verbose:
                try:
                    self.db_cursor.execute(
                        "SELECT id FROM note WHERE id = ANY(%s::text[])",
                        [note_ids]
                    )
                    
                    # 检查查询是否成功执行
                    if self.db_cursor.description is None:
                        if self.verbose:
                            print("调试: 初始查询未成功执行，尝试重新连接数据库")
                        # 重新创建游标并重试
                        self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
                        self.db_cursor.execute(
                            "SELECT id FROM note WHERE id = ANY(%s::text[])",
                            [note_ids]
                        )
                    
                    existing_notes = [row["id"] for row in self.db_cursor.fetchall()]

                    if not existing_notes:
                        if self.verbose:
                            print(f"调试: 批次中没有找到任何note在数据库中 {note_ids[:3]}...")
                        # 做一个更具体的测试 - 检查第一个ID是否存在
                        test_id = note_ids[0]
                        self.db_cursor.execute("SELECT COUNT(*) as count FROM note WHERE id = %s", [test_id])
                        
                        # 再次检查查询状态
                        if self.db_cursor.description is None:
                            if self.verbose:
                                print("调试: 测试查询也失败，数据库连接可能有问题")
                            return {}
                            
                        count_result = self.db_cursor.fetchone()
                        if self.verbose:
                            print(f"调试: 单独查询第一个ID '{test_id}' 的结果: {count_result['count'] if count_result else 'None'}")

                        # 检查数据库中实际存在的note样本
                        self.db_cursor.execute("SELECT id FROM note LIMIT 5")
                        if self.db_cursor.description is None:
                            if self.verbose:
                                print("调试: 样本查询失败")
                            return {}
                            
                        sample_notes = self.db_cursor.fetchall()
                        if sample_notes:
                            if self.verbose:
                                print(f"调试: 数据库中实际存在的note样本: {[row['id'] for row in sample_notes]}")
                        else:
                            if self.verbose:
                                print("调试: 数据库中没有找到任何note！")
                        return {}
                    elif len(existing_notes) != len(note_ids):
                        missing_notes = set(note_ids) - set(existing_notes)
                        if self.verbose:
                            print(f"调试: 批次中有 {len(missing_notes)} 个note不存在，继续处理现有的 {len(existing_notes)} 个")
                        
                except Exception as debug_e:
                    if self.verbose:
                        print(f"调试查询失败: {str(debug_e)}")
                    # 尝试重新连接
                    try:
                        self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
                    except Exception as reconnect_e:
                        if self.verbose:
                            print(f"重新连接失败: {str(reconnect_e)}")
                        return {}

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
                if self.verbose:
                    print("调试: 查询未成功执行，description为None")
                return {}

            results = self.db_cursor.fetchall()
            if not results:
                if self.verbose:
                    print(f"调试: 复杂查询未返回结果")
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

            if self.verbose:
                print(f"调试: 成功获取 {len(notes_info)} 个note信息")
            return notes_info

        except Exception as e:
            if self.verbose:
                print(f"调试: get_notes_batch异常: {str(e)}")
                import traceback
                print(f"调试: 异常详情: {traceback.format_exc()}")
            return {}

    def analyze_notes_batch(self, note_ids, end_id, redis_conn: RedisConnection, file_manager, batch_size=100):
        """
        批量分析note
        """
        if self.verbose:
            print(f"开始分析note批次，输入 {len(note_ids)} 个ID")
            if note_ids:
                print(f"输入的note ID样本: {note_ids[:3]}")
                print(f"end_id: {end_id}")

        # 获取所有相关note信息
        all_related_notes = {}
        to_process = set(note_ids)
        processed = set()

        # 添加安全机制防止无限循环
        max_depth = 1000  # 最大递归深度
        current_depth = 0

        while to_process and current_depth < max_depth:
            current_batch = list(to_process)[:batch_size]
            if self.verbose:
                print(f"处理批次 {current_depth + 1}，批次大小: {len(current_batch)}")

            notes_info = self.get_notes_batch(current_batch)

            # 如果批次没有返回任何数据，避免无限循环
            if not notes_info:
                if self.verbose:
                    print(f"警告: 批次 {current_batch[:5]}... 没有返回数据，跳过")
                to_process = to_process - set(current_batch)
                continue

            if self.verbose:
                print(f"批次返回 {len(notes_info)} 个note的信息")
            for note_id, info in notes_info.items():
                all_related_notes[note_id] = info
                processed.add(note_id)

                # 添加关联note到处理队列，增加安全检查
                if info.get("renoteId") and info["renoteId"] not in processed and info["renoteId"] not in to_process:
                    to_process.add(info["renoteId"])
                if info.get("replyId") and info["replyId"] not in processed and info["replyId"] not in to_process:
                    to_process.add(info["replyId"])

            to_process = to_process - processed
            current_depth += 1

            # 每10次迭代检查一次进度（降低频率）
            if current_depth % 10 == 0 and self.verbose:
                print(f"Note关联追踪深度: {current_depth}, 待处理: {len(to_process)}")

        if current_depth >= max_depth:
            if self.verbose:
                print(f"警告: Note关联追踪达到最大深度限制 ({max_depth})，可能存在循环引用")

        if self.verbose:
            print(f"关联分析完成，共收集到 {len(all_related_notes)} 个相关note")

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
        # 限制单次管道操作的大小，防止Redis超时
        max_pipeline_size = 1000
        user_id_list = list(all_user_ids)
        user_results = {}

        # 分批处理用户ID，避免单次管道操作过大
        for i in range(0, len(user_id_list), max_pipeline_size):
            batch_user_ids = user_id_list[i:i + max_pipeline_size]
            pipeline = redis_conn.pipeline()
            for user_id in batch_user_ids:
                pipeline.hget('user_cache', user_id)

            try:
                # 批量执行Redis命令
                batch_result_list = redis_conn.execute(lambda: pipeline.execute())
                # 合并结果
                user_results.update(dict(zip(batch_user_ids, batch_result_list)))
            except (ConnectionError, TimeoutError):
                # 如果执行失败，重试当前批处理
                if self.verbose:
                    print(f"Redis管道操作失败，重试批处理 (用户数: {len(batch_user_ids)})")
                pipeline = redis_conn.pipeline()
                for user_id in batch_user_ids:
                    pipeline.hget('user_cache', user_id)
                try:
                    batch_result_list = redis_conn.execute(lambda: pipeline.execute())
                    user_results.update(dict(zip(batch_user_ids, batch_result_list)))
                except Exception as e:
                    if self.verbose:
                        print(f"Redis管道重试也失败，跳过此批次: {str(e)}")
                    # 为失败的用户ID设置默认值
                    for user_id in batch_user_ids:
                        user_results[user_id] = None

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

    def _analyze_note_dependencies(self, note_ids: list[str]) -> tuple[list[str], list[str]]:
        """
        分析note的依赖关系，返回可以安全删除的顺序
        Args:
            note_ids: 要分析的note ID列表
        Returns:
            tuple: (可以安全删除的note列表, 有循环依赖的note列表)
        """
        if not note_ids:
            return [], []

        try:
            # 获取所有note的依赖关系
            self.db_cursor.execute(
                """
                SELECT id, "replyId", "renoteId"
                FROM note
                WHERE id = ANY(%s)
                """,
                [note_ids]
            )

            note_deps = {}
            for row in self.db_cursor.fetchall():
                note_id = row["id"]
                reply_id = row["replyId"]
                renote_id = row["renoteId"]

                # 只考虑在删除列表中的依赖
                deps = []
                if reply_id and reply_id in note_ids:
                    deps.append(reply_id)
                if renote_id and renote_id in note_ids:
                    deps.append(renote_id)

                note_deps[note_id] = deps

            # 使用拓扑排序找出删除顺序
            safe_order = []
            remaining = set(note_ids)
            circular_deps = []

            # 最多迭代note数量次，防止无限循环
            max_iterations = len(note_ids) + 1
            iteration = 0

            while remaining and iteration < max_iterations:
                iteration += 1
                found_deletable = False

                # 找出没有依赖或依赖已被删除的note
                for note_id in list(remaining):
                    deps = note_deps.get(note_id, [])
                    # 检查是否所有依赖都已经被标记为可删除
                    if all(dep not in remaining for dep in deps):
                        safe_order.append(note_id)
                        remaining.remove(note_id)
                        found_deletable = True

                # 如果这一轮没有找到可删除的note，说明存在循环依赖
                if not found_deletable:
                    circular_deps = list(remaining)
                    break

            return safe_order, circular_deps

        except Exception as e:
            if self.verbose:
                print(f"分析note依赖关系失败: {str(e)}")
            # 如果分析失败，返回原始列表
            return note_ids, []

    def delete_notes_batch_safe(self, note_ids: list[str], batch_size: int = 1000) -> None:
        """
        安全批量删除note，通过正确的删除顺序避免死锁
        Args:
            note_ids: 要删除的noteID列表
            batch_size: 批处理大小，默认1000
        """
        if not note_ids:
            return

        # 确保note_ids是列表类型
        note_ids = list(note_ids)

        # 死锁重试配置
        max_retries = 3
        base_delay = 0.5  # 基础延迟500ms

        for retry_count in range(max_retries):
            try:
                # 检查游标状态，如果已关闭则重新创建
                if self.db_cursor.closed:
                    self.db_cursor = self.db_conn.cursor(row_factory=dict_row)

                # 开始事务
                self.db_conn.autocommit = False

                # 先检查note_history表是否存在
                try:
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
                    note_history_exists = result["exists"] if result and "exists" in result else False
                except Exception as e:
                    if self.verbose:
                        print(f"检查note_history表存在性失败: {str(e)}")
                    note_history_exists = False

                # 步骤1: 删除note_history表中的关联记录（如果存在）
                if note_history_exists:
                    for i in range(0, len(note_ids), batch_size):
                        batch = note_ids[i:i + batch_size]
                        self.db_cursor.execute(
                            """DELETE FROM note_history WHERE "targetId" = ANY(%s)""",
                            [batch]
                        )

                # 步骤2: 分析note依赖关系
                safe_order, circular_deps = self._analyze_note_dependencies(note_ids)
                
                if self.verbose:
                    print(f"依赖分析结果: 安全删除{len(safe_order)}个, 循环依赖{len(circular_deps)}个")

                # 步骤3: 按安全顺序删除note
                if safe_order:
                    for i in range(0, len(safe_order), batch_size):
                        batch = safe_order[i:i + batch_size]
                        self.db_cursor.execute(
                            """DELETE FROM note WHERE id = ANY(%s)""",
                            [batch]
                        )

                # 步骤4: 处理循环依赖的note
                if circular_deps:
                    if self.verbose:
                        print(f"处理{len(circular_deps)}个有循环依赖的note")
                    # 使用更小的批次和逐个删除策略
                    small_batch_size = min(10, batch_size // 100)
                    
                    for i in range(0, len(circular_deps), small_batch_size):
                        batch = circular_deps[i:i + small_batch_size]
                        
                        # 首先尝试批量删除
                        try:
                            self.db_cursor.execute(
                                """DELETE FROM note WHERE id = ANY(%s)""",
                                [batch]
                            )
                        except psycopg.errors.ForeignKeyViolation:
                            # 如果批量删除失败，尝试逐个删除
                            for note_id in batch:
                                try:
                                    self.db_cursor.execute(
                                        """DELETE FROM note WHERE id = %s""",
                                        [note_id]
                                    )
                                except psycopg.errors.ForeignKeyViolation:
                                    if self.verbose:
                                        print(f"跳过有外键约束的note: {note_id}")
                                    continue

                # 提交事务
                self.db_conn.commit()
                if self.verbose:
                    print(f"成功删除note批次，共{len(note_ids)}个")
                break  # 成功完成，退出重试循环
                
            except psycopg.errors.DeadlockDetected as e:
                # 死锁检测，回滚并重试
                try:
                    self.db_conn.rollback()
                except Exception:
                    pass
                
                if retry_count < max_retries - 1:
                    # 指数退避延迟
                    delay = base_delay * (2 ** retry_count)
                    if self.verbose:
                        print(f"检测到死锁，{delay:.2f}秒后重试 ({retry_count + 1}/{max_retries})")
                    time.sleep(delay)
                    
                    # 重新创建游标
                    try:
                        self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
                    except Exception:
                        pass
                else:
                    if self.verbose:
                        print(f"删除note记录失败，已达最大重试次数: {str(e)}")
                    raise
                    
            except Exception as e:
                if self.verbose:
                    print(f"删除note记录失败: {str(e)}")
                try:
                    self.db_conn.rollback()
                    self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
                except Exception:
                    pass
                raise
            finally:
                # 确保恢复autocommit模式
                try:
                    self.db_conn.autocommit = True
                except Exception:
                    pass

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

            # 添加安全机制防止无限循环
            max_depth = 1000  # 最大递归深度
            current_depth = 0

            while to_process and current_depth < max_depth:
                current_batch = list(to_process)[:batch_size]
                notes_info = self.get_notes_batch(current_batch)

                # 如果批次没有返回任何数据，避免无限循环
                if not notes_info:
                    if self.verbose:
                        print(f"警告: 批次 {current_batch[:5]}... 没有返回数据，跳过")
                    to_process = to_process - set(current_batch)
                    continue

                for note_id, info in notes_info.items():
                    local_related_notes[note_id] = info
                    processed.add(note_id)

                    # 安全地检查和添加关联note
                    if info.get("renoteId") and info["renoteId"] not in processed and info["renoteId"] not in to_process:
                        to_process.add(info["renoteId"])
                    if info.get("replyId") and info["replyId"] not in processed and info["replyId"] not in to_process:
                        to_process.add(info["replyId"])

                to_process = to_process - processed
                current_depth += 1

                # 每100次迭代检查一次进度
                if current_depth % 100 == 0 and self.verbose:
                    print(f"并行Note关联追踪深度: {current_depth}, 待处理: {len(to_process)}")

            if current_depth >= max_depth:
                if self.verbose:
                    print(f"警告: 并行Note关联追踪达到最大深度限制 ({max_depth})，可能存在循环引用")

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
        # 限制单次管道操作的大小，防止Redis超时
        max_pipeline_size = 1000
        user_id_list = list(self.all_user_ids)
        user_results = {}

        # 分批处理用户ID，避免单次管道操作过大
        for i in range(0, len(user_id_list), max_pipeline_size):
            batch_user_ids = user_id_list[i:i + max_pipeline_size]
            pipeline = redis_conn.pipeline()
            for user_id in batch_user_ids:
                pipeline.hget('user_cache', user_id)

            try:
                # 批量执行Redis命令
                batch_result_list = redis_conn.execute(lambda: pipeline.execute())
                # 合并结果
                user_results.update(dict(zip(batch_user_ids, batch_result_list)))
            except (ConnectionError, TimeoutError):
                # 如果执行失败，重试当前批处理
                if self.verbose:
                    print(f"Redis管道操作失败，重试批处理 (用户数: {len(batch_user_ids)})")
                pipeline = redis_conn.pipeline()
                for user_id in batch_user_ids:
                    pipeline.hget('user_cache', user_id)
                try:
                    batch_result_list = redis_conn.execute(lambda: pipeline.execute())
                    user_results.update(dict(zip(batch_user_ids, batch_result_list)))
                except Exception as e:
                    if self.verbose:
                        print(f"Redis管道重试也失败，跳过此批次: {str(e)}")
                    # 为失败的用户ID设置默认值
                    for user_id in batch_user_ids:
                        user_results[user_id] = None

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

    def delete_notes_batch(self, note_ids: list[str], batch_size: int = 1000) -> None:
        """
        批量删除note及其相关历史记录，采用分批处理方式，增加死锁重试机制
        Args:
            note_ids: 要删除的noteID列表
            batch_size: 批处理大小，默认1000
        """
        if not note_ids:
            return

        # 确保note_ids是列表类型
        note_ids = list(note_ids)

        # 死锁重试配置
        max_retries = 5
        base_delay = 0.1  # 基础延迟100ms

        for retry_count in range(max_retries):
            try:
                # 检查游标状态，如果已关闭则重新创建
                if self.db_cursor.closed:
                    self.db_cursor = self.db_conn.cursor(row_factory=dict_row)

                # 先检查note_history表是否存在，添加安全检查
                try:
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
                    note_history_exists = result["exists"] if result and "exists" in result else False
                except Exception as e:
                    if self.verbose:
                        print(f"检查note_history表存在性失败: {str(e)}")
                    try:
                        self.db_conn.rollback()
                        self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
                    except Exception:
                        pass
                    note_history_exists = False

                # 开始事务
                self.db_conn.autocommit = False

                # 如果note_history表存在，先删除note_history表中的关联记录
                if note_history_exists:
                    try:
                        # 分批删除note_history记录，避免长时间锁定
                        for i in range(0, len(note_ids), batch_size):
                            batch = note_ids[i:i + batch_size]
                            self.db_cursor.execute(
                                """DELETE FROM note_history WHERE "targetId" = ANY(%s)""",
                                [batch]
                            )
                    except Exception as e:
                        if self.verbose:
                            print(f"删除note_history记录失败: {str(e)}")
                        self.db_conn.rollback()
                        raise

                # 分批删除note记录，使用更小的批次避免长时间锁定
                smaller_batch_size = min(batch_size // 4, 250)  # 使用更小的批次

                for i in range(0, len(note_ids), smaller_batch_size):
                    batch = note_ids[i:i + smaller_batch_size]

                    # 使用更安全的删除方式，避免外键约束冲突
                    # 先删除作为回复的note（没有被其他note引用的）
                    self.db_cursor.execute(
                        """DELETE FROM note
                           WHERE id = ANY(%s)
                           AND id NOT IN (
                               SELECT DISTINCT "replyId" FROM note
                               WHERE "replyId" IS NOT NULL
                               AND "replyId" = ANY(%s)
                           )
                           AND id NOT IN (
                               SELECT DISTINCT "renoteId" FROM note
                               WHERE "renoteId" IS NOT NULL
                               AND "renoteId" = ANY(%s)
                           )""",
                        [batch, batch, batch]
                    )

                # 处理剩余的有外键引用的note
                # 多次尝试删除，直到没有更多可删除的记录
                remaining_attempts = 5
                while remaining_attempts > 0:
                    self.db_cursor.execute(
                        """WITH deletable_notes AS (
                               SELECT id FROM note
                               WHERE id = ANY(%s)
                               AND id NOT IN (
                                   SELECT DISTINCT "replyId" FROM note
                                   WHERE "replyId" IS NOT NULL
                                   AND "replyId" = ANY(%s)
                                   AND "replyId" != id
                               )
                               AND id NOT IN (
                                   SELECT DISTINCT "renoteId" FROM note
                                   WHERE "renoteId" IS NOT NULL
                                   AND "renoteId" = ANY(%s)
                                   AND "renoteId" != id
                               )
                               LIMIT 100
                           )
                           DELETE FROM note
                           WHERE id IN (SELECT id FROM deletable_notes)""",
                        [note_ids, note_ids, note_ids]
                    )

                    if self.db_cursor.rowcount == 0:
                        break
                    remaining_attempts -= 1

                # 提交事务
                self.db_conn.commit()

                # 成功完成，退出重试循环
                break

            except psycopg.errors.DeadlockDetected as e:
                # 死锁检测，回滚并重试
                try:
                    self.db_conn.rollback()
                except Exception:
                    pass

                if retry_count < max_retries - 1:
                    # 指数退避延迟
                    delay = base_delay * (2 ** retry_count) + (retry_count * 0.05)
                    if self.verbose:
                        print(f"检测到死锁，{delay:.2f}秒后重试 ({retry_count + 1}/{max_retries})")
                    time.sleep(delay)

                    # 重新创建游标
                    try:
                        self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
                    except Exception:
                        pass
                else:
                    if self.verbose:
                        print(f"删除note记录失败，已达最大重试次数: {str(e)}")
                    raise

            except Exception as e:
                if self.verbose:
                    print(f"删除note记录失败: {str(e)}")
                try:
                    self.db_conn.rollback()
                    self.db_cursor = self.db_conn.cursor(row_factory=dict_row)
                except Exception:
                    pass
                raise
            finally:
                # 确保恢复autocommit模式
                try:
                    self.db_conn.autocommit = True
                except Exception:
                    pass
