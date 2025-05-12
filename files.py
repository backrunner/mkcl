from aix import generate_id
from connection import RedisConnection
from tqdm import tqdm

class FileManager:
    """
    文件相关操作类
    """

    def __init__(self, db_connection):
        """
        初始化数据库游标
        """
        try:
            self.db_conn = db_connection
            self.db_cursor = db_connection.cursor()
        except Exception as e:
            print(f"初始化文件管理器失败: {str(e)}")
            raise

    def get_file_references(self, file_id):
        """
        获取媒体文件引用数量
        """
        try:
            if not file_id:
                return 0

            self.db_cursor.execute("""
                SELECT COUNT(*)
                FROM note
                WHERE %s::text = ANY("fileIds");
            """, [file_id])

            result = self.db_cursor.fetchone()
            return result[0] if result else 0

        except Exception as e:
            print(f"获取文件引用数量失败 (file_id: {file_id}): {str(e)}")
            return 0

    def is_file_local(self, file_id):
        """
        判断是否为本地存储文件
        """
        try:
            if not file_id:
                return False

            self.db_cursor.execute("""
                SELECT "isLink"
                FROM drive_file
                WHERE id = %s::text;
            """, [file_id])

            result = self.db_cursor.fetchone()
            return result[0] if result and result[0] is not None else False

        except Exception as e:
            print(f"检查文件是否本地存储失败 (file_id: {file_id}): {str(e)}")
            return False

    def get_single_files(self, start_date, end_date, redis_conn: RedisConnection):
        """
        获取在一段时间内所有的单独文件id列表，使用批量查询优化
        """
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))
        print(f"扫描文件范围: {start_id}-{end_id}")

        # 首先获取总数
        try:
            self.db_cursor.execute(
                '''SELECT COUNT(*) FROM drive_file
                WHERE id BETWEEN %s::text AND %s::text
                AND "isLink" IS TRUE
                AND "userHost" IS NOT NULL''',
                [start_id, end_id]
            )
            total_count = self.db_cursor.fetchone()[0]
        except Exception as e:
            print(f"获取文件总数失败: {str(e)}")
            # 回滚事务并重新连接
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor()
            except Exception:
                pass
            return

        processed = 0
        deleted = 0
        batch_size = 1000  # 增加批处理大小
        last_id = start_id
        max_retries = 3  # 最大重试次数

        with tqdm(total=total_count, desc="扫描单独文件") as pbar:
            while processed < total_count:
                try:
                    self.db_cursor.execute(
                        '''SELECT id
                        FROM drive_file
                        WHERE id >= %s::text
                        AND id <= %s::text
                        AND "isLink" IS TRUE
                        AND "userHost" IS NOT NULL
                        ORDER BY id
                        LIMIT %s''',
                        [last_id, end_id, batch_size]
                    )
                    results = self.db_cursor.fetchall()
                    if not results:
                        break

                    file_ids = [result[0] for result in results]
                    last_id = file_ids[-1]

                    # 过滤掉已缓存的文件ID
                    uncached_file_ids = []
                    for fid in file_ids:
                        try:
                            is_cached = redis_conn.execute(
                                lambda: redis_conn.client.sismember('file_cache', fid)
                            )
                            if not is_cached:
                                uncached_file_ids.append(fid)
                        except Exception as e:
                            print(f"检查文件缓存状态失败 (file_id: {fid}): {str(e)}")
                            # 保守处理，将未能确认的ID添加到处理列表
                            uncached_file_ids.append(fid)

                    if uncached_file_ids:
                        # 批量获取文件引用
                        retry_count = 0
                        file_refs = {}
                        avatar_banner_files = set()
                        
                        while retry_count < max_retries:
                            try:
                                # 确保每次重试前回滚任何失败的事务
                                self.db_conn.rollback()
                                
                                # 依次处理每个查询，确保在错误时可以继续流程
                                try:
                                    file_refs = self.get_file_references_batch(uncached_file_ids)
                                except Exception as e:
                                    print(f"批量获取文件引用数失败: {str(e)}")
                                    self.db_conn.rollback()
                                    # 但不中断循环，继续尝试其他查询
                                
                                try:
                                    # 批量检查头像和横幅使用
                                    avatar_banner_files = self.check_user_avatar_banner_batch(uncached_file_ids)
                                except Exception as e:
                                    print(f"批量检查用户头像和横幅失败: {str(e)}")
                                    self.db_conn.rollback()
                                
                                # 如果至少有一个查询成功，继续处理
                                break
                            except Exception as e:
                                retry_count += 1
                                if retry_count >= max_retries:
                                    print(f"获取文件信息失败，已达最大重试次数: {str(e)}")
                                    # 继续处理下一批
                                else:
                                    print(f"获取文件信息失败，重试 {retry_count}/{max_retries}: {str(e)}")
                                    # 等待短暂时间后重试
                                    import time
                                    time.sleep(0.5)
                                
                                # 回滚任何失败的事务并重新创建游标
                                try:
                                    self.db_conn.rollback()
                                    self.db_cursor = self.db_conn.cursor()
                                except Exception:
                                    pass

                        # 找出单独文件
                        for file_id in uncached_file_ids:
                            if file_refs.get(file_id, 0) == 0 and file_id not in avatar_banner_files:
                                try:
                                    redis_conn.execute(
                                        lambda: redis_conn.client.sadd('files_to_delete', file_id)
                                    )
                                    deleted += 1
                                except Exception as e:
                                    print(f"添加文件到删除集合失败 (file_id: {file_id}): {str(e)}")

                    current_batch_size = len(file_ids)
                    processed += current_batch_size

                    pbar.update(current_batch_size)
                    pbar.set_postfix({
                        '已处理': processed,
                        '待删除': deleted
                    })
                except Exception as e:
                    print(f"处理文件批次失败，跳过当前批次: {str(e)}")
                    # 移动到下一批，避免卡在同一位置
                    try:
                        # 回滚事务
                        self.db_conn.rollback()
                        # 尝试重新初始化连接
                        self.db_cursor = self.db_conn.cursor()
                        # 增加last_id，避免无限循环
                        last_id_increment = int(last_id, 16) + 1 if last_id else 0
                        last_id = format(last_id_increment, 'x')
                    except Exception as conn_error:
                        print(f"重新初始化连接失败: {str(conn_error)}")
                        # 如果连接恢复失败，退出循环
                        break

        print(f"\n找到 {deleted} 个单独文件需要删除")

    def get_file_references_batch(self, file_ids):
        """
        批量获取文件引用数
        """
        if not file_ids:
            return {}

        try:
            # 确保 file_ids 是列表类型
            file_ids = list(file_ids)

            # 修复：使用明确的类型转换并改进查询
            self.db_cursor.execute(
                """
                WITH file_refs AS (
                    SELECT unnest(n."fileIds"::text[]) as file_id, count(*) as ref_count
                    FROM note n
                    WHERE n."fileIds"::text[] && %s::text[]
                    GROUP BY unnest(n."fileIds"::text[])
                )
                SELECT f.id, COALESCE(fr.ref_count, 0) as ref_count
                FROM unnest(%s::text[]) as f(id)
                LEFT JOIN file_refs fr ON fr.file_id = f.id
                """,
                [file_ids, file_ids]
            )

            # 检查游标是否有效
            if self.db_cursor is None or not hasattr(self.db_cursor, 'fetchall'):
                print("警告：数据库游标无效")
                return {}

            results = self.db_cursor.fetchall()
            if not results:
                return {}

            return dict(results)

        except Exception as e:
            print(f"批量获取文件引用数失败: {str(e)}")
            # 回滚事务并重新初始化连接
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            raise  # 重新抛出异常，让上层处理

    def get_files_info_batch(self, file_ids):
        """
        批量获取文件信息
        """
        if not file_ids:
            return {}

        try:
            # 确保 file_ids 是列表类型
            file_ids = list(file_ids)

            self.db_cursor.execute(
                """
                SELECT id, "isLink", "userHost"
                FROM drive_file
                WHERE id = ANY(%s::text[])
                """,
                [file_ids]
            )

            # 检查游标是否有效
            if self.db_cursor is None or not hasattr(self.db_cursor, 'fetchall'):
                print("警告：数据库游标无效")
                return {}

            results = self.db_cursor.fetchall()
            if not results:
                return {}

            return {
                row[0]: {
                    "isLink": row[1] if row[1] is not None else False,
                    "userHost": row[2]
                }
                for row in results
            }

        except Exception as e:
            print(f"批量获取文件信息失败: {str(e)}")
            # 回滚事务并重新初始化连接
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            raise  # 重新抛出异常，让上层处理

    def check_user_avatar_banner_batch(self, file_ids):
        """
        批量检查文件是否被用作头像或横幅
        """
        if not file_ids:
            return set()

        try:
            # 确保 file_ids 是列表类型
            file_ids = list(file_ids)

            self.db_cursor.execute(
                """
                SELECT DISTINCT "avatarId", "bannerId"
                FROM public.user
                WHERE "avatarId" = ANY(%s::text[]) OR "bannerId" = ANY(%s::text[])
                """,
                [file_ids, file_ids]
            )

            # 检查游标是否有效
            if self.db_cursor is None or not hasattr(self.db_cursor, 'fetchall'):
                print("警告：数据库游标无效")
                return set()

            results = self.db_cursor.fetchall()
            if not results:
                return set()

            used_files = set()
            for avatar_id, banner_id in results:
                if avatar_id:
                    used_files.add(avatar_id)
                if banner_id:
                    used_files.add(banner_id)
            return used_files

        except Exception as e:
            print(f"批量检查用户头像和横幅失败: {str(e)}")
            # 回滚事务并重新初始化连接
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            raise  # 重新抛出异常，让上层处理

    def delete_files_batch(self, file_ids: list[str], batch_size: int = 1000) -> None:
        """
        批量删除文件，采用分批处理方式
        Args:
            file_ids: 要删除的文件ID列表
        """
        if not file_ids:
            return

        # 使用 WITH 和 JOIN 来优化删除操作
        self.db_cursor.execute(
            """
            WITH batch_ids AS (
                SELECT unnest(%s::text[]) AS id
            )
            DELETE FROM drive_file df
            USING batch_ids b
            WHERE df.id = b.id
            """,
            [file_ids]
        )

        self.db_conn.commit()
