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
                WHERE %s = ANY("fileIds");
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
                WHERE id = %s;
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
        self.db_cursor.execute(
            '''SELECT COUNT(*) FROM drive_file
            WHERE id BETWEEN %s AND %s
            AND "isLink" IS TRUE
            AND "userHost" IS NOT NULL''',
            [start_id, end_id]
        )
        total_count = self.db_cursor.fetchone()[0]

        processed = 0
        deleted = 0
        batch_size = 1000  # 增加批处理大小
        last_id = start_id

        with tqdm(total=total_count, desc="扫描单独文件") as pbar:
            while processed < total_count:
                self.db_cursor.execute(
                    '''SELECT id
                    FROM drive_file
                    WHERE id >= %s
                    AND id <= %s
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
                uncached_file_ids = [
                    fid for fid in file_ids
                    if not redis_conn.execute(lambda: redis_conn.client.sismember('file_cache', fid))
                ]

                if uncached_file_ids:
                    # 批量获取文件引用
                    file_refs = self.get_file_references_batch(uncached_file_ids)
                    # 批量检查头像和横幅使用
                    avatar_banner_files = self.check_user_avatar_banner_batch(uncached_file_ids)

                    # 找出单独文件
                    for file_id in uncached_file_ids:
                        if file_refs.get(file_id, 0) == 0 and file_id not in avatar_banner_files:
                            redis_conn.execute(
                                lambda: redis_conn.client.sadd('files_to_delete', file_id)
                            )
                            deleted += 1

                current_batch_size = len(file_ids)
                processed += current_batch_size

                pbar.update(current_batch_size)
                pbar.set_postfix({
                    '已处理': processed,
                    '待删除': deleted
                })

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

            self.db_cursor.execute(
                """
                WITH file_refs AS (
                    SELECT unnest(n."fileIds") as file_id, count(*) as ref_count
                    FROM note n
                    WHERE n."fileIds" && %s
                    GROUP BY unnest(n."fileIds")
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
            # 尝试重新初始化连接
            try:
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            return {}

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
                WHERE id = ANY(%s)
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
            # 尝试重新初始化连接
            try:
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            return {}

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
                WHERE "avatarId" = ANY(%s) OR "bannerId" = ANY(%s)
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
            # 尝试重新初始化连接
            try:
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            return set()

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
