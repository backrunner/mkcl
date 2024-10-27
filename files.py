from aix import generate_id
from connection import RedisConnection

class FileManager:
    """
    文件相关操作类
    """

    def __init__(self, db_connection):
        """
        初始化数据库游标
        """
        self.db_cursor = db_connection.cursor()
        self.db_conn = db_connection

    def get_file_references(self, file_id):
        """
        获取媒体文件引用数量
        """
        self.db_cursor.execute("""SELECT id FROM note WHERE %s = ANY("fileIds");""", [file_id])
        results = self.db_cursor.fetchall()
        return len(results)

    def is_file_local(self, file_id):
        """
        判断是否为本地存储文件
        """
        self.db_cursor.execute("""SELECT "isLink" FROM drive_file WHERE id = %s;""", [file_id])
        result = self.db_cursor.fetchone()
        return result[0]

    def get_single_files(self, start_date, end_date):
        """
        获取在一段时间内所有的单独文件id列表
        """
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))
        print(f"{start_id}-{end_id}")
        self.db_cursor.execute(
            '''SELECT drive_file."id" FROM drive_file
            LEFT JOIN note ON drive_file.id = ANY(note."fileIds")
            LEFT JOIN public.user ON drive_file.id = public.user."avatarId" OR drive_file.id = public.user."bannerId"
            WHERE drive_file."id" < %s AND drive_file."id" > %s AND drive_file."isLink" IS TRUE
            AND drive_file."userHost" IS NOT NULL AND note."id" IS NULL AND public.user."id" IS NULL''',
            [end_id, start_id]
        )
        results = self.db_cursor.fetchall()
        return [result[0] for result in results]

    def get_single_files_new(self, start_date, end_date, redis_conn: RedisConnection):
        """
        获取在一段时间内所有的单独文件id列表（新方法）
        """
        page = 0
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))
        print(f"{start_id}-{end_id}")

        while True:
            count = 0
            self.db_cursor.execute(
                '''SELECT drive_file."id" FROM drive_file
                WHERE drive_file."id" BETWEEN %s AND %s AND drive_file."isLink" IS TRUE
                AND drive_file."userHost" IS NOT NULL LIMIT 100 OFFSET %s''',
                [start_id, end_id, page * 100]
            )
            results = self.db_cursor.fetchall()
            if not results:
                break

            file_ids = [result[0] for result in results]
            for file_id in file_ids:
                # 使用 execute 方法包装 Redis 操作
                if redis_conn.execute(lambda: redis_conn.client.sismember('file_cache', file_id)):
                    continue
                if self.check_file_single(file_id):
                    redis_conn.execute(
                        lambda: redis_conn.client.sadd('files_to_delete', file_id)
                    )
                    count += 1
            page += 1
            print(f"第{page}页-{count}")

    def check_file_single(self, file_id):
        """
        判断是否为单独文件
        """
        if self.get_file_references(file_id) > 0:
            return False
        self.db_cursor.execute(
            """SELECT "id" FROM public.user WHERE public.user."avatarId" = %s OR public.user."bannerId" = %s LIMIT 1;""",
            [file_id, file_id]
        )
        results = self.db_cursor.fetchall()
        return len(results) == 0

    def get_file_references_batch(self, file_ids):
        """
        批量获取文件引用数
        """
        if not file_ids:
            return {}

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
        return dict(self.db_cursor.fetchall())

    def get_files_info_batch(self, file_ids):
        """
        批量获取文件信息
        """
        if not file_ids:
            return {}

        self.db_cursor.execute(
            """
            SELECT id, "isLink", "userHost"
            FROM drive_file
            WHERE id = ANY(%s)
            """,
            [file_ids]
        )
        return {row[0]: {"isLink": row[1], "userHost": row[2]}
                for row in self.db_cursor.fetchall()}

    def check_user_avatar_banner_batch(self, file_ids):
        """
        批量检查文件是否被用作头像或横幅
        """
        if not file_ids:
            return set()

        self.db_cursor.execute(
            """
            SELECT DISTINCT "avatarId", "bannerId"
            FROM public.user
            WHERE "avatarId" = ANY(%s) OR "bannerId" = ANY(%s)
            """,
            [file_ids, file_ids]
        )
        used_files = set()
        for avatar_id, banner_id in self.db_cursor.fetchall():
            if avatar_id:
                used_files.add(avatar_id)
            if banner_id:
                used_files.add(banner_id)
        return used_files

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