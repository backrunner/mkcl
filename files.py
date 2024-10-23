from aix import generate_id

class FileManager:
    """
    文件相关操作类
    """

    def __init__(self, db_connection):
        """
        初始化数据库游标
        """
        self.db_cursor = db_connection.cursor()

    def get_file_references(self, file_id):
        """
        获取媒体文件引用数量
        """
        self.db_cursor.execute("""
            SELECT COUNT(*)
            FROM note
            WHERE %s = ANY("fileIds");
        """, [file_id])
        result = self.db_cursor.fetchone()
        return result[0]

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
            '''SELECT df."id"
            FROM drive_file df
            WHERE df."id" BETWEEN %s AND %s
              AND df."isLink" IS TRUE
              AND df."userHost" IS NOT NULL
              AND NOT EXISTS (
                SELECT 1 FROM note n WHERE df.id = ANY(n."fileIds")
              )
              AND NOT EXISTS (
                SELECT 1 FROM public.user u WHERE df.id IN (u."avatarId", u."bannerId")
              )
              ''',
            [start_id, end_id]
        )
        results = self.db_cursor.fetchall()
        return [result[0] for result in results]

    def get_single_files_new(self, start_date, end_date, redis_conn):
        """
        获取在一段时间内所有的单独文件id列表（新方法）
        """
        page = 0
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))
        print(f"{start_id}-{end_id}")

        while True:
            self.db_cursor.execute(
                '''SELECT df."id"
                FROM drive_file df
                WHERE df."id" BETWEEN %s AND %s
                  AND df."isLink" IS TRUE
                  AND df."userHost" IS NOT NULL
                  AND NOT EXISTS (
                    SELECT 1 FROM note n WHERE df.id = ANY(n."fileIds")
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM public.user u WHERE df.id IN (u."avatarId", u."bannerId")
                  )
                LIMIT 100 OFFSET %s''',
                [start_id, end_id, page * 100]
            )
            results = self.db_cursor.fetchall()
            if not results:
                break
            file_ids = [result[0] for result in results]
            count = 0
            for file_id in file_ids:
                if not redis_conn.sismember('file_cache', file_id):
                    redis_conn.sadd('files_to_delete', file_id)
                    count += 1
            page += 1
            print(f"第{page}页-{count}")

    def check_file_single(self, file_id):
        """
        判断是否为单独文件
        """
        self.db_cursor.execute(
            """
            SELECT
                CASE
                    WHEN EXISTS (SELECT 1 FROM note WHERE %s = ANY("fileIds")) THEN FALSE
                    WHEN EXISTS (SELECT 1 FROM public.user WHERE "avatarId" = %s OR "bannerId" = %s) THEN FALSE
                    ELSE TRUE
                END AS is_single
            """,
            [file_id, file_id, file_id]
        )
        result = self.db_cursor.fetchone()
        return result[0]
