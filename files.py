from aix import generate_id
from typing import Dict, Set
from psycopg.rows import dict_row

class FileManager:
    """
    文件相关操作类
    """

    def __init__(self, db_connection):
        """
        初始化数据库游标
        """
        self.db_connection = db_connection
        self.db_cursor = db_connection.cursor(row_factory=dict_row)
        self.file_references_cache: Dict[str, int] = {}
        self.file_local_cache: Dict[str, bool] = {}

    def preload_file_info(self, file_ids: Set[str]):
        """
        预加载文件引用计数和本地存储信息 - 优化版本
        使用单次查询获取所需数据
        """
        file_ids_list = list(file_ids)

        # 使用单次查询同时获取引用计数和存储信息
        self.db_cursor.execute("""
            WITH file_refs AS (
                SELECT file_id, COUNT(*) as ref_count
                FROM note, unnest("fileIds") as file_id
                WHERE file_id = ANY(%s)
                GROUP BY file_id
            )
            SELECT
                df.id,
                df."isLink",
                COALESCE(fr.ref_count, 0) as ref_count
            FROM drive_file df
            LEFT JOIN file_refs fr ON fr.file_id = df.id
            WHERE df.id = ANY(%s)
        """, (file_ids_list, file_ids_list,))

        for row in self.db_cursor.fetchall():
            self.file_references_cache[row['id']] = row['ref_count']
            self.file_local_cache[row['id']] = row['isLink']

    def get_file_references(self, file_id: str) -> int:
        """
        获取媒体文件引用数量
        """
        return self.file_references_cache.get(file_id, 0)

    def is_file_local(self, file_id: str) -> bool:
        """
        判断是否为本地存储文件
        """
        return self.file_local_cache.get(file_id, False)

    def get_single_files(self, start_date, end_date):
        """
        获取在一段时间内所有的单独文件id列表 - 优化版本
        添加索引建议：
        CREATE INDEX IF NOT EXISTS idx_drive_file_id_islink_userhost ON drive_file(id) WHERE "isLink" IS TRUE AND "userHost" IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_note_fileids ON note USING GIN ("fileIds");
        CREATE INDEX IF NOT EXISTS idx_user_avatar_banner ON public.user("avatarId", "bannerId");
        """
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))

        self.db_cursor.execute(
            '''WITH file_candidates AS (
                SELECT df."id"
                FROM drive_file df
                WHERE df."id" BETWEEN %s AND %s
                  AND df."isLink" IS TRUE
                  AND df."userHost" IS NOT NULL
            )
            SELECT fc."id"
            FROM file_candidates fc
            WHERE NOT EXISTS (
                SELECT 1 FROM note n WHERE fc.id = ANY(n."fileIds")
            )
            AND NOT EXISTS (
                SELECT 1 FROM public.user u WHERE fc.id IN (u."avatarId", u."bannerId")
            )''',
            [start_id, end_id]
        )
        return [result['id'] for result in self.db_cursor.fetchall()]

    def get_single_files_new(self, start_date, end_date, redis_conn):
        """
        获取在一段时间内所有的单独文件id列表
        使用游标进行批处理
        """
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))

        self.db_cursor.execute(
            '''DECLARE file_cursor CURSOR FOR
            WITH file_candidates AS (
                SELECT df."id"
                FROM drive_file df
                WHERE df."id" BETWEEN %s AND %s
                  AND df."isLink" IS TRUE
                  AND df."userHost" IS NOT NULL
            )
            SELECT fc."id"
            FROM file_candidates fc
            WHERE NOT EXISTS (
                SELECT 1 FROM note n WHERE fc.id = ANY(n."fileIds")
            )
            AND NOT EXISTS (
                SELECT 1 FROM public.user u WHERE fc.id IN (u."avatarId", u."bannerId")
            )''',
            [start_id, end_id]
        )

        batch_size = 1000
        while True:
            self.db_cursor.execute("FETCH %s FROM file_cursor", (batch_size,))
            results = self.db_cursor.fetchall()
            if not results:
                break

            file_ids = [result['id'] for result in results]
            pipe = redis_conn.pipeline()
            for file_id in file_ids:
                pipe.sismember('file_cache', file_id)
            cache_results = pipe.execute()

            to_delete = [
                file_id for file_id, in_cache in zip(file_ids, cache_results)
                if not in_cache
            ]
            if to_delete:
                redis_conn.sadd('files_to_delete', *to_delete)
                print(f"添加了 {len(to_delete)} 个文件到删除队列")

        self.db_cursor.execute("CLOSE file_cursor")

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
