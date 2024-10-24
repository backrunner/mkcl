from aix import generate_id

class NoteManager:
    """
    笔记管理类
    """

    def __init__(self, db_connection):
        """
        初始化数据库游标
        """
        self.db_cursor = db_connection.cursor()

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

    def get_note_info(self, note_id):
        """
        获取单个笔记的相关信息
        """
        self.db_cursor.execute("""
            SELECT n."userId", n."userHost", n.mentions, n."renoteId", n."replyId", n."fileIds", n."hasPoll",
                   EXISTS (
                       SELECT 1
                       FROM (
                           SELECT "noteId" FROM note_reaction WHERE "noteId" = %(note_id)s
                           UNION ALL
                           SELECT "noteId" FROM note_favorite WHERE "noteId" = %(note_id)s
                           UNION ALL
                           SELECT "noteId" FROM clip_note WHERE "noteId" = %(note_id)s
                           UNION ALL
                           SELECT "noteId" FROM note_unread WHERE "noteId" = %(note_id)s
                           UNION ALL
                           SELECT "noteId" FROM note_watching WHERE "noteId" = %(note_id)s
                           LIMIT 1
                       ) AS flag_subquery
                   ) AS is_flagged
            FROM note n
            WHERE n."id" = %(note_id)s
        """, {'note_id': note_id})
        result = self.db_cursor.fetchone()
        if result is None:
            return 'error'

        return {
            "id": note_id,
            "userId": result[0],
            "host": result[1],
            "mentions": result[2],
            "renoteId": result[3],
            "replyId": result[4],
            "fileIds": result[5],
            "hasPoll": result[6],
            "isFlagged": result[7]
        }

    def get_note_references(self, note_id):
        """
        获得引用和回复该笔记的id列表
        """
        self.db_cursor.execute(
            """SELECT id FROM note WHERE "renoteId" = %s OR "replyId" = %s""", [note_id, note_id])
        results = self.db_cursor.fetchall()
        return [result[0] for result in results]

    def is_note_pinned(self, note_id):
        """
        查询笔记是否被置顶
        """
        self.db_cursor.execute(
            """SELECT id FROM user_note_pining WHERE "noteId" = %s LIMIT 1""", [note_id])
        return self.db_cursor.fetchone() is not None

    def get_all_related_notes(self, note_ids, processed_ids=None, depth=0):
        """
        递归获取所有相关笔记信息的优化版本
        """
        if processed_ids is None:
            processed_ids = set()

        # 转换为list并去重，避免重复查询
        note_ids_to_process = list(set(note_ids) - processed_ids)
        if not note_ids_to_process:
            return {}

        all_notes = {}

        # 使用WITH RECURSIVE优化递归查询
        self.db_cursor.execute("""
            WITH RECURSIVE related_notes AS (
                -- Base case: direct notes
                SELECT n.id, n."userId", n."userHost", n.mentions, n."renoteId", n."replyId", 
                       n."fileIds", n."hasPoll"
                FROM note n
                WHERE n.id = ANY(%(note_ids)s)

                UNION

                -- Recursive case: related notes (replies and renotes)
                SELECT n.id, n."userId", n."userHost", n.mentions, n."renoteId", n."replyId",
                       n."fileIds", n."hasPoll"
                FROM note n
                INNER JOIN related_notes rn
                ON n.id = rn."renoteId" OR n.id = rn."replyId" OR
                   n."renoteId" = rn.id OR n."replyId" = rn.id
                WHERE rn.id IS NOT NULL
            ),
            flag_status AS (
                SELECT DISTINCT "noteId", TRUE AS is_flagged
                FROM (
                    SELECT "noteId" FROM note_reaction
                    UNION ALL
                    SELECT "noteId" FROM note_favorite
                    UNION ALL
                    SELECT "noteId" FROM clip_note
                    UNION ALL
                    SELECT "noteId" FROM note_unread
                    UNION ALL
                    SELECT "noteId" FROM note_watching
                ) AS flag_sources
                WHERE "noteId" IN (SELECT id FROM related_notes)
            )
            SELECT
                rn.id, rn."userId", rn."userHost", rn.mentions,
                rn."renoteId", rn."replyId", rn."fileIds", rn."hasPoll",
                COALESCE(fs.is_flagged, FALSE) AS is_flagged
            FROM related_notes rn
            LEFT JOIN flag_status fs ON rn.id = fs."noteId"
        """, {'note_ids': note_ids_to_process})

        for result in self.db_cursor.fetchall():
            note_id = result[0]
            processed_ids.add(note_id)
            all_notes[note_id] = {
                "id": note_id,
                "userId": result[1],
                "host": result[2],
                "mentions": result[3],
                "renoteId": result[4],
                "replyId": result[5],
                "fileIds": result[6],
                "hasPoll": result[7],
                "isFlagged": result[8]
            }

        return all_notes
class NoteDeleter:
    """
    笔记删除类
    """
    def __init__(self, db_connection):
        """
        初始化
        """
        self.db_connection = db_connection
        self.db_cursor = db_connection.cursor()

    def delete_note(self, note_id):
        """
        删除笔记
        """
        self.db_cursor.execute(
            """DELETE FROM note WHERE "id" = %s""", [note_id])
        self.db_connection.commit()

    def delete_file(self, file_id):
        """
        删除文件
        """
        self.db_cursor.execute(
            """DELETE FROM drive_file WHERE "id" = %s""", [file_id])
        self.db_connection.commit()
