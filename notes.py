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
            WITH flag_check AS (
                SELECT TRUE AS is_flagged
                FROM (
                    SELECT 1 FROM note_reaction WHERE "noteId" = %(note_id)s
                    UNION ALL
                    SELECT 1 FROM note_favorite WHERE "noteId" = %(note_id)s
                    UNION ALL
                    SELECT 1 FROM clip_note WHERE "noteId" = %(note_id)s
                    UNION ALL
                    SELECT 1 FROM note_unread WHERE "noteId" = %(note_id)s
                    UNION ALL
                    SELECT 1 FROM note_watching WHERE "noteId" = %(note_id)s
                    LIMIT 1
                ) AS flag_subquery
            )
            SELECT n."userId", n."userHost", n.mentions, n."renoteId", n."replyId", n."fileIds", n."hasPoll",
                   COALESCE((SELECT is_flagged FROM flag_check), FALSE) AS is_flagged
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
        递归获取所有相关笔记信息
        """
        if processed_ids is None:
            processed_ids = set()

        all_notes = {}
        new_ids_to_process = set()

        # 批量获取笔记信息
        self.db_cursor.execute("""
            WITH flag_check AS (
                SELECT "noteId", TRUE AS is_flagged
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
                ) AS flag_subquery
                WHERE "noteId" = ANY(%(note_ids)s)
                GROUP BY "noteId"
            )
            SELECT n.id, n."userId", n."userHost", n.mentions, n."renoteId", n."replyId", n."fileIds", n."hasPoll",
                   COALESCE(f.is_flagged, FALSE) AS is_flagged
            FROM note n
            LEFT JOIN flag_check f ON n.id = f."noteId"
            WHERE n.id = ANY(%(note_ids)s)
        """, {'note_ids': list(set(note_ids) - processed_ids)})

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

            if all_notes[note_id]["renoteId"] and all_notes[note_id]["renoteId"] not in processed_ids:
                new_ids_to_process.add(all_notes[note_id]["renoteId"])
            if all_notes[note_id]["replyId"] and all_notes[note_id]["replyId"] not in processed_ids:
                new_ids_to_process.add(all_notes[note_id]["replyId"])

        # 批量获取引用和回复
        if note_ids:
            self.db_cursor.execute("""
                SELECT id FROM note WHERE "renoteId" = ANY(%(note_ids)s) OR "replyId" = ANY(%(note_ids)s)
            """, {'note_ids': list(note_ids)})
            new_ids_to_process.update(id[0] for id in self.db_cursor.fetchall() if id[0] not in processed_ids)

        if new_ids_to_process:
            related_notes = self.get_all_related_notes(new_ids_to_process, processed_ids, depth + 1)
            all_notes.update(related_notes)

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
