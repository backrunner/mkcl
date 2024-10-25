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
        self.db_cursor.execute("""SELECT "userId", "userHost", mentions, "renoteId", "replyId", "fileIds", "hasPoll" FROM note WHERE "id" = %s""", [note_id])
        result = self.db_cursor.fetchone()
        if result is None:
            return 'error'

        note_info = {
            "id": note_id,
            "userId": result[0],
            "host": result[1],
            "mentions": result[2],
            "renoteId": result[3],
            "replyId": result[4],
            "fileIds": result[5],
            "hasPoll": result[6],
            "isFlagged": False
        }

        tables_to_check = [
            "note_reaction", "note_favorite", "clip_note",
            "note_unread", "note_watching"
        ]

        for table in tables_to_check:
            self.db_cursor.execute(f"""SELECT id FROM {table} WHERE "noteId" = %s LIMIT 1""", [note_id])
            if self.db_cursor.fetchone():
                note_info["isFlagged"] = True
                break

        return note_info

    def get_note_references(self, note_id):
        """
        获得引用和回复该笔记的id列表
        """
        self.db_cursor.execute(
            """SELECT id FROM note WHERE "renoteId" = %s OR "replyId" = %s""", [note_id, note_id])
        results = self.db_cursor.fetchall()
        return [result[0] for result in results]

    def get_all_related_notes(self, note_ids, processed_ids=None, depth=0):
        """
        递归获取所有相关笔记信息
        """
        if processed_ids is None:
            processed_ids = set()

        all_notes = {}
        new_ids_to_process = []

        for note_id in note_ids:
            if note_id in processed_ids:
                continue
            processed_ids.add(note_id)

            note_info = self.get_note_info(note_id)
            if note_info == 'error':
                print(f'出现错误 id {note_id}')
                continue

            all_notes[note_id] = note_info

            if note_info["renoteId"] and note_info["renoteId"] not in processed_ids:
                new_ids_to_process.append(note_info["renoteId"])
            if note_info["replyId"] and note_info["replyId"] not in processed_ids:
                new_ids_to_process.append(note_info["replyId"])

            referenced_notes = self.get_note_references(note_id)
            new_ids_to_process.extend([id for id in referenced_notes if id not in processed_ids])

        if new_ids_to_process:
            related_notes = self.get_all_related_notes(new_ids_to_process, processed_ids, depth + 1)
            all_notes.update(related_notes)

        return all_notes

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

        # 使用 IN 查询一次性获取所有置顶帖子
        placeholders = ','.join(['%s'] * len(note_ids))
        query = f"""
            SELECT "noteId"
            FROM user_note_pining
            WHERE "noteId" IN ({placeholders})
        """
        self.db_cursor.execute(query, note_ids)
        results = self.db_cursor.fetchall()
        return {str(result[0]) for result in results}


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
