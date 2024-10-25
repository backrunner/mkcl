from aix import generate_id

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
        """
        if not note_ids:
            return {}

        # 使用CTE优化查询
        self.db_cursor.execute(
            """
            WITH flagged_notes AS (
                SELECT DISTINCT "noteId"
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
                CASE WHEN fn."noteId" IS NOT NULL THEN TRUE ELSE FALSE END as "isFlagged"
            FROM note n
            LEFT JOIN flagged_notes fn ON n.id = fn."noteId"
            WHERE n.id = ANY(%s)
            """,
            [note_ids, note_ids, note_ids, note_ids, note_ids, note_ids]
        )

        notes_info = {}
        for row in self.db_cursor.fetchall():
            notes_info[row[0]] = {
                "id": row[0],
                "userId": row[1],
                "host": row[2],
                "mentions": row[3],
                "renoteId": row[4],
                "replyId": row[5],
                "fileIds": row[6] or [],
                "hasPoll": row[7],
                "isFlagged": row[8]
            }

        return notes_info

    def analyze_notes_batch(self, note_ids, end_id, redis_conn, file_manager, batch_size=100):
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
        user_results = dict(zip(all_user_ids, pipeline.execute()))

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

        # 更新Redis
        pipeline = redis_conn.pipeline()
        for note_id in notes_to_delete:
            pipeline.sadd('notes_to_delete', note_id)
        for note_id in note_ids:
            pipeline.srem('note_list', note_id)
        pipeline.execute()

        return len(notes_to_delete)


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
