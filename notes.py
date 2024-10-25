from aix import generate_id

class NoteManager:
    def __init__(self, db_connection):
        self.db_cursor = db_connection.cursor()
        self.db_conn = db_connection

    def get_notes_batch(self, note_ids):
        """
        批量获取帖子信息
        """
        if not note_ids:
            return {}
            
        self.db_cursor.execute(
            """
            SELECT id, "userId", "fileIds", "hasPoll", 
                   "isFlagged", "renoteId", "replyId"
            FROM note 
            WHERE id = ANY(%s)
            """, 
            [note_ids]
        )
        return {
            row[0]: {
                "id": row[0],
                "userId": row[1],
                "fileIds": row[2] or [],
                "hasPoll": row[3],
                "isFlagged": row[4],
                "renoteId": row[5],
                "replyId": row[6]
            }
            for row in self.db_cursor.fetchall()
        }

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
