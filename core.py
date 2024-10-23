import psycopg
import redis

import datetime
import pytz
from notes import NoteManager, NoteDeleter
from users import User
from files import FileManager
from aix import generate_id

class RedisCache:
    """
    使用Redis进行数据库缓存查询
    """

    def __init__(self, db_connection, redis_connection):
        """
        初始化
        """
        self.db_connection = db_connection
        self.redis = redis_connection

    def get_user_info(self, user_id):
        """
        获取用户信息
        """
        user_info = self.redis.hget('users', user_id)
        if user_info is not None:
            return user_info == 'True'
        else:
            user = User(self.db_connection, user_id)
            if user.is_local or user.is_vip:
                self.redis.hset('users', user_id, 'True')
                return True
            else:
                self.redis.hset('users', user_id, 'False')
                return False

    def clear_cache(self):
        """
        清除缓存用户列表
        """
        self.redis.delete("users")

def clean_data(db_info, redis_info, start_date, end_date):
    redis_conn = redis.Redis(host=redis_info[0], port=redis_info[1], db=redis_info[3], password=redis_info[2], decode_responses=True)
    db_conn = psycopg.connect("dbname={} user={} password={} host={} port={}".format(db_info[2], db_info[3], db_info[4], db_info[0], db_info[1]))

    start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
    end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
    end_id = generate_id(int(end_datetime.timestamp() * 1000))

    note_manager = NoteManager(db_conn)
    file_manager = FileManager(db_conn)

    notes_to_process = note_manager.get_notes_list(start_datetime, end_datetime)
    for note_id in notes_to_process:
        if not note_manager.is_note_pinned(note_id):
            redis_conn.sadd('note_list', note_id)

    note_to_delete = redis_conn.srandmember('note_list')
    redis_cache = RedisCache(db_conn, redis_conn)

    while note_to_delete is not None:
        current_note_id = str(note_to_delete)
        notes_info = note_manager.get_all_related_notes([current_note_id])
        should_keep = False
        note_ids = []
        file_ids = []
        user_ids = []

        for note_id, note_content in notes_info.items():
            note_ids.append(note_id)
            user_ids.append(note_content['userId'])
            file_ids.extend(note_content["fileIds"])
            if note_content['hasPoll'] or note_content["isFlagged"]:
                should_keep = True
            if note_content["id"] > end_id:
                should_keep = True

        for user_id in user_ids:
            user_info = redis_cache.get_user_info(user_id)
            should_keep = should_keep or user_info

        if not should_keep:
            for note_id in note_ids:
                redis_conn.sadd('notes_to_delete', note_id)

            for file_id in file_ids:
                file_references = file_manager.get_file_references(file_id)
                is_local_file = file_manager.is_file_local(file_id)
                if file_references > 1 or not is_local_file:
                    print(f"特殊情况：{file_id} 不予删除 {is_local_file}")
                    redis_conn.sadd('files_to_keep', file_id)
                else:
                    redis_conn.sadd('files_to_delete', file_id)
        else:
            for file_id in file_ids:
                redis_conn.sadd('files_to_keep', file_id)

        for note_id in note_ids:
            redis_conn.srem('note_list', note_id)
        if current_note_id not in note_ids:
            redis_conn.srem('note_list', current_note_id)

        note_to_delete = redis_conn.srandmember('note_list')

    note_deleter = NoteDeleter(db_conn)
    note_to_delete = redis_conn.srandmember('notes_to_delete')
    deleted_notes_count = 0
    deleted_files_count = 0

    while note_to_delete is not None:
        deleted_notes_count += 1
        note_deleter.delete_note(note_to_delete)
        redis_conn.srem('notes_to_delete', note_to_delete)
        print(f'已移除帖子 {note_to_delete}')
        note_to_delete = redis_conn.srandmember('notes_to_delete')

    file_to_delete = redis_conn.srandmember('files_to_delete')
    while file_to_delete is not None:
        deleted_files_count += 1
        note_deleter.delete_file(file_to_delete)
        redis_conn.srem('files_to_delete', file_to_delete)
        print(f'已移除文件 {file_to_delete}')
        file_to_delete = redis_conn.srandmember('files_to_delete')

    print("开始清理单独文件")
    file_manager.get_single_files_new(start_datetime, end_datetime, redis_conn)

    file_to_delete = redis_conn.srandmember('files_to_delete')
    while file_to_delete is not None:
        deleted_files_count += 1
        note_deleter.delete_file(file_to_delete)
        redis_conn.srem('files_to_delete', file_to_delete)
        print(f'已移除文件 {file_to_delete}')
        file_to_delete = redis_conn.srandmember('files_to_delete')

    redis_conn.delete("files_to_keep")
    redis_cache.clear_cache()
    print(f'共移除{deleted_notes_count}帖子 {deleted_files_count}文件')
    return f'共清退{deleted_notes_count}帖子 {deleted_files_count}文件'
