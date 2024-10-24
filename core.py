import psycopg
import redis
import datetime
import pytz
from typing import List, Dict, Set
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
        self.user_cache: Dict[str, bool] = {}

    def get_user_info(self, user_id: str) -> bool:
        """
        获取用户信息
        """
        if user_id in self.user_cache:
            return self.user_cache[user_id]

        user_info = self.redis.hget('users', user_id)
        if user_info is not None:
            result = user_info == 'True'
        else:
            user = User(self.db_connection, user_id)
            result = user.is_local or user.is_vip
            self.redis.hset('users', user_id, str(result))

        self.user_cache[user_id] = result
        return result

    def clear_cache(self):
        """
        清除缓存用户列表
        """
        self.redis.delete("users")
        self.user_cache.clear()

def clean_data(db_info: List[str], redis_info: List[str], start_date: str, end_date: str) -> str:
    redis_conn = redis.Redis(host=redis_info[0], port=redis_info[1], db=redis_info[3], password=redis_info[2], decode_responses=True)
    db_conn = psycopg.connect(f"dbname={db_info[2]} user={db_info[3]} password={db_info[4]} host={db_info[0]} port={db_info[1]}")

    start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.UTC)
    end_id = generate_id(int(end_datetime.timestamp() * 1000))

    note_manager = NoteManager(db_conn)
    file_manager = FileManager(db_conn)
    redis_cache = RedisCache(db_conn, redis_conn)

    notes_to_process = note_manager.get_notes_list(start_datetime, end_datetime)
    print(f"找到 {len(notes_to_process)} 条需要处理的帖子")
    if notes_to_process:
        redis_conn.sadd('note_list', *[note_id for note_id in notes_to_process if not note_manager.is_note_pinned(note_id)])

    notes_to_delete: Set[str] = set()
    files_to_delete: Set[str] = set()
    files_to_keep: Set[str] = set()

    processed_notes_count = 0
    while True:
        note_batch = redis_conn.spop('note_list', 100)  # 批量处理提高效率
        if not note_batch:
            break

        notes_info = note_manager.get_all_related_notes(note_batch)
        processed_notes_count += len(notes_info)
        print(f"正在处理第 {processed_notes_count} 条帖子")

        for current_note_id, note_content in notes_info.items():
            should_keep = (
                note_content['hasPoll'] or
                note_content["isFlagged"] or
                note_content["id"] > end_id or
                redis_cache.get_user_info(note_content['userId'])
            )

            if should_keep:
                files_to_keep.update(note_content["fileIds"])
            else:
                notes_to_delete.add(current_note_id)
                for file_id in note_content["fileIds"]:
                    file_references = file_manager.get_file_references(file_id)
                    is_local_file = file_manager.is_file_local(file_id)
                    if file_references > 1 or not is_local_file:
                        print(f"特殊情况：{file_id} 不予删除 {is_local_file}")
                        files_to_keep.add(file_id)
                    else:
                        files_to_delete.add(file_id)

    print(f"处理完成，共处理 {processed_notes_count} 条帖子")
    print(f"待删除帖子数: {len(notes_to_delete)}")
    print(f"待删除文件数: {len(files_to_delete)}")
    print(f"需要保留的文件数: {len(files_to_keep)}")

    # 批量添加到 Redis
    if notes_to_delete:
        redis_conn.sadd('notes_to_delete', *notes_to_delete)
    if files_to_delete:
        redis_conn.sadd('files_to_delete', *files_to_delete)
    if files_to_keep:
        redis_conn.sadd('files_to_keep', *files_to_keep)

    note_deleter = NoteDeleter(db_conn)
    deleted_notes_count = delete_items(redis_conn, 'notes_to_delete', note_deleter.delete_note, '帖子')
    deleted_files_count = delete_items(redis_conn, 'files_to_delete', note_deleter.delete_file, '文件')

    print("开始清理单独文件")
    single_files_count = file_manager.get_single_files_new(start_datetime, end_datetime, redis_conn)
    print(f"找到 {single_files_count} 个单独文件")
    deleted_files_count += delete_items(redis_conn, 'files_to_delete', note_deleter.delete_file, '文件')

    redis_conn.delete("files_to_keep")
    redis_cache.clear_cache()

    result = f'共清退 {deleted_notes_count} 帖子 {deleted_files_count} 文件'
    print(result)
    return result

def delete_items(redis_conn: redis.Redis, key: str, delete_func, item_name: str) -> int:
    count = 0
    batch_size = 100
    while True:
        items = redis_conn.spop(key, batch_size)
        if not items:
            break
        for item in items:
            delete_func(item)
            count += 1
        print(f'已移除 {count} 个{item_name}')
    return count
