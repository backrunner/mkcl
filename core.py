import datetime
import pytz
from notes import NoteManager
from users import User
from files import FileManager
from aix import generate_id
from tqdm import tqdm
import psycopg_pool
from contextlib import contextmanager
from redis import ConnectionPool, Redis
from redis.exceptions import ConnectionError, TimeoutError
import backoff
from typing import Optional

class RedisConnection:
    """
    Redis connection manager with connection pool and retry mechanism
    """
    def __init__(self, redis_info: tuple):
        self.redis_info = redis_info
        self.pool = self._create_connection_pool()
        self._client: Optional[Redis] = None

    def _create_connection_pool(self) -> ConnectionPool:
        return ConnectionPool(
            host=self.redis_info[0],
            port=self.redis_info[1],
            db=self.redis_info[3],
            password=self.redis_info[2],
            decode_responses=True,
            max_connections=20,  # 最大连接数
            socket_timeout=5.0,  # socket 超时时间
            socket_connect_timeout=2.0,  # 连接超时时间
            retry_on_timeout=True  # 超时时自动重试
        )

    @property
    def client(self) -> Redis:
        if self._client is None:
            self._client = Redis(connection_pool=self.pool)
        return self._client

    def pipeline(self):
        """
        获取 Redis pipeline
        """
        return self.client.pipeline()

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, TimeoutError),
        max_tries=5,
        max_time=30
    )
    def execute(self, func, *args, **kwargs):
        """
        执行 Redis 操作，带有重试机制
        """
        try:
            return func(*args, **kwargs)
        except (ConnectionError, TimeoutError) as e:
            # 重新创建连接池
            self.pool = self._create_connection_pool()
            self._client = None
            raise e

class RedisCache:
    """
    使用Redis进行数据库缓存查询
    """
    def __init__(self, db_connection, redis_connection: RedisConnection):
        self.db_connection = db_connection
        self.redis = redis_connection

    def get_user_info(self, user_id):
        """
        获取用户信息
        """
        def _get_user():
            return self.redis.client.hget('users', user_id)

        user_info = self.redis.execute(_get_user)
        if user_info is not None:
            return user_info == 'True'
        else:
            user = User(self.db_connection, user_id)
            if user.is_local or user.is_vip:
                self.redis.execute(
                    lambda: self.redis.client.hset('users', user_id, 'True')
                )
                return True
            else:
                self.redis.execute(
                    lambda: self.redis.client.hset('users', user_id, 'False')
                )
                return False

    def clear_cache(self):
        """
        清除缓存用户列表
        """
        self.redis.execute(lambda: self.redis.client.delete("users"))

class DatabaseConnection:
    """
    Database connection manager with reconnection capability
    """
    def __init__(self, db_info: tuple):
        self.db_info = db_info
        self.pool = self._create_connection_pool()

    def _create_connection_pool(self):
        conninfo = "dbname={} user={} password={} host={} port={}".format(
            self.db_info[2], self.db_info[3], self.db_info[4], 
            self.db_info[0], self.db_info[1]
        )
        return psycopg_pool.ConnectionPool(
            conninfo=conninfo,
            min_size=5,
            max_size=20,
            max_idle=300,  # 5 minutes
            num_workers=3
        )

    @contextmanager
    def get_connection(self):
        try:
            conn = self.pool.getconn()
            yield conn
        except psycopg.OperationalError:
            # Try to recreate pool if connection fails
            self.pool = self._create_connection_pool()
            conn = self.pool.getconn()
            yield conn
        finally:
            self.pool.putconn(conn)

def clean_data(db_info, redis_info, start_date, end_date):
    print("开始数据清理流程...")

    # 使用新的 Redis 连接管理器
    redis_conn = RedisConnection(redis_info)
    db = DatabaseConnection(db_info)

    with db.get_connection() as db_conn:
        start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
        end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
        end_id = generate_id(int(end_datetime.timestamp() * 1000))

        note_manager = NoteManager(db_conn)
        file_manager = FileManager(db_conn)

        print("\n步骤 1/5: 收集需要处理的帖子...")
        notes_to_process = note_manager.get_notes_list(start_datetime, end_datetime)
        total_notes = len(notes_to_process)
        print(f"找到 {total_notes} 个帖子需要处理")

        # 批量获取置顶帖子
        pinned_notes = note_manager.get_pinned_notes(notes_to_process)
        print(f"其中置顶帖子 {len(pinned_notes)} 个")

        # 使用集合操作来过滤非置顶帖子
        notes_to_process = list(set(notes_to_process) - pinned_notes)

        print("\n步骤 2/5: 分析帖子关联...")
        batch_size = 100
        processed_count = 0
        deleted_notes = 0

        with tqdm(total=len(notes_to_process), desc="分析帖子") as pbar:
            while notes_to_process:
                current_batch = notes_to_process[:batch_size]
                notes_to_process = notes_to_process[batch_size:]

                batch_deleted = note_manager.analyze_notes_batch(
                    current_batch,
                    end_id,
                    redis_conn,
                    file_manager
                )

                processed_count += len(current_batch)
                deleted_notes += batch_deleted
                pbar.update(len(current_batch))
                pbar.set_postfix({
                    '已处理': processed_count,
                    '待删除': deleted_notes
                })

        print("\n步骤 3/5: 删除帖子...")
        notes_to_delete = redis_conn.execute(
            lambda: redis_conn.client.smembers('notes_to_delete')
        )
        with tqdm(total=len(notes_to_delete), desc="删除帖子") as pbar:
            for notes_batch in [list(notes_to_delete)[i:i+batch_size]
                              for i in range(0, len(notes_to_delete), batch_size)]:
                note_manager.delete_notes_batch(notes_batch)
                pbar.update(len(notes_batch))

        print("\n步骤 4/5: 删除关联文件...")
        files_to_delete = redis_conn.execute(
            lambda: redis_conn.client.smembers('files_to_delete')
        )
        with tqdm(total=len(files_to_delete), desc="删除文件") as pbar:
            for files_batch in [list(files_to_delete)[i:i+batch_size]
                              for i in range(0, len(files_to_delete), batch_size)]:
                file_manager.delete_files_batch(files_batch)
                pbar.update(len(files_batch))

        print("\n步骤 5/5: 清理单独文件...")
        file_manager.get_single_files_new(start_datetime, end_datetime, redis_conn)

        remaining_files = redis_conn.execute(
            lambda: redis_conn.client.smembers('files_to_delete')
        )
        with tqdm(total=len(remaining_files), desc="删除单独文件") as pbar:
            for files_batch in [list(remaining_files)[i:i+batch_size]
                              for i in range(0, len(remaining_files), batch_size)]:
                file_manager.delete_files_batch(files_batch)
                pbar.update(len(files_batch))

        # 清理缓存
        redis_conn.execute(lambda: redis_conn.client.delete("files_to_keep"))
        redis_conn.execute(lambda: redis_conn.client.delete("user_cache"))

        # 生成总结
        summary = f"""
清理完成！
总计：
- 处理帖子：{processed_count} 个
- 删除帖子：{len(notes_to_delete)} 个
- 删除文件：{len(files_to_delete) + len(remaining_files)} 个
"""
        print(summary)
        return f'共清退{len(notes_to_delete)}帖子 {len(files_to_delete) + len(remaining_files)}文件'
