
import psycopg_pool
from contextlib import contextmanager
from redis import ConnectionPool, Redis
import backoff
from typing import Optional
from users import User
from redis.exceptions import ConnectionError, TimeoutError

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