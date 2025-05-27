import psycopg
import psycopg_pool
from contextlib import contextmanager
from redis import ConnectionPool, Redis
import backoff
from typing import Optional
from users import User
from redis.exceptions import ConnectionError, TimeoutError
import time

class RedisConnection:
    """
    Redis connection manager with connection pool and retry mechanism
    """
    def __init__(self, redis_info: tuple):
        self.redis_info = redis_info
        self.pool = self._create_connection_pool()
        self._client: Optional[Redis] = None
        # 熔断器状态
        self.circuit_breaker = {
            'failures': 0,
            'last_failure_time': 0,
            'state': 'closed',  # closed, open, half_open
            'failure_threshold': 10,
            'recovery_timeout': 60  # 60秒后尝试恢复
        }

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

    def _check_circuit_breaker(self):
        """检查熔断器状态"""
        current_time = time.time()
        if self.circuit_breaker['state'] == 'open':
            if current_time - self.circuit_breaker['last_failure_time'] > self.circuit_breaker['recovery_timeout']:
                self.circuit_breaker['state'] = 'half_open'
                print("Redis熔断器进入半开状态，尝试恢复连接")
            else:
                raise ConnectionError("Redis熔断器开启，暂时拒绝连接")

    def _record_success(self):
        """记录成功操作"""
        self.circuit_breaker['failures'] = 0
        if self.circuit_breaker['state'] == 'half_open':
            self.circuit_breaker['state'] = 'closed'
            print("Redis熔断器已恢复正常状态")

    def _record_failure(self):
        """记录失败操作"""
        self.circuit_breaker['failures'] += 1
        self.circuit_breaker['last_failure_time'] = time.time()
        
        if self.circuit_breaker['failures'] >= self.circuit_breaker['failure_threshold']:
            self.circuit_breaker['state'] = 'open'
            print(f"Redis熔断器开启，连续失败次数: {self.circuit_breaker['failures']}")

    @backoff.on_exception(
        backoff.expo,
        (ConnectionError, TimeoutError),
        max_tries=3,  # 减少重试次数
        max_time=15   # 减少最大重试时间
    )
    def execute(self, func, *args, **kwargs):
        """
        执行 Redis 操作，带有重试机制和熔断器
        """
        try:
            self._check_circuit_breaker()
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except (ConnectionError, TimeoutError) as e:
            self._record_failure()
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
            num_workers=3,
            timeout=30.0  # 添加连接获取超时
        )

    @contextmanager
    def get_connection(self):
        conn = None
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # 添加超时控制，防止无限等待
                conn = self.pool.getconn(timeout=10.0)
                yield conn
                break
            except (psycopg.OperationalError, psycopg_pool.PoolTimeout) as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"数据库连接失败，已达最大重试次数: {str(e)}")
                    raise
                else:
                    print(f"数据库连接失败，重试 {retry_count}/{max_retries}: {str(e)}")
                    # 尝试重新创建连接池
                    try:
                        self.pool = self._create_connection_pool()
                    except Exception as pool_error:
                        print(f"重新创建连接池失败: {str(pool_error)}")
                    # 等待短暂时间后重试
                    time.sleep(1.0 * retry_count)
            except Exception as e:
                print(f"意外的数据库错误: {str(e)}")
                if conn:
                    try:
                        self.pool.putconn(conn)
                    except:
                        pass
                raise
            finally:
                # 确保连接始终返回到池中
                if conn:
                    try:
                        self.pool.putconn(conn)
                    except Exception as put_error:
                        print(f"返回连接到池时出错: {str(put_error)}")