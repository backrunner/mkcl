import datetime
import pytz
from notes import NoteManager
from files import FileManager
from aix import generate_id
from tqdm import tqdm
from connection import RedisConnection, DatabaseConnection
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
from typing import Dict, Any, Tuple
import traceback
import psycopg
from redis.exceptions import ConnectionError, TimeoutError
import time

def _create_index_safely(db_connection_manager, index_name, table_name, columns, index_type="btree", where_clause=""):
    """
    安全地创建索引，优先使用CONCURRENTLY模式
    """
    try:
        # 使用DatabaseConnection的get_connection方法获取新连接
        with db_connection_manager.get_connection() as conn:
            # 使用独立连接和autocommit模式
            old_autocommit = conn.autocommit
            conn.autocommit = True
            cursor = conn.cursor()

            try:
                # 尝试CONCURRENTLY创建
                concurrent_sql = f'CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} ON {table_name}'
                if index_type == "gin":
                    concurrent_sql += f' USING gin ({columns})'
                else:
                    concurrent_sql += f' ({columns})'

                if where_clause:
                    concurrent_sql += f' {where_clause}'

                cursor.execute(concurrent_sql)
                print(f"✓ 索引 {index_name} 创建成功 (CONCURRENTLY)")
                return True

            except Exception as e:
                print(f"CONCURRENTLY模式失败: {str(e)}")

                # 回退到普通模式
                try:
                    normal_sql = f'CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}'
                    if index_type == "gin":
                        normal_sql += f' USING gin ({columns})'
                    else:
                        normal_sql += f' ({columns})'

                    if where_clause:
                        normal_sql += f' {where_clause}'

                    cursor.execute(normal_sql)
                    print(f"✓ 索引 {index_name} 创建成功 (普通模式)")
                    return True

                except Exception as normal_error:
                    print(f"✗ 索引 {index_name} 创建失败: {str(normal_error)}")
                    return False
            finally:
                conn.autocommit = old_autocommit

    except Exception as conn_error:
        print(f"✗ 连接错误，无法创建索引 {index_name}: {str(conn_error)}")
        return False

def _optimize_database_indexes(db_connection_manager):
    """
    检查并创建必要的索引以优化查询性能
    """
    # 获取一个连接用于索引检查
    with db_connection_manager.get_connection() as db_conn:
        cursor = db_conn.cursor()

        # 首先检查和优化PostgreSQL配置
        _check_postgresql_config(cursor)

        # 关键索引列表 - 根据查询模式优化，与README.md保持一致
        indexes_to_check = [
            # Note表基本索引 - 与README.md一致
            ("idx_note_id_composite", "note", '(id, "userId", "userHost", "renoteId", "replyId")'),
            ("idx_note_renote_reply", "note", '("renoteId", "replyId")'),
            ("idx_note_fileids", "note", '"fileIds"', "gin"),

            # 时间范围查询优化 - 与README.md一致
            ("idx_note_id_range", "note", "id DESC"),
            ("idx_drive_file_id_range", "drive_file", "id DESC"),
            ("idx_drive_file_link_host_id", "drive_file", '("isLink", "userHost", id)', "btree", 'WHERE "isLink" IS TRUE AND "userHost" IS NOT NULL'),
            ("idx_drive_file_link_host_id_btree", "drive_file", "id", "btree", 'WHERE "isLink" IS TRUE AND "userHost" IS NOT NULL'),

            # User表索引 - 与README.md一致
            ("idx_user_avatar_banner", "user", '("avatarId", "bannerId")'),
            ("idx_user_host_composite", "user", '(host, "followersCount", "followingCount")'),
            ("idx_user_id_host_counts", "user", '(id, host, "followersCount", "followingCount")'),
            ("idx_user_is_local", "user", "id", "btree", "WHERE host IS NULL"),

            # Note关联表索引 - 与README.md一致
            ("idx_note_reaction_noteid", "note_reaction", '"noteId"'),
            ("idx_note_favorite_noteid", "note_favorite", '"noteId"'),
            ("idx_clip_note_noteid", "clip_note", '"noteId"'),
            ("idx_note_unread_noteid", "note_unread", '"noteId"'),
            ("idx_note_watching_noteid", "note_watching", '"noteId"'),
            ("idx_user_note_pining_noteid", "user_note_pining", '"noteId"'),

            # 帖子分析优化索引 - 与README.md一致
            ("idx_note_userid_composite", "note", '("userId", "userHost", "hasPoll")', "btree", 'WHERE "hasPoll" = true OR "userHost" IS NULL'),

            # GIN索引 - 与README.md一致
            ("idx_note_non_empty_fileids", "note", '"fileIds"', "gin", 'WHERE array_length("fileIds", 1) > 0'),

            # 文件计数索引 - 与README.md一致
            ("idx_note_has_files", "note", '(array_length("fileIds", 1) > 0)', "btree", 'WHERE array_length("fileIds", 1) > 0'),

            # 历史记录索引 - 与README.md一致
            ("idx_note_history_targetid", "note_history", '"targetId"'),
        ]

        print("正在检查和创建性能优化索引...")
        created_count = 0
        skipped_count = 0
        failed_count = 0

        for index_info in indexes_to_check:
            index_name = index_info[0]
            table_name = index_info[1]
            columns = index_info[2]
            index_type = index_info[3] if len(index_info) > 3 else "btree"
            where_clause = index_info[4] if len(index_info) > 4 else ""

            try:
                # 检查索引是否存在
                cursor.execute("""
                    SELECT 1 FROM pg_indexes
                    WHERE indexname = %s AND tablename = %s
                """, [index_name, table_name])

                if not cursor.fetchone():
                    # 检查表是否存在
                    cursor.execute("""
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = %s AND table_schema = 'public'
                    """, [table_name])

                    if cursor.fetchone():
                        print(f"创建索引: {index_name}")
                        if _create_index_safely(db_connection_manager, index_name, table_name, columns, index_type, where_clause):
                            created_count += 1
                        else:
                            failed_count += 1
                    else:
                        print(f"表 {table_name} 不存在，跳过索引 {index_name}")
                        skipped_count += 1
                else:
                    print(f"索引 {index_name} 已存在")

            except Exception as e:
                print(f"处理索引 {index_name} 时出错: {str(e)}")
                failed_count += 1
                # 回滚并继续处理下一个索引
                try:
                    db_conn.rollback()
                except:
                    pass

        print(f"\n索引创建总结:")
        print(f"  ✓ 成功创建: {created_count} 个")
        print(f"  - 跳过: {skipped_count} 个")
        print(f"  ✗ 失败: {failed_count} 个")

        # 更新表统计信息
        try:
            print("\n更新表统计信息...")
            tables_to_analyze = [
                'note', 'drive_file', '"user"', 'note_reaction', 'note_favorite',
                'clip_note', 'note_unread', 'note_watching'
            ]

            analyze_success = 0
            for table in tables_to_analyze:
                try:
                    cursor.execute(f"ANALYZE {table}")
                    db_conn.commit()
                    analyze_success += 1
                    # 显示时去掉引号，更美观
                    display_name = table.strip('"')
                    print(f"  ✓ 分析表 {display_name}")
                except Exception as e:
                    display_name = table.strip('"')
                    print(f"  ✗ 分析表 {display_name} 失败: {str(e)}")
                    try:
                        db_conn.rollback()
                    except:
                        pass

            print(f"统计信息更新完成: {analyze_success}/{len(tables_to_analyze)} 个表")

        except Exception as e:
            print(f"更新统计信息失败: {str(e)}")

def _check_postgresql_config(cursor):
    """
    检查PostgreSQL配置并提供优化建议
    """
    print("检查PostgreSQL配置...")

    config_checks = [
        ('work_mem', '256MB', '工作内存'),
        ('maintenance_work_mem', '1GB', '维护工作内存'),
        ('shared_buffers', '25%RAM', '共享缓冲区'),
        ('effective_cache_size', '75%RAM', '有效缓存大小'),
        ('random_page_cost', '1.1', '随机页面成本'),
        ('checkpoint_completion_target', '0.9', '检查点完成目标'),
        ('max_connections', '200', '最大连接数'),
        ('max_parallel_workers_per_gather', '4', '并行工作进程数')
    ]

    print("\n=== PostgreSQL 配置检查 ===")
    optimizations_needed = []

    for setting, recommended, description in config_checks:
        try:
            cursor.execute(f"SHOW {setting}")
            result = cursor.fetchone()
            current_value = result[0] if result else 'unknown'

            if setting == 'work_mem':
                current_mb = _parse_memory_value(current_value)
                if current_mb < 256:
                    optimizations_needed.append(f"SET work_mem = '256MB';  -- 当前: {current_value}")

            elif setting == 'maintenance_work_mem':
                current_mb = _parse_memory_value(current_value)
                if current_mb < 1024:
                    optimizations_needed.append(f"SET maintenance_work_mem = '1GB';  -- 当前: {current_value}")

            elif setting == 'random_page_cost':
                if float(current_value) > 2.0:
                    optimizations_needed.append(f"SET random_page_cost = 1.1;  -- 当前: {current_value}")

            print(f"  {description} ({setting}): {current_value}")

        except Exception as e:
            print(f"  检查 {setting} 失败: {str(e)}")

    if optimizations_needed:
        print(f"\n=== 建议的优化配置 ===")
        print("请考虑在postgresql.conf中应用以下配置:")
        for opt in optimizations_needed:
            print(f"  {opt}")
        print("\n注意: 修改配置后需要重启PostgreSQL服务")
    else:
        print("  PostgreSQL配置看起来不错!")

def _parse_memory_value(memory_str):
    """
    解析PostgreSQL内存值为MB
    """
    try:
        memory_str = memory_str.lower().strip()
        if memory_str.endswith('gb'):
            return int(float(memory_str[:-2]) * 1024)
        elif memory_str.endswith('mb'):
            return int(float(memory_str[:-2]))
        elif memory_str.endswith('kb'):
            return int(float(memory_str[:-2]) / 1024)
        else:
            # 假设是字节
            return int(float(memory_str) / (1024 * 1024))
    except:
        return 0

class CleanupError(Exception):
    """清理过程中的错误基类"""
    def __init__(self, message, stage, details=None):
        self.message = message
        self.stage = stage
        self.details = details
        super().__init__(f"{stage}: {message}")

class DatabaseError(CleanupError):
    """数据库错误"""
    pass

class RedisError(CleanupError):
    """Redis连接或操作错误"""
    pass

class NoteDeletionError(CleanupError):
    """Note删除错误"""
    pass

class FileDeletionError(CleanupError):
    """文件删除错误"""
    pass

def clean_data(db_info, redis_info, start_date, end_date, timeout_minutes=180, verbose=False):
    print("开始数据清理流程...")
    print(f"配置超时时间: {timeout_minutes} 分钟")
    if verbose:
        print("启用详细调试模式")

    # 全局超时控制
    global_start_time = time.time()
    global_timeout_seconds = timeout_minutes * 60

    def check_global_timeout():
        """检查是否超过全局超时时间"""
        if time.time() - global_start_time > global_timeout_seconds:
            raise CleanupError(f"全局操作超时 ({timeout_minutes} 分钟)", "全局超时检查",
                             f"运行时间已超过 {timeout_minutes} 分钟")

    # 使用新的 Redis 连接管理器
    redis_conn = RedisConnection(redis_info)
    db = DatabaseConnection(db_info)

    # 添加索引优化步骤
    print("检查和优化数据库索引...")
    try:
        _optimize_database_indexes(db)
    except Exception as e:
        print(f"索引优化失败，继续执行: {str(e)}")

    # 清理之前的缓存数据
    print("清理历史缓存...")
    cache_keys = [
        'notes_to_delete',
        'files_to_delete',
        'files_to_keep',
        'file_cache',
        'user_cache',
        'users',
        'note_list'
    ]

    # 使用管道批量删除缓存键
    try:
        pipeline = redis_conn.pipeline()
        for key in cache_keys:
            pipeline.delete(key)
        redis_conn.execute(lambda: pipeline.execute())
    except (ConnectionError, TimeoutError) as e:
        error_details = traceback.format_exc()
        raise RedisError(f"Redis连接或超时错误: {str(e)}", "初始化阶段", error_details)
    except Exception as e:
        error_details = traceback.format_exc()
        raise RedisError("清理缓存键失败", "初始化阶段", error_details)

    try:
        with db.get_connection() as db_conn:
            try:
                start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
                end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
                end_id = generate_id(int(end_datetime.timestamp() * 1000))
            except ValueError as e:
                raise CleanupError(f"日期格式错误: {str(e)}", "参数验证阶段")

            note_manager = NoteManager(db_conn, verbose=verbose)
            file_manager = FileManager(db_conn, verbose=verbose)

            print("\n步骤 1/5: 收集需要处理的note...")
            try:
                notes_to_process = note_manager.get_notes_list(start_datetime, end_datetime)
                total_notes = len(notes_to_process)
                print(f"找到 {total_notes} 个note需要处理")

                # 批量获取置顶note
                pinned_notes = note_manager.get_pinned_notes(notes_to_process)
                print(f"其中置顶note {len(pinned_notes)} 个")

                # 使用集合操作来过滤非置顶note
                notes_to_process = list(set(notes_to_process) - pinned_notes)
            except psycopg.Error as e:
                error_details = traceback.format_exc()
                raise DatabaseError("收集note时数据库错误", "步骤1:收集note", error_details)
            except Exception as e:
                error_details = traceback.format_exc()
                raise CleanupError(f"收集note失败: {str(e)}", "步骤1:收集note", error_details)

            print("\n步骤 2/5: 分析note关联...")
            batch_size = 200
            processed_count = 0
            deleted_notes = 0

            try:
                with tqdm(total=len(notes_to_process), desc="分析note") as pbar:
                    while notes_to_process:
                        # 检查全局超时
                        check_global_timeout()

                        current_batch = notes_to_process[:batch_size]
                        notes_to_process = notes_to_process[batch_size:]

                        batch_to_delete = note_manager.analyze_notes_batch(
                            current_batch,
                            end_id,
                            redis_conn,
                            file_manager
                        )

                        processed_count += len(current_batch)
                        deleted_notes += batch_to_delete
                        pbar.update(len(current_batch))
                        pbar.set_postfix({
                            '已处理': processed_count,
                            '待删除': deleted_notes
                        })
            except psycopg.Error as e:
                error_details = traceback.format_exc()
                raise DatabaseError("分析note时数据库错误", "步骤2:分析note", error_details)
            except Exception as e:
                error_details = traceback.format_exc()
                raise CleanupError(f"分析note失败: {str(e)}", "步骤2:分析note", error_details)

            print("\n步骤 3/5: 删除note...")
            # 检查全局超时
            check_global_timeout()

            # 批量获取待删除的notes
            try:
                notes_to_delete = redis_conn.execute(
                    lambda: redis_conn.client.smembers('notes_to_delete')
                )
            except (ConnectionError, TimeoutError) as e:
                error_details = traceback.format_exc()
                raise RedisError(f"Redis连接或超时错误: {str(e)}", "步骤3:删除note", error_details)
            except Exception as e:
                error_details = traceback.format_exc()
                raise RedisError("获取待删除note列表失败", "步骤3:删除note", error_details)

            # 改为单线程删除，避免死锁问题
            print(f"准备删除 {len(notes_to_delete)} 个note（使用单线程模式避免死锁）")
            
            try:
                with tqdm(total=len(notes_to_delete), desc="删除note") as pbar:
                    # 使用较大的批处理大小，但单线程执行
                    optimized_batch_size = 500
                    note_batches = [list(notes_to_delete)[i:i+optimized_batch_size]
                                for i in range(0, len(notes_to_delete), optimized_batch_size)]

                    for batch in note_batches:
                        try:
                            # 检查全局超时
                            check_global_timeout()
                            
                            # 使用安全删除方法，避免外键约束死锁
                            note_manager.delete_notes_batch_safe(batch)
                            pbar.update(len(batch))
                            
                        except psycopg.errors.DeadlockDetected as e:
                            print(f"删除note批次时检测到死锁，跳过此批次: {str(e)}")
                            # 记录失败的批次，但继续处理其他批次
                            pbar.update(len(batch))
                            continue
                        except Exception as e:
                            print(f"删除note批次时出错 (batch_size: {len(batch)}): {str(e)}")
                            # 尝试更小的批次
                            if len(batch) > 50:
                                print(f"尝试使用更小的批次重新删除...")
                                smaller_batches = [batch[i:i+50] for i in range(0, len(batch), 50)]
                                for small_batch in smaller_batches:
                                    try:
                                        note_manager.delete_notes_batch_safe(small_batch)
                                    except Exception as small_e:
                                        print(f"小批次删除也失败: {str(small_e)}")
                                        continue
                            pbar.update(len(batch))
                            
            except CleanupError:
                raise
            except Exception as e:
                error_details = traceback.format_exc()
                raise NoteDeletionError(f"删除note阶段失败: {str(e)}", "步骤3:删除note", error_details)

            print("\n步骤 4/5: 删除关联文件...")
            # 检查全局超时
            check_global_timeout()

            # 批量获取待删除的文件
            try:
                files_to_delete = redis_conn.execute(
                    lambda: redis_conn.client.smembers('files_to_delete')
                )
            except (ConnectionError, TimeoutError) as e:
                error_details = traceback.format_exc()
                raise RedisError(f"Redis连接或超时错误: {str(e)}", "步骤4:删除关联文件", error_details)
            except Exception as e:
                error_details = traceback.format_exc()
                raise RedisError("获取待删除文件列表失败", "步骤4:删除关联文件", error_details)

            # 使用多线程并行删除文件
            def delete_files_batch_parallel(batch):
                # 每个线程使用独立的数据库连接，避免并发问题
                try:
                    with db.get_connection() as thread_db_conn:
                        thread_file_manager = FileManager(thread_db_conn)
                        thread_file_manager.delete_files_batch(batch)
                        return len(batch)
                except Exception as e:
                    print(f"删除文件批次时出错: {str(e)}")
                    raise  # 重新抛出异常，让主线程能够捕获

            try:
                with tqdm(total=len(files_to_delete), desc="删除文件") as pbar:
                    # 优化批处理大小
                    optimized_batch_size = min(batch_size * 2, 500)  # 增大删除操作的批处理大小
                    file_batches = [list(files_to_delete)[i:i+optimized_batch_size]
                                for i in range(0, len(files_to_delete), optimized_batch_size)]

                    try:
                        # 添加超时控制
                        import concurrent.futures
                        with ThreadPoolExecutor(max_workers=min(cpu_count(), 8)) as executor:
                            futures = {executor.submit(delete_files_batch_parallel, batch): batch
                                    for batch in file_batches}

                            # 使用配置的超时时间
                            timeout_seconds = timeout_minutes * 60
                            start_time = time.time()

                            for future in as_completed(futures, timeout=timeout_seconds):
                                try:
                                    # 检查是否超过总时间限制
                                    if time.time() - start_time > timeout_seconds:
                                        print("删除文件操作超时，取消剩余任务")
                                        for f in futures:
                                            if not f.done():
                                                f.cancel()
                                        break

                                    result = future.result(timeout=300)  # 单个任务5分钟超时
                                    pbar.update(result)
                                except concurrent.futures.TimeoutError:
                                    print(f"删除文件任务超时，取消剩余任务")
                                    for f in futures:
                                        if not f.done():
                                            f.cancel()
                                    raise FileDeletionError("删除文件操作超时", "步骤4:删除关联文件", "任务执行时间过长")
                                except Exception as e:
                                    # 取消所有未完成的任务
                                    for f in futures:
                                        if not f.done():
                                            f.cancel()
                                    # 终止整个流程
                                    error_details = traceback.format_exc()
                                    if isinstance(e, psycopg.Error):
                                        raise DatabaseError(f"删除文件过程中数据库错误: {str(e)}", "步骤4:删除关联文件", error_details)
                                    else:
                                        raise FileDeletionError(f"删除文件过程中发生错误: {str(e)}", "步骤4:删除关联文件", error_details)
                    except Exception as e:
                        if not isinstance(e, CleanupError):
                            error_details = traceback.format_exc()
                            raise FileDeletionError(f"删除文件过程失败: {str(e)}", "步骤4:删除关联文件", error_details)
                        raise
            except CleanupError:
                raise
            except Exception as e:
                error_details = traceback.format_exc()
                raise FileDeletionError(f"删除关联文件阶段失败: {str(e)}", "步骤4:删除关联文件", error_details)

            print("\n步骤 5/5: 清理单独文件...")
            # 检查全局超时
            check_global_timeout()

            try:
                file_manager.get_single_files(start_datetime, end_datetime, redis_conn)
            except psycopg.Error as e:
                error_details = traceback.format_exc()
                raise DatabaseError(f"扫描单独文件时数据库错误: {str(e)}", "步骤5:清理单独文件", error_details)
            except Exception as e:
                error_details = traceback.format_exc()
                raise CleanupError(f"扫描单独文件失败: {str(e)}", "步骤5:清理单独文件", error_details)

            # 批量获取待删除的单独文件
            try:
                remaining_files = redis_conn.execute(
                    lambda: redis_conn.client.smembers('files_to_delete')
                )
            except (ConnectionError, TimeoutError) as e:
                error_details = traceback.format_exc()
                raise RedisError(f"Redis连接或超时错误: {str(e)}", "步骤5:清理单独文件", error_details)
            except Exception as e:
                error_details = traceback.format_exc()
                raise RedisError("获取待删除单独文件列表失败", "步骤5:清理单独文件", error_details)

            # 使用多线程并行删除单独文件
            def delete_remaining_files_batch_parallel(batch):
                # 每个线程使用独立的数据库连接，避免并发问题
                try:
                    with db.get_connection() as thread_db_conn:
                        thread_file_manager = FileManager(thread_db_conn)
                        thread_file_manager.delete_files_batch(batch)
                        return len(batch)
                except Exception as e:
                    print(f"删除单独文件批次时出错: {str(e)}")
                    raise  # 重新抛出异常，让主线程能够捕获

            try:
                with tqdm(total=len(remaining_files), desc="删除单独文件") as pbar:
                    # 优化批处理大小
                    optimized_batch_size = min(batch_size * 2, 500)  # 增大删除操作的批处理大小
                    remaining_batches = [list(remaining_files)[i:i+optimized_batch_size]
                                    for i in range(0, len(remaining_files), optimized_batch_size)]

                    try:
                        # 添加超时控制
                        with ThreadPoolExecutor(max_workers=min(cpu_count(), 8)) as executor:
                            futures = {executor.submit(delete_remaining_files_batch_parallel, batch): batch
                                    for batch in remaining_batches}

                            # 使用配置的超时时间
                            timeout_seconds = timeout_minutes * 60
                            start_time = time.time()

                            for future in as_completed(futures, timeout=timeout_seconds):
                                try:
                                    # 检查是否超过总时间限制
                                    if time.time() - start_time > timeout_seconds:
                                        print("删除单独文件操作超时，取消剩余任务")
                                        for f in futures:
                                            if not f.done():
                                                f.cancel()
                                        break

                                    result = future.result(timeout=300)  # 单个任务5分钟超时
                                    pbar.update(result)
                                except concurrent.futures.TimeoutError:
                                    print(f"删除单独文件任务超时，取消剩余任务")
                                    for f in futures:
                                        if not f.done():
                                            f.cancel()
                                    raise FileDeletionError("删除单独文件操作超时", "步骤5:清理单独文件", "任务执行时间过长")
                                except Exception as e:
                                    # 取消所有未完成的任务
                                    for f in futures:
                                        if not f.done():
                                            f.cancel()
                                    # 终止整个流程
                                    error_details = traceback.format_exc()
                                    if isinstance(e, psycopg.Error):
                                        raise DatabaseError(f"删除单独文件过程中数据库错误: {str(e)}", "步骤5:清理单独文件", error_details)
                                    else:
                                        raise FileDeletionError(f"删除单独文件过程中发生错误: {str(e)}", "步骤5:清理单独文件", error_details)
                    except Exception as e:
                        if not isinstance(e, CleanupError):
                            error_details = traceback.format_exc()
                            raise FileDeletionError(f"删除单独文件过程失败: {str(e)}", "步骤5:清理单独文件", error_details)
                        raise
            except CleanupError:
                raise
            except Exception as e:
                error_details = traceback.format_exc()
                raise FileDeletionError(f"删除单独文件阶段失败: {str(e)}", "步骤5:清理单独文件", error_details)

            # 清理缓存 - 使用管道批量操作
            try:
                pipeline = redis_conn.pipeline()
                pipeline.delete("files_to_keep")
                pipeline.delete("user_cache")
                redis_conn.execute(lambda: pipeline.execute())
            except (ConnectionError, TimeoutError) as e:
                error_details = traceback.format_exc()
                # 不中断流程，只记录错误
                print(f"清理缓存时发生Redis连接或超时错误: {str(e)}")
            except Exception as e:
                error_details = traceback.format_exc()
                # 不中断流程，只记录错误
                print(f"清理缓存失败: {str(e)}")

            # 生成总结
            summary = f"""
清理完成！
总计：
- 处理note：{processed_count} 个
- 删除note：{len(notes_to_delete)} 个
- 删除文件：{len(files_to_delete) + len(remaining_files)} 个
"""
            print(summary)
            return f'共清退{len(notes_to_delete)}个note {len(files_to_delete) + len(remaining_files)}个文件'
    except CleanupError as e:
        # 根据不同的错误类型返回具体的错误信息
        error_type = type(e).__name__
        if hasattr(e, 'details') and e.details:
            # 为开发调试提供详细信息
            print(f"错误详情: {e.details}")
        error_message = f"{error_type} - {e.stage}: {e.message}"
        print(f"清理过程中发生错误: {error_message}")
        return f"清理失败: {error_message}"
    except Exception as e:
        # 捕获整个流程中的未分类异常
        error_details = traceback.format_exc()
        print(f"清理过程中发生严重错误: {str(e)}")
        print(f"错误详情: {error_details}")
        return f"清理失败: 未知错误 - {str(e)}"
