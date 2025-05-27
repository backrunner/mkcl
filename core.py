import datetime
import pytz
from notes import NoteManager
from files import FileManager
from aix import generate_id
from tqdm import tqdm
from connection import RedisConnection, DatabaseConnection
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count
import traceback
import psycopg
from redis.exceptions import ConnectionError, TimeoutError
import time

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

def clean_data(db_info, redis_info, start_date, end_date, timeout_minutes=180):
    print("开始数据清理流程...")
    print(f"配置超时时间: {timeout_minutes} 分钟")
    
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

            note_manager = NoteManager(db_conn)
            file_manager = FileManager(db_conn)

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

                        batch_to_delete = note_manager.analyze_notes_batch_parallel(
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

            # 使用多线程并行删除note
            max_workers = min(cpu_count(), 8)  # 限制最大工作线程数，避免过多线程

            def delete_notes_batch_parallel(batch):
                # 每个线程使用独立的数据库连接，避免并发问题
                thread_db_conn = None
                try:
                    with db.get_connection() as thread_db_conn:
                        thread_note_manager = NoteManager(thread_db_conn)
                        thread_note_manager.delete_notes_batch(batch)
                        return len(batch)
                except Exception as e:
                    print(f"删除note批次时出错 (batch_size: {len(batch)}): {str(e)}")
                    raise  # 重新抛出异常，让主线程能够捕获

            try:
                with tqdm(total=len(notes_to_delete), desc="删除note") as pbar:
                    # 优化批处理大小，可根据实际情况调整
                    optimized_batch_size = min(batch_size * 2, 500)  # 增大删除操作的批处理大小
                    note_batches = [list(notes_to_delete)[i:i+optimized_batch_size]
                                for i in range(0, len(notes_to_delete), optimized_batch_size)]

                    try:
                        # 添加超时控制
                        import concurrent.futures
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = {executor.submit(delete_notes_batch_parallel, batch): batch
                                    for batch in note_batches}

                            # 设置总超时时间（180分钟）
                            timeout_seconds = timeout_minutes * 60
                            start_time = time.time()
                            
                            for future in as_completed(futures, timeout=timeout_seconds):
                                try:
                                    # 检查是否超过总时间限制
                                    if time.time() - start_time > timeout_seconds:
                                        print("操作超时，取消剩余任务")
                                        for f in futures:
                                            if not f.done():
                                                f.cancel()
                                        break
                                        
                                    result = future.result(timeout=300)  # 单个任务5分钟超时
                                    pbar.update(result)
                                except concurrent.futures.TimeoutError:
                                    print(f"删除note任务超时，取消剩余任务")
                                    for f in futures:
                                        if not f.done():
                                            f.cancel()
                                    raise NoteDeletionError("删除note操作超时", "步骤3:删除note", "任务执行时间过长")
                                except Exception as e:
                                    # 取消所有未完成的任务
                                    for f in futures:
                                        if not f.done():
                                            f.cancel()
                                    # 终止整个流程
                                    error_details = traceback.format_exc()
                                    if isinstance(e, psycopg.Error):
                                        raise DatabaseError(f"删除note过程中数据库错误: {str(e)}", "步骤3:删除note", error_details)
                                    else:
                                        raise NoteDeletionError(f"删除note过程中发生错误: {str(e)}", "步骤3:删除note", error_details)
                    except Exception as e:
                        if not isinstance(e, CleanupError):
                            error_details = traceback.format_exc()
                            raise NoteDeletionError(f"删除note过程失败: {str(e)}", "步骤3:删除note", error_details)
                        raise
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
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
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
