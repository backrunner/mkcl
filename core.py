import datetime
import pytz
from notes import NoteManager
from files import FileManager
from aix import generate_id
from tqdm import tqdm
from connection import RedisConnection, DatabaseConnection
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import cpu_count

def clean_data(db_info, redis_info, start_date, end_date):
    print("开始数据清理流程...")

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
    pipeline = redis_conn.pipeline()
    for key in cache_keys:
        pipeline.delete(key)
    redis_conn.execute(lambda: pipeline.execute())

    try:
        with db.get_connection() as db_conn:
            start_datetime = datetime.datetime.strptime(start_date, '%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
            end_datetime = datetime.datetime.strptime(end_date, '%Y-%m-%d').replace(tzinfo=pytz.timezone('UTC'))
            end_id = generate_id(int(end_datetime.timestamp() * 1000))

            note_manager = NoteManager(db_conn)
            file_manager = FileManager(db_conn)

            print("\n步骤 1/5: 收集需要处理的note...")
            notes_to_process = note_manager.get_notes_list(start_datetime, end_datetime)
            total_notes = len(notes_to_process)
            print(f"找到 {total_notes} 个note需要处理")

            # 批量获取置顶note
            pinned_notes = note_manager.get_pinned_notes(notes_to_process)
            print(f"其中置顶note {len(pinned_notes)} 个")

            # 使用集合操作来过滤非置顶note
            notes_to_process = list(set(notes_to_process) - pinned_notes)

            print("\n步骤 2/5: 分析note关联...")
            batch_size = 200
            processed_count = 0
            deleted_notes = 0

            with tqdm(total=len(notes_to_process), desc="分析note") as pbar:
                while notes_to_process:
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

            print("\n步骤 3/5: 删除note...")
            # 批量获取待删除的notes
            notes_to_delete = redis_conn.execute(
                lambda: redis_conn.client.smembers('notes_to_delete')
            )
            
            # 使用多线程并行删除note
            max_workers = cpu_count()
            
            def delete_notes_batch_parallel(batch):
                try:
                    note_manager.delete_notes_batch(batch)
                    return len(batch)
                except Exception as e:
                    print(f"删除note批次时出错: {str(e)}")
                    raise  # 重新抛出异常，让主线程能够捕获
                
            with tqdm(total=len(notes_to_delete), desc="删除note") as pbar:
                # 优化批处理大小，可根据实际情况调整
                optimized_batch_size = min(batch_size * 2, 500)  # 增大删除操作的批处理大小
                note_batches = [list(notes_to_delete)[i:i+optimized_batch_size] 
                               for i in range(0, len(notes_to_delete), optimized_batch_size)]
                
                try:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {executor.submit(delete_notes_batch_parallel, batch): batch 
                                  for batch in note_batches}
                        
                        for future in as_completed(futures):
                            try:
                                pbar.update(future.result())
                            except Exception as e:
                                # 取消所有未完成的任务
                                for f in futures:
                                    if not f.done():
                                        f.cancel()
                                # 终止整个流程
                                raise RuntimeError(f"删除note过程中发生错误，终止清理流程: {str(e)}")
                except Exception as e:
                    print(f"删除note过程失败，终止整个清理流程: {str(e)}")
                    return f"清理失败: {str(e)}"

            print("\n步骤 4/5: 删除关联文件...")
            # 批量获取待删除的文件
            files_to_delete = redis_conn.execute(
                lambda: redis_conn.client.smembers('files_to_delete')
            )
            
            # 使用多线程并行删除文件
            def delete_files_batch_parallel(batch):
                try:
                    file_manager.delete_files_batch(batch)
                    return len(batch)
                except Exception as e:
                    print(f"删除文件批次时出错: {str(e)}")
                    raise  # 重新抛出异常，让主线程能够捕获
                
            with tqdm(total=len(files_to_delete), desc="删除文件") as pbar:
                # 优化批处理大小
                optimized_batch_size = min(batch_size * 2, 500)  # 增大删除操作的批处理大小
                file_batches = [list(files_to_delete)[i:i+optimized_batch_size]
                              for i in range(0, len(files_to_delete), optimized_batch_size)]
                
                try:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {executor.submit(delete_files_batch_parallel, batch): batch 
                                  for batch in file_batches}
                        
                        for future in as_completed(futures):
                            try:
                                pbar.update(future.result())
                            except Exception as e:
                                # 取消所有未完成的任务
                                for f in futures:
                                    if not f.done():
                                        f.cancel()
                                # 终止整个流程
                                raise RuntimeError(f"删除文件过程中发生错误，终止清理流程: {str(e)}")
                except Exception as e:
                    print(f"删除文件过程失败，终止整个清理流程: {str(e)}")
                    return f"清理失败: {str(e)}"

            print("\n步骤 5/5: 清理单独文件...")
            file_manager.get_single_files(start_datetime, end_datetime, redis_conn)

            # 批量获取待删除的单独文件
            remaining_files = redis_conn.execute(
                lambda: redis_conn.client.smembers('files_to_delete')
            )
            
            # 使用多线程并行删除单独文件
            with tqdm(total=len(remaining_files), desc="删除单独文件") as pbar:
                # 优化批处理大小
                optimized_batch_size = min(batch_size * 2, 500)  # 增大删除操作的批处理大小
                remaining_batches = [list(remaining_files)[i:i+optimized_batch_size]
                                   for i in range(0, len(remaining_files), optimized_batch_size)]
                
                try:
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = {executor.submit(delete_files_batch_parallel, batch): batch 
                                  for batch in remaining_batches}
                        
                        for future in as_completed(futures):
                            try:
                                pbar.update(future.result())
                            except Exception as e:
                                # 取消所有未完成的任务
                                for f in futures:
                                    if not f.done():
                                        f.cancel()
                                # 终止整个流程
                                raise RuntimeError(f"删除单独文件过程中发生错误，终止清理流程: {str(e)}")
                except Exception as e:
                    print(f"删除单独文件过程失败，终止整个清理流程: {str(e)}")
                    return f"清理失败: {str(e)}"

            # 清理缓存 - 使用管道批量操作
            pipeline = redis_conn.pipeline()
            pipeline.delete("files_to_keep")
            pipeline.delete("user_cache")
            redis_conn.execute(lambda: pipeline.execute())

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
    except Exception as e:
        # 捕获整个流程中的异常
        print(f"清理过程中发生严重错误: {str(e)}")
        return f"清理失败: {str(e)}"
