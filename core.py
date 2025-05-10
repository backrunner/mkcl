import datetime
import pytz
from notes import NoteManager
from files import FileManager
from aix import generate_id
from tqdm import tqdm
from connection import RedisConnection, DatabaseConnection
from concurrent.futures import ThreadPoolExecutor, as_completed

def clean_data(db_info, redis_info, start_date, end_date, batch_size=500, max_workers=None):
    """
    数据清理主函数
    Args:
        db_info: 数据库连接信息
        redis_info: Redis连接信息
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        batch_size: 批处理大小，默认500
        max_workers: 最大工作线程数，默认None (由CPU核心数决定)
    """
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

    # 批量删除Redis键
    redis_conn.execute(lambda: redis_conn.client.delete(*cache_keys))

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
        processed_count = 0
        deleted_notes = 0

        # 更大的批处理大小，更少的批次
        analysis_batch_size = batch_size
        batches = [notes_to_process[i:i+analysis_batch_size]
                  for i in range(0, len(notes_to_process), analysis_batch_size)]

        with tqdm(total=len(notes_to_process), desc="分析note") as pbar:
            # 并行处理多个批次
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 创建并行任务
                future_to_batch = {
                    executor.submit(
                        note_manager.analyze_notes_batch_parallel,
                        batch, end_id, redis_conn, file_manager,
                        analysis_batch_size // 5  # 子批次大小
                    ): batch
                    for batch in batches
                }

                # 收集结果
                for future in as_completed(future_to_batch):
                    batch = future_to_batch[future]
                    try:
                        batch_deleted = future.result()
                        processed_count += len(batch)
                        deleted_notes += batch_deleted
                        pbar.update(len(batch))
                        pbar.set_postfix({
                            '已处理': processed_count,
                            '待删除': deleted_notes
                        })
                    except Exception as e:
                        print(f"处理批次时出错: {str(e)}")

        print("\n步骤 3/5: 删除note...")
        notes_to_delete = redis_conn.execute(
            lambda: redis_conn.client.smembers('notes_to_delete')
        )

        # 更大的批处理大小以加快删除速度
        delete_batch_size = min(2000, batch_size * 2)
        delete_batches = [list(notes_to_delete)[i:i+delete_batch_size]
                        for i in range(0, len(notes_to_delete), delete_batch_size)]

        with tqdm(total=len(notes_to_delete), desc="删除note") as pbar:
            # 并行删除
            with ThreadPoolExecutor(max_workers=max(1, max_workers // 2)) as executor:
                futures = [
                    executor.submit(note_manager.delete_notes_batch, batch)
                    for batch in delete_batches
                ]

                for i, future in enumerate(as_completed(futures)):
                    try:
                        future.result()
                        pbar.update(len(delete_batches[i]))
                    except Exception as e:
                        print(f"删除批次时出错: {str(e)}")

        print("\n步骤 4/5: 删除关联文件...")
        files_to_delete = redis_conn.execute(
            lambda: redis_conn.client.smembers('files_to_delete')
        )

        files_batch_size = min(1000, batch_size)
        files_batches = [list(files_to_delete)[i:i+files_batch_size]
                      for i in range(0, len(files_to_delete), files_batch_size)]

        with tqdm(total=len(files_to_delete), desc="删除文件") as pbar:
            # 并行删除文件
            with ThreadPoolExecutor(max_workers=max(1, max_workers // 2)) as executor:
                futures = [
                    executor.submit(file_manager.delete_files_batch, batch)
                    for batch in files_batches
                ]

                for i, future in enumerate(as_completed(futures)):
                    try:
                        future.result()
                        pbar.update(len(files_batches[i]))
                    except Exception as e:
                        print(f"删除文件批次时出错: {str(e)}")

        print("\n步骤 5/5: 清理单独文件...")
        # 获取单独文件
        file_manager.get_single_files(start_datetime, end_datetime, redis_conn)

        remaining_files = redis_conn.execute(
            lambda: redis_conn.client.smembers('files_to_delete')
        )

        remaining_batches = [list(remaining_files)[i:i+files_batch_size]
                          for i in range(0, len(remaining_files), files_batch_size)]

        with tqdm(total=len(remaining_files), desc="删除单独文件") as pbar:
            # 并行删除单独文件
            with ThreadPoolExecutor(max_workers=max(1, max_workers // 2)) as executor:
                futures = [
                    executor.submit(file_manager.delete_files_batch, batch)
                    for batch in remaining_batches
                ]

                for i, future in enumerate(as_completed(futures)):
                    try:
                        future.result()
                        pbar.update(len(remaining_batches[i]))
                    except Exception as e:
                        print(f"删除单独文件批次时出错: {str(e)}")

        # 清理缓存
        redis_conn.execute(lambda: redis_conn.client.delete("files_to_keep"))
        redis_conn.execute(lambda: redis_conn.client.delete("user_cache"))

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
