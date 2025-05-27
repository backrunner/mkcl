from aix import generate_id
from connection import RedisConnection
from tqdm import tqdm

class FileManager:
    """
    文件相关操作类
    """

    def __init__(self, db_connection, verbose=False):
        """
        初始化数据库游标
        """
        try:
            self.db_conn = db_connection
            self.db_cursor = db_connection.cursor()
            self.verbose = verbose
        except Exception as e:
            print(f"初始化文件管理器失败: {str(e)}")
            raise

    def get_file_references(self, file_id):
        """
        获取媒体文件引用数量
        """
        try:
            if not file_id:
                return 0

            self.db_cursor.execute("""
                SELECT COUNT(*)
                FROM note
                WHERE %s::text = ANY("fileIds");
            """, [file_id])

            result = self.db_cursor.fetchone()
            return result[0] if result else 0

        except Exception as e:
            print(f"获取文件引用数量失败 (file_id: {file_id}): {str(e)}")
            return 0

    def is_file_local(self, file_id):
        """
        判断是否为本地存储文件
        """
        try:
            if not file_id:
                return False

            self.db_cursor.execute("""
                SELECT "isLink"
                FROM drive_file
                WHERE id = %s::text;
            """, [file_id])

            result = self.db_cursor.fetchone()
            return result[0] if result and result[0] is not None else False

        except Exception as e:
            print(f"检查文件是否本地存储失败 (file_id: {file_id}): {str(e)}")
            return False

    def get_single_files(self, start_date, end_date, redis_conn: RedisConnection):
        """
        获取在一段时间内所有的单独文件id列表，使用优化的批量查询
        """
        start_id = generate_id(int(start_date.timestamp() * 1000))
        end_id = generate_id(int(end_date.timestamp() * 1000))
        print(f"扫描文件范围: {start_id}-{end_id}")

        # 使用更高效的计数查询
        try:
            self.db_cursor.execute(
                '''SELECT COUNT(*) FROM drive_file
                WHERE id BETWEEN %s::text AND %s::text
                AND "isLink" IS TRUE
                AND "userHost" IS NOT NULL''',
                [start_id, end_id]
            )
            result = self.db_cursor.fetchone()
            total_count = result[0] if result else 0
            print(f"预计扫描文件数量: {total_count:,}")
        except Exception as e:
            print(f"获取文件总数失败: {str(e)}")
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor()
            except Exception:
                pass
            return

        processed = 0
        deleted = 0
        batch_size = 2000  # 增加批处理大小
        last_id = start_id
        max_retries = 3
        max_iterations = (total_count // batch_size) + 100
        iteration_count = 0
        
        # 添加性能监控
        import time
        scan_start_time = time.time()

        with tqdm(total=total_count, desc="扫描单独文件") as pbar:
            while processed < total_count and iteration_count < max_iterations:
                iteration_count += 1
                batch_start_time = time.time()

                try:
                    # 使用优化的查询，一次性获取所需的所有信息
                    self.db_cursor.execute(
                        '''SELECT df.id, df."userHost"
                        FROM drive_file df
                        WHERE df.id > %s::text 
                        AND df.id <= %s::text
                        AND df."isLink" IS TRUE
                        AND df."userHost" IS NOT NULL
                        ORDER BY df.id
                        LIMIT %s''',
                        [last_id, end_id, batch_size]
                    )
                    results = self.db_cursor.fetchall()
                    
                    if not results:
                        print("没有更多结果，退出扫描")
                        break

                    file_ids = [result[0] for result in results]
                    last_id = file_ids[-1]

                    # 并行处理文件检查
                    files_to_delete_batch = self._process_files_batch_optimized(
                        file_ids, redis_conn, max_retries)
                    
                    deleted += files_to_delete_batch
                    current_batch_size = len(file_ids)
                    processed += current_batch_size

                    # 计算性能指标
                    batch_time = time.time() - batch_start_time
                    total_time = time.time() - scan_start_time
                    avg_speed = processed / total_time if total_time > 0 else 0

                    pbar.update(current_batch_size)
                    pbar.set_postfix({
                        '已处理': f"{processed:,}",
                        '待删除': f"{deleted:,}",
                        '速度': f"{avg_speed:.0f}/s",
                        '批次时间': f"{batch_time:.1f}s"
                    })

                except Exception as e:
                    print(f"处理文件批次失败，跳过当前批次: {str(e)}")
                    try:
                        self.db_conn.rollback()
                        self.db_cursor = self.db_conn.cursor()
                        last_id_increment = int(last_id, 16) + batch_size if last_id else batch_size
                        last_id = format(last_id_increment, 'x')
                        print(f"跳转到新的起始ID: {last_id}")
                    except Exception as conn_error:
                        print(f"重新初始化连接失败: {str(conn_error)}")
                        break

        total_scan_time = time.time() - scan_start_time
        avg_speed = processed / total_scan_time if total_scan_time > 0 else 0
        
        print(f"\n文件扫描完成:")
        print(f"- 总处理时间: {total_scan_time:.1f}秒")
        print(f"- 平均速度: {avg_speed:.0f} 文件/秒")
        print(f"- 找到 {deleted:,} 个单独文件需要删除")
        
    def _process_files_batch_optimized(self, file_ids, redis_conn, max_retries):
        """
        优化的批量文件处理
        """
        deleted_count = 0
        
        # 过滤已缓存的文件
        uncached_file_ids = []
        try:
            # 使用管道批量检查缓存状态
            pipeline = redis_conn.pipeline()
            for fid in file_ids:
                pipeline.sismember('file_cache', fid)
            cache_results = redis_conn.execute(lambda: pipeline.execute())
            
            uncached_file_ids = [fid for fid, is_cached in zip(file_ids, cache_results) if not is_cached]
        except Exception as e:
            print(f"批量检查缓存失败: {str(e)}")
            uncached_file_ids = file_ids  # 保守处理

        if not uncached_file_ids:
            return 0

        # 批量处理文件信息
        retry_count = 0
        while retry_count < max_retries:
            try:
                # 使用单一优化查询获取所有需要的信息
                file_info = self._get_file_info_optimized(uncached_file_ids)
                
                # 找出单独文件并批量添加到Redis
                files_to_delete = []
                for file_id, info in file_info.items():
                    if info['ref_count'] == 0 and not info['is_avatar_banner']:
                        files_to_delete.append(file_id)
                
                if files_to_delete:
                    # 批量添加到Redis
                    redis_conn.execute(
                        lambda: redis_conn.client.sadd('files_to_delete', *files_to_delete)
                    )
                    deleted_count = len(files_to_delete)
                
                break
                
            except Exception as e:
                retry_count += 1
                if retry_count >= max_retries:
                    print(f"批量处理文件失败，已达最大重试次数: {str(e)}")
                else:
                    print(f"批量处理文件失败，重试 {retry_count}/{max_retries}: {str(e)}")
                    import time
                    time.sleep(0.5)
                
                try:
                    self.db_conn.rollback()
                    self.db_cursor = self.db_conn.cursor()
                except Exception:
                    pass
        
        return deleted_count

    def _get_file_info_optimized(self, file_ids):
        """
        使用单一优化查询获取文件的所有相关信息
        """
        if not file_ids:
            return {}
        
        # 使用CTE和左连接的优化查询，一次性获取所有信息
        self.db_cursor.execute("""
            WITH file_list AS (
                SELECT unnest(%s::text[]) as file_id
            ),
            file_refs AS (
                SELECT 
                    unnest(n."fileIds"::text[]) as file_id, 
                    count(*) as ref_count
                FROM note n
                WHERE n."fileIds"::text[] && %s::text[]
                GROUP BY unnest(n."fileIds"::text[])
            ),
            avatar_banner_files AS (
                SELECT DISTINCT "avatarId" as file_id FROM public.user 
                WHERE "avatarId" = ANY(%s::text[])
                UNION
                SELECT DISTINCT "bannerId" as file_id FROM public.user 
                WHERE "bannerId" = ANY(%s::text[])
            )
            SELECT 
                fl.file_id,
                COALESCE(fr.ref_count, 0) as ref_count,
                CASE WHEN abf.file_id IS NOT NULL THEN true ELSE false END as is_avatar_banner
            FROM file_list fl
            LEFT JOIN file_refs fr ON fl.file_id = fr.file_id
            LEFT JOIN avatar_banner_files abf ON fl.file_id = abf.file_id
        """, [file_ids, file_ids, file_ids, file_ids])
        
        results = self.db_cursor.fetchall()
        return {
            row[0]: {
                'ref_count': row[1],
                'is_avatar_banner': row[2]
            }
            for row in results
        }

    def get_file_references_batch(self, file_ids):
        """
        批量获取文件引用数
        """
        if not file_ids:
            return {}

        try:
            # 确保 file_ids 是列表类型
            file_ids = list(file_ids)

            # 修复：使用明确的类型转换并改进查询
            self.db_cursor.execute(
                """
                WITH file_refs AS (
                    SELECT unnest(n."fileIds"::text[]) as file_id, count(*) as ref_count
                    FROM note n
                    WHERE n."fileIds"::text[] && %s::text[]
                    GROUP BY unnest(n."fileIds"::text[])
                )
                SELECT f.id, COALESCE(fr.ref_count, 0) as ref_count
                FROM unnest(%s::text[]) as f(id)
                LEFT JOIN file_refs fr ON fr.file_id = f.id
                """,
                [file_ids, file_ids]
            )

            # 检查游标是否有效
            if self.db_cursor is None or not hasattr(self.db_cursor, 'fetchall'):
                print("警告：数据库游标无效")
                return {}

            results = self.db_cursor.fetchall()
            if not results:
                return {}

            return dict(results)

        except Exception as e:
            print(f"批量获取文件引用数失败: {str(e)}")
            # 回滚事务并重新初始化连接
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            raise  # 重新抛出异常，让上层处理

    def get_files_info_batch(self, file_ids):
        """
        批量获取文件信息
        """
        if not file_ids:
            return {}

        try:
            # 确保 file_ids 是列表类型
            file_ids = list(file_ids)

            self.db_cursor.execute(
                """
                SELECT id, "isLink", "userHost"
                FROM drive_file
                WHERE id = ANY(%s::text[])
                """,
                [file_ids]
            )

            # 检查游标是否有效
            if self.db_cursor is None or not hasattr(self.db_cursor, 'fetchall'):
                print("警告：数据库游标无效")
                return {}

            results = self.db_cursor.fetchall()
            if not results:
                return {}

            return {
                row[0]: {
                    "isLink": row[1] if row[1] is not None else False,
                    "userHost": row[2]
                }
                for row in results
            }

        except Exception as e:
            print(f"批量获取文件信息失败: {str(e)}")
            # 回滚事务并重新初始化连接
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            raise  # 重新抛出异常，让上层处理

    def check_user_avatar_banner_batch(self, file_ids):
        """
        批量检查文件是否被用作头像或横幅
        """
        if not file_ids:
            return set()

        try:
            # 确保 file_ids 是列表类型
            file_ids = list(file_ids)

            self.db_cursor.execute(
                """
                SELECT DISTINCT "avatarId", "bannerId"
                FROM public.user
                WHERE "avatarId" = ANY(%s::text[]) OR "bannerId" = ANY(%s::text[])
                """,
                [file_ids, file_ids]
            )

            # 检查游标是否有效
            if self.db_cursor is None or not hasattr(self.db_cursor, 'fetchall'):
                print("警告：数据库游标无效")
                return set()

            results = self.db_cursor.fetchall()
            if not results:
                return set()

            used_files = set()
            for avatar_id, banner_id in results:
                if avatar_id:
                    used_files.add(avatar_id)
                if banner_id:
                    used_files.add(banner_id)
            return used_files

        except Exception as e:
            print(f"批量检查用户头像和横幅失败: {str(e)}")
            # 回滚事务并重新初始化连接
            try:
                self.db_conn.rollback()
                self.db_cursor = self.db_conn.cursor()
            except Exception as conn_error:
                print(f"重新初始化连接失败: {str(conn_error)}")
            raise  # 重新抛出异常，让上层处理

    def delete_files_batch(self, file_ids: list[str], batch_size: int = 1000) -> None:
        """
        批量删除文件，采用分批处理方式
        Args:
            file_ids: 要删除的文件ID列表
        """
        if not file_ids:
            return

        # 使用 WITH 和 JOIN 来优化删除操作
        self.db_cursor.execute(
            """
            WITH batch_ids AS (
                SELECT unnest(%s::text[]) AS id
            )
            DELETE FROM drive_file df
            USING batch_ids b
            WHERE df.id = b.id
            """,
            [file_ids]
        )

        self.db_conn.commit()
