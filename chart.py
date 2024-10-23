import time
import psycopg

def clean_chart(db_info, days):
    """
    删除指定天数前的chart数据
    """
    pg_connection = psycopg.connect(f"dbname={db_info[2]} user={db_info[3]} password={db_info[4]} host={db_info[0]} port={db_info[1]}")
    delete_chart_data(pg_connection, days)

def delete_chart_data(db_connection, days):
    """
    删除指定天数前的chart数据
    """
    cursor = db_connection.cursor()
    current_time = time.time()
    delete_time = int(current_time - 60 * 60 * 24 * days)

    delete_queries = [
        """DELETE FROM __chart__hashtag WHERE "date" < %s""",
        """DELETE FROM __chart_day__hashtag WHERE "date" < %s""",
        """DELETE FROM __chart__per_user_notes WHERE "date" < %s""",
        """DELETE FROM __chart_day__per_user_notes WHERE "date" < %s""",
        """DELETE FROM __chart__instance WHERE "date" < %s"""
    ]

    for query in delete_queries:
        cursor.execute(query, [delete_time])
        db_connection.commit()
