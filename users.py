class User:
    """
    用户相关信息
    """

    is_local = False
    is_vip = False

    def __init__(self, db_connection, user_id):
        """
        初始化用户信息
        """
        cursor = db_connection.cursor()
        cursor.execute(
            """SELECT host, "followersCount", "followingCount" FROM public.user WHERE id = %s""", [user_id])
        user_data = cursor.fetchone()
        if user_data[0] is None:
            self.is_local = True
        if (user_data[1] + user_data[2]) > 0:
            self.is_vip = True
        self.user_data = user_data
