import psycopg2


class DBConnection:
    def __init__(self, user: str, password: str, db_name: str):
        pass
        self.conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host="127.0.0.1",
            port="5432",
        )
        self.cursor = self.conn.cursor()

    def get_conn(self):
        return self.conn

    def close_conn(self):
        return self.conn.close_conn()

    def get_cursor(self):
        return self.cursor

    def close_cursor(self):
        self.cursor.close()
