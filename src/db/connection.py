import psycopg2


class DBConnection:
    def __init__(self, user: str, password: str, db_name: str):
        self.conn = psycopg2.connect(
            database=db_name,
            user=user,
            password=password,
            host="127.0.0.1",
            port="5432",
        )
        self.cursor = self.conn.cursor()

    def get(self):
        return self.conn

    def close(self):
        return self.conn.close()
