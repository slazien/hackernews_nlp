def is_table_exists(conn, table_name: str) -> bool:
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
                SELECT EXISTS (
                   SELECT FROM pg_tables
                   WHERE  schemaname = 'public'
                   AND    tablename  = '{}'
                   );
            """.format(
                table_name
            )
        )
        res = cursor.fetchall()
        return res[0][0]
    except Exception as e:
        raise Exception("Exception: {}".format(e))
