# from src.entities.item import Item
# from src.db.connection import DBConnection
#
#
# class ItemInserter:
#     def __init__(self, conn: DBConnection, table_name: str = "items"):
#         self.conn = conn.get()
#         self.cursor = self.conn.cursor()
#         self.conn.autocommit = True
#         self.table_name = table_name
#
#     def insert_item(self, item: Item):
#         id = item.get_property("id")
#         deleted = item.get_property("deleted")
#         type = item.get_property("type")
#         by = item.get_property("by")
#         time = item.get_property("time")
#         text = item.get_property("text")
#         dead = item.get_property("dead")
#         parent = item.get_property("parent")
#         poll = item.get_property("poll")
#         kids = item.get_property("kids")
#         url = item.get_property("url")
#         score = item.get_property("score")
#         title = item.get_property("title")
#         parts = item.get_property("parts")
#         descendants = item.get_property("descendants")
#
#         sql = """
#         INSERT INTO {} (id, deleted, type, by, time, text, dead, parent, poll, kids, url, score, title, parts, descendants)
#         VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#         """.format(self.table_name)
#
#         self.cursor.execute(
#             sql,
#             (
#                 id,
#                 deleted,
#                 type,
#                 by,
#                 time,
#                 text,
#                 dead,
#                 parent,
#                 poll,
#                 kids,
#                 url,
#                 score,
#                 title,
#                 parts,
#                 descendants,
#             ),
#         )
