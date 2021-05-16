from __future__ import annotations

import logging
from typing import Any, List, Optional


class Item:
    def __init__(
        self,
        id: int = None,
        deleted: Optional[bool] = None,
        type: str = None,
        by: str = None,
        time: int = None,
        text: str = None,
        dead: Optional[bool] = None,
        parent: Optional[int] = None,
        poll: Optional[int] = None,
        kids: Optional[List[int]] = None,
        url: Optional[str] = None,
        score: int = None,
        title: Optional[str] = None,
        parts: Optional[List[int]] = None,
        descendants: Optional[int] = None,
    ):
        self.id = id
        self.deleted = deleted
        self.type = type
        self.by = by
        self.time = time
        self.text = text
        self.dead = dead
        self.parent = parent
        self.poll = poll
        self.kids = kids
        self.url = url
        self.score = score
        self.title = title
        self.parts = parts
        self.descendants = descendants

    def get_id(self) -> int:
        """
        Get the ID of the Item object
        :return: ID of the Item
        """
        return self.id

    def has_kids(self) -> bool:
        """
        Check if an item has any kids
        :return: True if any kids exist, False otherwise
        """
        if self.kids is None or len(self.kids) == 0:
            return False
        else:
            return True

    def get_kids_ids(self) -> Optional[List[int]]:
        """
        Get kid IDs of an Item
        :return: list of kid IDs if any exist, None otherwise
        """
        if self.has_kids():
            return self.kids
        else:
            return None

    def has_parent(self) -> bool:
        """
        Check if an Item has a parent
        :return: True if parent exists, False otherwise
        """
        return self.parent is not None

    def get_parent_id(self) -> Optional[id]:
        """
        Get item parent's ID
        :return: Id of the parent if exists, None otherwise
        """
        if self.has_parent():
            return self.parent
        else:
            return None

    @staticmethod
    def from_api_call(data: dict) -> Optional[Item]:
        """
        Create an Item object from a dict
        :param data: dict of key:value pairs with field values from the API
        :return: an Item object
        """

        if data is None:
            return None

        return Item(
            id=data["id"] if "id" in data else None,
            deleted=data["deleted"] if "deleted" in data else None,
            type=data["type"] if "type" in data else None,
            by=data["by"] if "by" in data else None,
            time=data["time"] if "time" in data else None,
            text=data["text"] if "text" in data else None,
            dead=data["dead"] if "dead" in data else None,
            parent=data["parent"] if "parent" in data else None,
            poll=data["poll"] if "poll" in data else None,
            kids=data["kids"] if "kids" in data else None,
            url=data["url"] if "url" in data else None,
            score=data["score"] if "score" in data else None,
            title=data["title"] if "title" in data else None,
            parts=data["parts"] if "parts" in data else None,
            descendants=data["descendants"] if "descendants" in data else None,
        )

    @staticmethod
    def from_tuple(tup: tuple) -> Optional[Item]:
        """
        Create an Item object from a tuple (order of values must be the same as in the `from_api_call` method)
        :param tup: tuple of values to create an Item from
        :return: Item object if the number of values matches the number of needed fields, None otherwise
        """
        # If there aren't 15 columns
        if len(tup) != 15:
            logging.warning(
                "tried creating item from tuple with < 15 columns: {}".format(tup)
            )
            return None
        else:
            return Item(
                id=tup[0],
                deleted=tup[1],
                type=tup[2],
                by=tup[3],
                time=tup[4],
                text=tup[5],
                dead=tup[6],
                parent=tup[7],
                poll=tup[8],
                kids=tup[9],
                url=tup[10],
                score=tup[11],
                title=tup[12],
                parts=tup[13],
                descendants=tup[14],
            )

    @staticmethod
    def from_db_call(query_res: List) -> Optional[Item]:
        """
        Create an Item object from the results of a DB query
        :param query_res: result of a DB query
        :return: Item object if number of values matches the number of fields, None otherwise
        """
        data = query_res[0]
        return Item().from_tuple(data)

    def get_property(self, property_name: str) -> Optional[Any]:
        """
        Get the value of a given Item's property
        :param property_name: name of the property to get the value of
        :return: property value if exists, None otherwise
        """
        if hasattr(self, property_name):
            return getattr(self, property_name)
        else:
            return None

    def __repr__(self):
        return (
            "Item("
            "id={},"
            "deleted={},"
            "type={},"
            "by={},"
            "time={},"
            "text={},"
            "dead={},"
            "parent={},"
            "poll={},"
            "kids={},"
            "url={},"
            "score={},"
            "title={},"
            "parts={},"
            "descendants={}".format(
                self.id,
                self.deleted,
                self.type,
                self.by,
                self.time,
                self.text,
                self.dead,
                self.parent,
                self.poll,
                self.kids,
                self.url,
                self.score,
                self.title,
                self.parts,
                self.descendants,
            )
        )

    def __str__(self):
        return (
            "Item("
            "id={},"
            "deleted={},"
            "type={},"
            "by={},"
            "time={},"
            "text={},"
            "dead={},"
            "parent={},"
            "poll={},"
            "kids={},"
            "url={},"
            "score={},"
            "title={},"
            "parts={},"
            "descendants={}".format(
                self.id,
                self.deleted,
                self.type,
                self.by,
                self.time,
                self.text,
                self.dead,
                self.parent,
                self.poll,
                self.kids,
                self.url,
                self.score,
                self.title,
                self.parts,
                self.descendants,
            )
        )
