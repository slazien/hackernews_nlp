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
        return self.id

    def has_kids(self) -> bool:
        if self.kids is None or len(self.kids) == 0:
            return False
        else:
            return True

    def get_kids_ids(self) -> Optional[List[int]]:
        if self.has_kids():
            return self.kids
        else:
            return None

    def has_parent(self) -> bool:
        return self.parent is not None

    def get_parent_id(self) -> Optional[id]:
        if self.has_parent():
            return self.parent
        else:
            return None

    @staticmethod
    def from_res(data: dict):
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

    def get_property(self, property_name: str) -> Optional[Any]:
        if hasattr(self, property_name):
            return getattr(self, property_name)
        else:
            return None

    def __repr__(self):
        return (
            "Comment("
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
            "Comment("
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
