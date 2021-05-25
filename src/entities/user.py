from __future__ import annotations

import logging
from typing import Any, List, Optional


class User:
    def __init__(
        self,
        id: str = None,
        created: int = None,
        karma: int = None,
        about: Optional[str] = None,
        submitted: Optional[List[int]] = None,
    ):
        self.id = id
        self.created = created
        self.karma = karma
        self.about = about
        self.submitted = submitted

    def get_id(self) -> str:
        """
        Get the ID of the User object
        :return: ID of the User
        """
        return self.id

    @staticmethod
    def from_api_call(data: dict) -> Optional[User]:
        """
        Create a User object from a dict
        :param data: dict of key:value pairs with field values from the API
        :return: a User object
        """

        if data is None:
            return None

        return User(
            id=data["id"] if "id" in data else None,
            created=data["created"] if "created" in data else None,
            karma=data["karma"] if "karma" in data else None,
            about=data["about"] if "about" in data else None,
            submitted=data["submitted"] if "submitted" in data else None,
        )

    @staticmethod
    def from_tuple(tup: tuple) -> Optional[User]:
        """
        Create a User object from a tuple (order of values must be the same as in the `from_api_call` method)
        :param tup: tuple of values to create a User from
        :return: User object if the number of values matches the number of needed fields, None otherwise
        """
        # If there aren't 5 columns
        if len(tup) != 5:
            logging.warning("tried creating user from tuple with < 5 columns: %s", tup)
            return None

        return User(
            id=tup[0], created=tup[1], karma=tup[2], about=tup[3], submitted=tup[4]
        )

    @staticmethod
    def from_db_call(query_res: List) -> Optional[User]:
        """
        Create a User object from the results of a DB query
        :param query_res: result of a DB query
        :return: User object if number of values matches the number of fields, None otherwise
        """
        data = query_res[0]
        return User().from_tuple(data)

    def get_property(self, property_name: str) -> Optional[Any]:
        """
        Get the value of a given User's property
        :param property_name: name of the property to get the value of
        :return: property value if exists, None otherwise
        """
        if hasattr(self, property_name):
            return getattr(self, property_name)

        return None

    def __repr__(self):
        return (
            "User("
            "id={},"
            "created={},"
            "karma={},"
            "about={},"
            "submitted={}".format(
                self.id, self.created, self.karma, self.about, self.submitted
            )
        )

    def __str__(self):
        return (
            "User("
            "id={},"
            "created={},"
            "karma={},"
            "about={},"
            "submitted={}".format(
                self.id, self.created, self.karma, self.about, self.submitted
            )
        )
