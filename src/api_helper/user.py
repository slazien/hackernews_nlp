"""
A module providing convenience methods for using HN's User API
"""

import logging
from typing import List, Optional

from ratelimit import limits, sleep_and_retry
from requests import get

from src.entities.user import User
from src.utils.time import MINUTE

RATE_LIMIT_DEFAULT = 100000


class UserAPI:
    """
    Convenience class for interacting with HN's API for getting Users
    """

    def __init__(self):
        self.url = "https://hacker-news.firebaseio.com/v0/user/"

    def create_url(self, user_id: str) -> str:
        """
        Create a url for calling HN API based on provided user ID
        :param user_id: ID of the user to create the url for
        :return: url to the user
        """

        return self.url + user_id + ".json"

    # Hard rate limit, no exponential backoff
    @sleep_and_retry
    @limits(calls=RATE_LIMIT_DEFAULT, period=MINUTE)
    def get_user(self, user_id: str) -> Optional[User]:
        """
        Get a user with a given ID
        :param user_id: ID of the user to get
        :return: an object of class User (or None if an item does not exist)
        """
        url = self.create_url(user_id=user_id)
        logging.info("making GET request: %s", url)
        res = get(url=url)

        if res.status_code != 200:
            logging.warning(
                "GET request failed: %s, user_id: %s", res.status_code, user_id
            )
            # raise Exception(
            #     "API response: {}. User id: {}".format(res.status_code, user)
            # )
            return None

        logging.info("GET request succeeded for user_id: %s", user_id)
        user = User().from_api_call(res.json())
        return user

    def get_user_batch(self, user_ids: List[int]) -> List[User]:
        """
        Get multiple users based on a list of provided IDs
        :param user_ids: list of user IDs to get
        :return: list of User objects (or None if user doesn't exist)
        """
        data_out_list = []
        for user_id in user_ids:
            data_out_list.append(self.get_user(user_id=user_id))

        return data_out_list
