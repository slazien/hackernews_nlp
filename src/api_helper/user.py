import logging
from typing import List, Optional

from ratelimit import limits, sleep_and_retry
from requests import get

from src.entities.user import User
from src.utils.time import MINUTE

RATE_LIMIT_DEFAULT = 100000


class UserAPI:
    def __init__(self):
        self.URL = "https://hacker-news.firebaseio.com/v0/user/"

    def create_url(self, user_id: str) -> str:
        """
        Create a URL for calling HN API based on provided user ID
        :param user_id: ID of the user to create the URL for
        :return: URL to the user
        """

        return self.URL + user_id + ".json"

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
        logging.info("making GET request: {}".format(url))
        res = get(url=url)

        if res.status_code != 200:
            logging.warning(
                "GET request failed: {}, user_id: {}".format(res.status_code, user_id)
            )
            # raise Exception(
            #     "API response: {}. User id: {}".format(res.status_code, user)
            # )
            return None
        else:
            logging.info("GET request succeeded for user_id: {}".format(user_id))
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
