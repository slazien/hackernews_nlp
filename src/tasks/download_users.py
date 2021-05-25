import logging

import luigi
from tqdm import tqdm

from src.api_helper.user import UserAPI
from src.db.connection import DBConnection
from src.db.constants import (
    DB_NAME_HACKERNEWS,
    DB_PASSWORD,
    PRIMARY_KEY_NAME_USERS,
    TABLE_NAME_USERS,
)
from src.db.getters import UserGetter
from src.db.inserters import UserInserter


class TaskDownloadUsers(luigi.Task):
    user_ids = luigi.ListParameter()

    def run(self):
        """
        Run the download task using Luigi for all user IDs currently present in the "items" table
        :return:
        """
        logging.info("starting luigi task: %s", self.__class__)
        conn = DBConnection(
            user="postgres", password=DB_PASSWORD, db_name=DB_NAME_HACKERNEWS
        )

        user_getter = UserGetter(conn, TABLE_NAME_USERS, PRIMARY_KEY_NAME_USERS)
        user_inserter = UserInserter(conn, TABLE_NAME_USERS, PRIMARY_KEY_NAME_USERS)
        user_api = UserAPI()

        user_ids_intersection = set(user_getter.get_all_user_ids()).intersection(
            self.user_ids
        )

        for user_id in tqdm(user_ids_intersection):
            current_user = user_api.get_user(user_id=user_id)
            if current_user is not None:
                user_inserter.insert_user(current_user)

        logging.info("finished task: %s", self.__class__)
