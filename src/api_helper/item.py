import logging
from collections import deque
from typing import List, Optional

from ratelimit import limits, sleep_and_retry
from requests import get
from tqdm import tqdm
from treelib import Tree

from src.entities.item import Item
from src.utils.time import MINUTE

# Rate limit / minute
RATE_LIMIT_DEFAULT = 100000


class ItemAPI:
    def __init__(self):
        self.URL = "https://hacker-news.firebaseio.com/v0/item/"

    def create_url(self, item_id: int) -> str:
        """
        Create a URL for calling HN API based on provided item ID
        :param item_id: ID of the item to create the URL for
        :return: URL to the item
        """
        return self.URL + str(item_id) + ".json"

    # Hard rate limit, no exponential backoff
    @sleep_and_retry
    @limits(calls=RATE_LIMIT_DEFAULT, period=MINUTE)
    def get_item(self, item_id: int) -> Optional[Item]:
        """
        Get an item with a given ID
        :param item_id: ID of the item to get
        :return: an object of class Item (or None if an item does not exist)
        """
        url = self.create_url(item_id=item_id)
        logging.info("making GET request: {}".format(url))
        res = get(url=url)
        if res.status_code != 200:
            logging.warning(
                "GET request failed: {}, item_id: {}".format(res.status_code, item_id)
            )
            # raise Exception(
            #     "API response: {}. Item id: {}".format(res.status_code, item_id)
            # )
            return None
        else:
            logging.info("GET request succeeded for item_id: {}".format(item_id))
            comment = Item().from_api_call(res.json())
            return comment

    def get_item_batch(self, item_ids: List[int]) -> List[Item]:
        """
        Get multiple items based on a list of provided IDs
        :param item_ids: list of item IDs to get
        :return: list of Item objects (or None if item doesn't exist)
        """
        data_out_list = []
        for item_id in item_ids:
            data_out_list.append(self.get_item(item_id=item_id))

        return data_out_list

    def get_all_kids_for_item_id(self, item_id: int) -> Optional[List[Item]]:
        """
        Get all kid items for a given item ID
        :param item_id: item ID to get children for
        :return: list of children IDs (or None if no children exist)
        """
        item_kid_list = []
        item = self.get_item(item_id)

        if item.has_kids():
            for kid_id in item.get_kids_ids():
                item_kid_list.append(self.get_item(kid_id))
            return item_kid_list
        else:
            return None

    @sleep_and_retry
    @limits(calls=RATE_LIMIT_DEFAULT, period=MINUTE)
    def get_maxitem_id(self) -> Optional[int]:
        """
        Get the ID of the latest item
        :return: Id of the latest item
        """
        url = "https://hacker-news.firebaseio.com/v0/maxitem.json"
        logging.info("making GET request: {}".format(url))
        res = get(url=url)

        if res.status_code != 200:
            logging.error("GET request failed: {}".format(res.status_code))
            # raise Exception("API response: {}".format(res.status_code))
            return None
        else:
            max_item_id = res.json()
            logging.info("GET request succeeded, max_id: {}".format(max_item_id))

            return max_item_id

    def get_root_item(self, item_id: int) -> Optional[Item]:
        """
        Get the ID of the root item for a given item ID
        :param item_id: item ID to get the root item ID for
        :return: root item (object of type Item or None if the item doesn't exist)
        """
        logging.info("getting root item for item_id: {}".format(item_id))
        item_cache = []

        # Get initial item
        item = self.get_item(item_id)

        # Traverse the comment tree until we hit the root node (no parents)
        while item.has_parent():
            item = self.get_item(item.get_parent_id())
            item_cache.append(item)

        if len(item_cache) > 0:
            logging.info(
                "got root item for item_id: {}, id: {}".format(item_id, item_cache[-1])
            )
            return item_cache[-1]
        else:
            logging.warning("found no root item for item_id: {}".format(item_id))
            return None

    def build_tree_for_root_item(self, item: Item) -> Tree:
        """
        Given a root item, build a complete tree of all child items
        :param item: root item to build a tree for
        :return: an object of type Tree containing the root item and all of its children
        """
        item_id = item.get_id()
        logging.info("creating tree for root item_id: {}".format(item_id))
        tree = Tree()
        stack = deque([])

        # Preorder contains ids of all visited nodes
        preorder = [item_id]

        # Add the root node to the tree
        tree.create_node(
            tag=item_id,
            identifier=item_id,
            parent=None,
            data=item,
        )
        stack.append(item)

        while len(stack) > 0:
            # Flag checks if all children nodes have been visited
            flag = 0

            # CASE 1: if the top of the stack is a leaf node (== doesn't have children),
            # remove it from the stack
            if not stack[-1].has_kids():
                stack.pop()

            # CASE 2: if the top of the stack is parent with children
            else:
                parent = stack[-1]

            for kid_id in tqdm(parent.get_kids_ids()):
                # Get the kid item
                kid = self.get_item(kid_id)

                # Insert the kid into the tree
                if kid_id not in tree:
                    tree.create_node(
                        tag=kid_id, identifier=kid_id, parent=parent.get_id(), data=kid
                    )

                if kid_id not in preorder:
                    flag = 1
                    stack.append(kid)
                    preorder.append(kid_id)
                    break

            if flag == 0:
                stack.pop()

        logging.info("finished creating tree for item_id: {}".format(item_id))
        return tree
