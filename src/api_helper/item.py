import logging
from collections import deque
from typing import List, Optional

from ratelimit import limits, sleep_and_retry
from requests import get
from treelib import Tree

from src.entities.item import Item
from src.utils.time import MINUTE

# Rate limit / minute
RATE_LIMIT_DEFAULT = 100000


class ItemAPI:
    def __init__(self):
        self.URL = "https://hacker-news.firebaseio.com/v0/item/"

    def create_url(self, item_id: int) -> str:
        return self.URL + str(item_id) + ".json"

    # Hard rate limit, no exponential backoff
    @sleep_and_retry
    @limits(calls=RATE_LIMIT_DEFAULT, period=MINUTE)
    def get_item(self, item_id: int) -> Item:
        url = self.create_url(item_id=item_id)
        logging.info("making GET request: {}".format(url))
        res = get(url=url)
        if res.status_code != 200:
            logging.error(
                "GET request failed: {}, item_id: {}".format(res.status_code, item_id)
            )
            # raise Exception(
            #     "API response: {}. Item id: {}".format(res.status_code, item_id)
            # )

        logging.info("GET request succeeded for item_id: {}".format(item_id))
        comment = Item().from_res(res.json())

        return comment

    def get_item_batch(self, item_ids: List[int]) -> List[Item]:
        data_out_list = []
        for item_id in item_ids:
            data_out_list.append(self.get_item(item_id=item_id))

        return data_out_list

    def get_all_kids_for_item_id(self, item_id: int) -> Optional[List[Item]]:
        item_kid_list = []
        item = self.get_item(item_id)

        if item.has_kids():
            for kid_id in item.get_kids_ids():
                item_kid_list.append(self.get_item(kid_id))
            return item_kid_list
        else:
            return None

    def create_tree_for_item_id(self, item_id: int) -> Tree:
        logging.info("creating tree for item_id: ".format(item_id))
        tree_out = Tree()
        item = self.get_item(item_id)
        item_kids = self.get_all_kids_for_item_id(item_id)

        # Create root node
        tree_out.create_node(
            tag=str(item.get_id()), identifier=item_id, data=item, parent=None
        )

        if item_kids is not None:
            for item_kid in item_kids:
                tree_out.create_node(
                    tag=item_kid.get_id(),
                    identifier=item_kid.get_id(),
                    parent=item_id,
                    data=item_kid,
                )

        logging.info("created tree for item_id: {}".format(item_id))

        return tree_out

    @sleep_and_retry
    @limits(calls=RATE_LIMIT_DEFAULT, period=MINUTE)
    def get_maxitem_id(self) -> int:
        url = "https://hacker-news.firebaseio.com/v0/maxitem.json"
        logging.info("making GET request: {}".format(url))
        res = get(url=url)

        if res.status_code != 200:
            logging.error("GET request failed: {}".format(res.status_code))
            # raise Exception("API response: {}".format(res.status_code))

        max_item_id = res.json()
        logging.info("GET request succeeded, max_id: {}".format(max_item_id))

        return max_item_id

    def get_root_item(self, item_id: int) -> Optional[Item]:
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

            for kid_id in parent.get_kids_ids():
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
