import json

from treelib import Tree


def tree_to_dict(tree: Tree, with_data=True) -> dict:
    tree_dict = dict(json.loads(tree.to_dict(with_data=with_data)))
