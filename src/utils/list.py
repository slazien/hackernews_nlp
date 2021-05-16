from typing import List


def chunk_for_size(input_list: List, chunk_size: int) -> List[List]:
    """
    Returns consecutive sublists of size chunk_size
    :param input_list: list to chunk
    :param chunk_size: size of each sublist
    :return: list of lists (chunks)
    """
    for x in range(0, len(input_list), chunk_size):
        chunk = input_list[x : chunk_size + x]

        yield chunk
