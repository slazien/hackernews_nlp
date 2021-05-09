from typing import List


def chunk_for_size(input_list: List, chunk_size: int) -> List[List]:
    for x in range(0, len(input_list), chunk_size):
        chunk = input_list[x : chunk_size + x]

        yield chunk
