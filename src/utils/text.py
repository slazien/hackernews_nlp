def is_string_empty(text: str) -> bool:
    """
    Check if input string is empty for the purpose of preprocessing
    :param text: input string
    :return: True if text is empty, False otherwise
    """

    return bool(text == "" or text is None)
