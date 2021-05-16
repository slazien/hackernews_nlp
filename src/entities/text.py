from __future__ import annotations


class Text:
    def __init__(self, id_item: int = None, text: str = None):
        self.id_item = id_item
        self.text = text

    def get_id_item(self):
        return self.id_item

    def get_text(self):
        return self.text
