from typing import List

class Subindex:
    def __init__(self, id: int):
        self.id = id
        self.doc_id = 0
        self._files: List[str] = []

    def push_file(self, fpath: str):
        self._files.append(fpath)

    def pop_file(self, fpath: str):
        return self._files.pop()
