from typing import List

class Subindex:
    def __init__(self, id: int):
        self.id = id
        self.docid = 0
        self._files: Mapping[str, int] = {}

    def __len__(self):
        return len(self._files)

    def push_file(self, fpath: str, checkpoint=0):
        assert fpath not in self._files
        self._files[fpath] = checkpoint

    def pop_file(self):
        return self._files.popitem()
