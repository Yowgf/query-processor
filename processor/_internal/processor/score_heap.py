import heapq

class ScoreHeap:
    def __init__(self):
        self._l = []

    def __len__(self):
        return len(self._l)

    def pop(self):
        score, docid = heapq.heappop(self._l)
        return docid, score

    def push(self, docid, score):
        return heapq.heappush(self._l, (score, docid))
