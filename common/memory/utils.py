from collections import Iterable
import sys

def sizeof(obj):
    if not isinstance(obj, Iterable) or isinstance(obj, str):
        return sys.getsizeof(obj)
    elif isinstance(obj, dict):
        sum = sys.getsizeof(obj)
        for k in obj:
            sum += sizeof(obj[k])
        return sum
    else:
        sum = sys.getsizeof(obj)
        for item in obj:
            sum += sizeof(item)
        return sum
