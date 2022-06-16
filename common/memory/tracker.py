import tracemalloc

from .defs import MEGABYTE

class Tracker:
    tracking = False

    def __init__(self):
        Tracker.tracking = True

    def __del__(self):
        tracemalloc.stop()

    def track(self):
        tracemalloc.start()

def log_memory_usage(logger):
    if not Tracker.tracking:
        return

    current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    current_mb = current_bytes / MEGABYTE
    peak_mb = peak_bytes / MEGABYTE
    logger.info(f"Current memory usage: {current_mb:.2f}MB. "+
                f"Peak memory usage: {peak_mb:.2f}MB.")
