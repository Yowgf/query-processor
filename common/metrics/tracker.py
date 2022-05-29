import tracemalloc

class Tracker:
    def __init__(self):
        pass

    def __del__(self):
        tracemalloc.stop()

    def track(self):
        tracemalloc.start()

def log_memory_usage(logger):
    current_bytes, peak_bytes = tracemalloc.get_traced_memory()
    MB = 1024 * 1024
    current_mb = current_bytes / MB
    peak_mb = peak_bytes / MB
    logger.info(f"Current memory usage: {current_mb:.2f}MB. "+
                f"Peak memory usage: {peak_mb:.2f}MB")
