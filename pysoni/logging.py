import time
import logging
import sys



logger = logging.getLogger()
logging.basicConfig(stream=sys.stdout, 
                    format="%(asctime)s [%(threadName)-12.12s] "
                            "[%(levelname)-5.5s]  %(message)s",
                    level=logging.INFO,
                    )

class PysoniLogging(object):
    

    def pysoni_logging(get_logs=True):
        def logging(func):
            def logging_wrapper(*args, **kwargs):
                if get_logs:
                    now = time.time()
                    logger.info(f"Calling function {func.__name__} with args {args}{kwargs}")
                    function_output = func(*args, **kwargs)
                    logger.info(f"function {func.__name__} executed succesfully in {time.time()-now} ms")
                    return function_output

                return func(*args, **kwargs)
            return logging_wrapper
        return logging