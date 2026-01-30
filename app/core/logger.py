import sys
from loguru import logger

def setup_logger():
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("parus.log", rotation="10 MB")