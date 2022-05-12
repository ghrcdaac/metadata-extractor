import logging
import os
from cumulus_logger import CumulusLogger

class MyLogger:
    pass
[setattr(MyLogger,ele, print) for ele in ['info', 'warning', 'error']]


def get_logger(logger_name='MDX-Processing'):
    
    return CumulusLogger(name=logger_name) if os.getenv('enable_logging', 'false').lower() == 'true' else MyLogger
