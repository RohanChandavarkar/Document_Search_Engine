state = None
import logging, logging.config

class State:

    def __init__(self):
        self.type = 1
        logging.config.fileConfig('logging.conf')
        # create logger
        self.logger = logging.getLogger('logger_simpleExample')

state = State()