
# 
# Copyright (c) 2019, 2020, 2021, John Grundback
# All rights reserved.
# 

import logging
logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)


class GFSLogger():

    @classmethod
    def getLogger(self, name):
        return GFSLogger(name)

    @classmethod
    def getLogLevel(self):
        return None

    def __init__(self, name, **kwargs):
        self._name = name
        self._logger = logging.getLogger(name)

    def debug(self, *args, **kwargs):
        self._logger.debug(*args, **kwargs)

    def info(self, *args, **kwargs):
        self._logger.info(*args, **kwargs)

    def warning(self, *args, **kwargs):
        self._logger.warning(*args, **kwargs)

    def error(self, *args, **kwargs):
        self._logger.error(*args, **kwargs)

    def critical(self, *args, **kwargs):
        self._logger.critical(*args, **kwargs)

    def exception(self, *args, **kwargs):
        self._logger.exception(*args, **kwargs)

    def log(self, lvl, *args, **kwargs):
        self._logger.log(lvl, *args, **kwargs)



__all__ = [
    'GFSLogger'
]

__default__ = 'GFSLogger'
