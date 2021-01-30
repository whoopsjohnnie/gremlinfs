
# 
# Copyright (c) 2019, 2020, 2021, John Grundback
# All rights reserved.
# 



class GFSError(Exception):

    def __init__(self, path = None):
        self.path = path



class GFSExistsError(GFSError):

    def __init__(self, path = None):
        self.path = path



class GFSNotExistsError(GFSError):

    def __init__(self, path = None):
        self.path = path



class GFSIsFileError(GFSError):

    def __init__(self, path = None):
        self.path = path



class GFSIsFolderError(GFSError):

    def __init__(self, path = None):
        self.path = path
