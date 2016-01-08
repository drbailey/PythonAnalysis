__author__  = 'drew bailey'
__version__ = 0.1

"""
Gets data from web pages or web APIs.
"""

# extracts data from web pages, not complete.


class NetData(object):
    """
    Parses web pages.
    """
    def __init__(self):
        # import proxies
        self.http = None
        self.ftp = None
