__author__  = 'drew bailey'
__version__ = 0.1

"""
Gets data from outlook attachments in a user friendly way.
"""

from ..com.comoutlook import ComOutlook


class OutlookData(object):
    """
    data functions from ComOutlook, operates like a function... may include other data handling methods later
    takes rules as a tuple of (regex string to search, property to search) only matches string properties in mail items
    PATH is where to save, defaults to cwd\data\
    saverule is a regex string attachments have to include to be saved
    """
    def __init__(self, rules, path=None, saverule='.xls', datamode='all'):
        self.rules = rules
        self.path = path
        self.saverule = saverule
        self.datamode = datamode
        self.c = ComOutlook(env='pc')
        self.outlook_data()

    def outlook_data(self):
        objs = self.c.filter_mail(self.rules)
        self.c.save_attachments(objects=objs, path=self.path, saverule=self.saverule, datamode=self.datamode)

    # def from_files(self, files, ranges=None):
    #     exts = self.file_ext_checker(files)
    #     if '.xlsx':
    #     for range in ranges:
    #         # collect data
    #         pass
    #
    # def file_ext_checker(self, files):
    #     pass

    def __cleanup(self):
        del self.c
