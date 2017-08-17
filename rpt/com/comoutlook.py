__author__  = 'drew bailey'
__version__ = 0.1


"""
THIS IS NOT IN USE.
Outlook Com handling via win32com. Primarily used to extract data from Outlook's currently signed in user's mail box.
Uses regex filters to determine valid mail data_objects, can save attachments if criteria are met.
"""


from ..config import VOLUME
from .mipl import properties, get
from datetime import datetime
from win32com import client
import pythoncom
import re
import os


class ComOutlook(object):

    def __init__(self, path=None, env='server'):
        """
        :param path: PATH to task directory if non-standard
        :return:
        """
        if path:
            self.path = path
        else:
            self.path = os.getcwd()  # for object saving
        self.env = env
        self.volume = VOLUME
        self.outlook = client.Dispatch("Outlook.Application")
        self.namespace = self.outlook.GetNamespace('MAPI')
        self.profile = self.namespace.CurrentProfileName
        self.user = self.namespace.CurrentUser
        self.server = self.namespace.ExchangeMailboxServerName
        self.allitems = self.namespace.Folders
        self.rootitems = {}  # all base level items (data_objects with a Subject property)
        self.directory = {}  # structure preserved dict object
        print 'user ', self.user.Name, ' as ', self.profile, ' logged into ', self.server, '...'
        print 'populating items:'
        self.index = 0
        self.__fill_items()

    def process(self, comObject, sub=-1):
        items = {}
        if hasattr(comObject, 'Subject'):
            name, value = self._get_items(comObject, sub=sub+1)
            items[name] = value
            self.rootitems[self.index] = value
            self.index += 1
        elif hasattr(comObject, 'Name'):
            name, value = self._get_folder(comObject, sub=sub+1)
            items[name] = value
        elif hasattr(comObject, 'Count'):
            value = self._get_folders(comObject, sub=sub+1)
            name = self.__get_unused_name(value)
            items[name] = value
        else:
            if self.volume > 8:
                print '\t'*sub, 'other-type ', comObject, type(comObject)
        return items

    # def __enter__(self):
    #     pass
    #
    # def __exit__(self):
    #     pass
    #
    # def __del__(self):
    #
    #
    # def __cleanup(self):
    #     del self.outlook

    def _get_folders(self, comFolders, sub=0):
        """ gets data_objects in outlook instance """
        items = {}
        count = comFolders.Count
        if self.env == 'server':
            rng = range(count)
        elif self.env == 'pc':
            rng = range(1, count+1)
        else:  # ?
            rng = range(count)
        if self.volume > 3:
            print '\t'*sub, 'folders: ', count
        for index in rng:
            if self.volume > 1:
                print comFolders[index].Name
            value = self.process(comFolders[index], sub=sub)
            items = dict(items.items() + value.items())
        return items

    def _get_folder(self, comFolder, sub=1):
        items = {}
        name = comFolder.Name
        files = comFolder.Items
        filecount = files.Count
        folders = comFolder.Folders
        foldcount = folders.Count
        if self.env == 'server':
            rng_file = range(filecount)
            rng_folder = range(foldcount)
        elif self.env == 'pc':
            rng_file = range(1, filecount+1)
            rng_folder = range(1, foldcount+1)
        else:  # ?
            rng_file = range(filecount)
            rng_folder = range(foldcount)
        if self.volume > 3:
            print '\t'*sub, 'folder: ', name, ': folders - ', foldcount, ' files - ', filecount
        for index in rng_file:
            v = self.process(files[index], sub=sub)
            items = dict(items.items() + v.items())
        for index in rng_folder:
            v = self.process(folders[index], sub=sub)
            items = dict(items.items() + v.items())
        return name, items

    def _get_items(self, comItem, sub=1):
        """ gets items from a single folder object """
        value = comItem
        name = comItem.Subject
        if self.volume > 8:
            print '\t'*sub, 'object: ', name
        return name, value

    def __fill_items(self):
        self.directory = self.process(self.allitems)

    # filter on key
    # srules = ['RE: RR UPC Company Sales.xlsx']
    #  rules of form [(regex string, property),] -- only for string property returns
    def filter_mail(self, rules):
        """
        Filters outlook mail items based on regex rules provided.
        :param rules: Regex rules as strings.
        :return: List of filtered mail items (win32com data_objects).
        """
        filtered = {}
        for key, value in self.rootitems.items():
            for rule, p in rules:
                if p in properties:
                    item = get(mail_item=value, prop=p)
                    m = re.search(rule, item)
                    if m:
                        filtered[key] = value
                        break
                else:
                    print '%s is not a valid outlook mailitem property.' % property
                    break
        print 'Filters returned %s / %s root items matching "%s".' % \
              (len(filtered), len(self.rootitems), ', and '.join([str(x[0]) for x in rules]))
        return filtered

    def save_attachments(self, objects, path=None, saverule='.xls', datamode='all'):
        """
        Saves attachments matching a rule.
        :param objects: Mail data_objects (Outlook Com data_objects) to search.
        :param path: Path if not current working directory.
        :param saverule: Regex to match for file names and extentions.
        :param datamode: Key word for what to save, 'all' for all matching files, 'recent' for only the most recent.
        :return: None
        """
        names = []

        if not path:
            path = self.path+'\\data\\'
        recent = datetime.min
        recenti = None
        for index, value in objects.items():
            try:  # count=0, or
                count = value.Attachments.Count
                if self.env == 'server':
                    rng = range(count)
                elif self.env == 'pc':
                    rng = range(1, count+1)
                else:  # ?
                    rng = range(count)
                for i in rng:
                    attach = value.Attachments[i]
                    m = re.search(saverule, attach.FileName)  # case insensitivity by default? '(?i)'+saverule
                    if m:
                        sent = self.pytime_to_datetime(value.SentOn)
                        if sent > recent:
                            recent = sent
                            recenti = (index, i)
                        if datamode == 'all':
                            fn = re.search('(.*)(\.\w+)$', attach.FileName).groups()
                            fnam = fn[0]
                            fext = fn[1]
                            fname = self.__get_unused_name(obj=names, rootstr=fnam)
                            names.append(fname)
                            attach.SaveAsFile(path+fname+fext)
            except:
                pass

        if datamode == 'recent':
            if recenti:
                attach = objects[recenti[0]].Attachments[recenti[1]]
                name = attach.FileName
                names.append(name)
                attach.SaveAsFile(path+name)

    @staticmethod
    def __get_unused_name(obj, rootstr='f'):
        """
        Gets an unused name in directory so that many files with the same name can be extracted for outlook without
        overlap.
        :param obj: Com object with membership.
        :param rootstr: a base string to append an index to for uniqueness. ex) 'f' -> f0.txt, f1.txt, f2.txt ... fn.txt
        :return: First unused name as string matching inputs.
        """
        i = 0
        while True:
            key = rootstr+str(i)
            if key not in obj:
                return key
            i += 1

    @staticmethod
    def pytime_to_datetime(pytime):
        """
        Converts a python 'pytime' object to a python 'datetime' object for module continuity all dates are datetimes or
        formatted strings matching format in config.
        :param pytime: pytime object.
        :return: datetime object converted from input pytime object.
        """
        dtat = datetime.fromtimestamp(int(pytime))
        return dtat


class OutlookHandler(object):
    """
    Mail pump. Can be used to take action on incoming mail matching certain codes. This would allow remote entry and
    runs from any mobile device.
    ex) Incoming mail items with subject 'FOR RPT PROCESSING' and containing a .ini attachment could be run as a task
    via cron.
    """
    def __init__(self, path=None):
        self.outlook = client.DispatchWithEvents("Outlook.Application", OutlookHandler)
        if path:
            self.path = path
        else:
            self.path = os.getcwd()

    def OnNewMailEx(self, receivedItemsIDs):
        """
        Action to take on mail item relieved.
        :param receivedItemsIDs: Unique mail id.
        :return:
        """
        # RecrivedItemIDs is a collection of mail IDs separated by a ",".
        # You know, sometimes more than 1 mail is received at the same moment.
        for ID in receivedItemsIDs.split(","):
            print ID
            mail = self.outlook.Session.GetItemFromID(ID)
            print dir(mail)
            subject = mail.Subject
            try:
                # Taking all the "BLAHBLAH" which is enclosed by two "%".
                # command = re.search(r".*(ABS LLC Daily Flash Sales by Store by Department for Week End Date.*)", subject).group(1)
                command = re.search(r"(.*)", subject).group(1)
                try:  # nope, use getter
                    attachs = self.get_items(mail.Attachments)
                    for k, v in attachs.items():
                        print 'saving:\t', k
                        v.SaveAsFile(self.path+'\\'+k)
                except:
                    print 'oops.'  # error this...
                print command  # Or whatever code you wish to execute.
            except:
                pass

    @staticmethod
    def apply_rules(string):
        pass


### FINISH LATER ###
class MailPump(OutlookHandler):
    """
    Processes the above mail pump in a user friendly way. Or will do this...
    """
    def __init__(self, rules, path=None):
        handler = OutlookHandler.__init__(self, path)

    @staticmethod
    def pump():
        pythoncom.PumpMessages()

### Usage Example ###
# from rpt.com.comoutlook import ComOutlook
# c = ComOutlook()
# prop = 'Subject'
# rule = 'Report Card'
# rules = [(rule,prop)]
# d = c.filter_mail(rules)
# c.save_attachments(d)
# sample = d.iteritems().next()
# attach = sample.Attachments[1]