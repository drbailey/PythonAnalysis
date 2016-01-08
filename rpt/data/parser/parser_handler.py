__author__  = 'drew bailey'
__version__ = 0.1

"""
Handles file parsing.
Accepts lightweight parsing rules and expected header rows as lists.
Returns WorkingTable data object similar to other data class outputs.
Should look very close to the same as other data in/out methods to a user.
"""

from collections import OrderedDict
from ...config import VOLUME
from ...setup import Config
# from ..data_objects import WorkingTable
import os
import re


class Parser(Config):
    """
    inputs:
    1) list of rules as regex strings
    2) list of header names coupled with boolean
        if True, writeline holds this value until it is replaced by a new one
        if False, writeline clears this value if it isn't in the current line
        ex) [(field_1,True),(field_2,False)]
    3) trigger index, which header to trigger on.
        string of name or integer of position starting at 0.
        ex) 'field_2' or 1
    optional inputs:
    4) data_mode,
        'all' -> all files in folders in Parse,
        'latest' -> all files in latest folder date; default,
        'date: yyyy.mm.dd' -> or all files in specific date folder
    5) file_rule, a regex string that matches all file names desired (defaults to '(?i).*.txt')
    usually run from .__plugin__() mode
    """

    #filename = 'config.ini'
    #section = ''
    white_list = {'task_name': 's',
                  'header': 'list',
                  'line_rules': 'list',
                  'trigger': 'i',
                  'PATH': 's',
                  'path_rules': 's',
                  'file_rules': 's',
                  'data_mode': 's',
                  'parser': 's'}
    #config = {}

    def __init__(self, mode='normal', **kwargs):
        """
        most often used with .__plugin__() classmethod called.
        :param mode:
                    'normal' for **kwargs input,
                    '__fromfile__' for config,
                    or '__plugin__('app_plugin')' for predefined parser
        :param kwargs: any white_list quantities
        :return: None
        """
        ### defaults ###
        self.data_mode = 'latest'
        self.file_rules = '(?i).*.txt'
        self.header = []
        self.task_name = ''
        self.line_rules = ''
        self.trigger = 0

        self.volume = VOLUME

        Config.__init__(self, mode, kwargs)

        #if self.parser: self.parser = app + '_' + self.parser # for plugin use
        self.key_line = OrderedDict(self.header)
        if not self.path:
            self.path = os.getcwd()
        ### Unicode used since some builtin os functions are broken
        self.parse_path = os.getcwd() + u'\\%s\\Parse' % self.task_name
        # self.file_rules = file_rules
        # self.pathrules = pathrules

    def auto_parse(self):
        files = self.get_files(data_mode=self.data_mode)
        data = self.get_data(files=files, rules=self.line_rules, key=self.key_line, trigger=self.trigger)
        return data

    #not to be static, add self
    def get_files(self, data_mode=None, path=None):
        """
        gets file paths that meet data_mode and file_rule criteria.
        :param data_mode: 'all', 'latest', or 'date: yyyy.mm.dd'
        :param path: defaults to self.parse_path
        :return: list of full paths
        """
        if not data_mode:
            data_mode = self.data_mode
        if not path:
            path = self.parse_path
        data_mode = data_mode.lower()
        m = re.search('(\d{4}.\d{2}.\d{2})', data_mode)
        if data_mode == 'all':
            pass
        elif data_mode == 'latest':
            latest, greatest = '', 0
            for name in os.listdir(path):
                parts = int(name.replace('.', ''))
                if parts > greatest:
                    latest = name
                    greatest = parts
            if latest:
                path += u'\\'+latest
        elif m:
            path += r'\%s' % m.groups()[0]
        else:
            print "Mode Error: Unknown mode."
            return []
        return self.walk(path)

    def walk(self, path):
        """
        sub func for get_files
        :param path: root PATH for os.walk func
        :return: list of full paths
        """
        flist = []
        for root, directories, files in os.walk(path):
            # print root, directories, files
            for f in files:
                if re.search(self.file_rules, str(f)): flist.append((root+u'\\%s' % f))
        return flist

    ### CAN I SPLIT THIS UP? Very confusing... even to the guy who wrote it... ###
    def get_data(self, files, rules, key, trigger):
        """

        :param files: file list
        :param rules: rule list
        :param key: header, boolean tuple list
        :param trigger: combined file/row trigger to auto_write when True
        :param debug:
                True: checks every rule to every line, calls out overlap
                False: accepts first rule match
        :return: mimic data object of parsed lines
        """
        lim = len(key)  # expected argument number
        rkey, ckr = self.rfields(rules)
        if ckr is not lim:
            print 'Header has ', lim, ' items, but rules generate ', ckr, ' fields.'
        data = []
        data.append(([k for k in key]))
        for fil in files:
            triggered = False
            with open(fil, 'r') as f:
                curline = [None]*lim
                lines = f.readlines()
                for index, line in enumerate(lines):
                    flag = []
                    if self.volume > 5:
                        print index+1, line,
                    for i, rule in enumerate(rules):
                        l = re.search(rule, line)
                        if l:
                            l = list(l.groups())
                            func = self.check_funcs(ruleindex=i)
                            if func:
                                funcfields = self.eval_func(func=func, groups=l)
                                l.extend(funcfields)
                            lg = self.group_formatter(l)
                            curline[rkey[rule][0]:rkey[rule][1]] = lg
                            ### file trigger ###
                            if curline[trigger]: triggered = True
                            ### don't auto_write lines without values in the trigger position ###
                            ###  is this a good idea? (works with cpi) ###
                            if triggered and curline[trigger]:
                                data.append([x for x in curline])
                            curline = self.wiper(curline, key)
                            if self.volume < 8:
                                break
                            else:
                                flag.append((rule, lg))
                    if len(flag) > 1:
                        print 'Line Ambiguous:'
                        print 'line:', index+1, '"', line[:-1], '"'
                        for x, y in flag:
                            print 'rule:', x, 'result:', y
                        print 'Rules should be unique to line type.'
        return WorkingTable(data=data[1:], header=data[0])

    def rfields(self, rules):
        """
        generate desired field locations for rules
        :param rules: rules list
        :return: dictionary of indexes for rules and number of fields generated by all rules for check vs header length.
        """
        modified = {}
        start, stop = 0, None
        for i, r in enumerate(rules):
            rlen = len(re.findall('\(', r.replace('\\(', '').replace('(?', '')))
            for f, d in self.functions.items():
                if i is d['rule_index']:
                    rlen += d['field_num']
                    if self.volume > 7:
                        print 'func: ', f, ' vars: ', start
            stop = start+rlen
            modified[r] = (start, stop)
            start = stop
            if self.volume > 7:
                print 'rule: ', r, ' vars: ', stop
        return modified, stop

    def check_funcs(self, ruleindex):
        """
        gets function data_objects for relevant rules
        :param ruleindex: which rule number to get function for
        :return: function object or None
        """
        for f, d in self.functions.items():
            if ruleindex is d['rule_index']:
                return d['function']
        return None

    @staticmethod
    def eval_func(func, groups):
        """
        evaluates a function object that accepts regex groups()
        :param func:
        :param groups:
        :return: list of fields to add
        """
        # print groups
        # print func
        return func(groups)

    @staticmethod
    def wiper(status_line, key):
        """
        resets a line for next input
        :param status_line: current line list
        :param key: header key
        :return: line with False key items wiped
        """
        for x, y in enumerate(key):
            if not key[y]:
                status_line[x] = None
        return status_line

    @staticmethod
    def group_formatter(groups):
        """
        supplements mimic with additional formatting checks specific to file parsing
        doesn't really do much right now, if parsing large files this can most likely be omitted.
        :param groups: line groups
        :return: new line object
        """
        newgroups = []
        for group in groups:
            try:
                group = group.replace(',', '')
            except:
                pass
            try:
                group = int(group)
            except:
                try:
                    group = float(group)
                except:
                    if group:
                        group = str(group).strip()
                    else:
                        pass
            newgroups.append(group)
        return newgroups


### Test parameters ###
# task = 'TestParse'
#
# header = [('field_1', True),
#           ('field_2', False),
#           ('field_3', True)]
#
# linerules = ['run ([\d:]\d*)',
#          'two.+--([ \d]*)',
#          'ary --([ \w]*)']
#
# trigger = 2
#
# data_mode = 'all'
#
# if __name__ == '__main__':
#     p = Parser(task,header,linerules,trigger)
#     f = p.get_files(data_mode=data_mode)
#     d = p.get_data(f,p.linerules,p.key_line,p.trigger)

#
# p = Parser.__plugin__(plugin_name='cpi', app='parser')
# f = p.get_files(data_mode=p.data_mode)
# d = p.get_data(files=f,rules=p.linerules,key=p.key_line,trigger=p.trigger)