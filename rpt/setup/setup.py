__author__  = 'drew bailey'
__version__ = 0.1

"""
Base setup classes and Menus for inheritance.
"""

from ..util import clean, transform_string_date
from ConfigParser import SafeConfigParser
import ast


class Config(object):
    """ A class containing class method fromfile in inherit. """
    configfile = 'config.ini'
    section = 'DEFAULT'
    # 's' for string, 'i' for int, 'f' for float, 'b' for boolean, 'd' for datetime, any other evaluated literally
    white_list = {'user_name': 's',
                  'task_name': 's'}
    config = {}
    functions = {}

    @classmethod
    def __plugin__(cls, plugin_name='cpi', app='parser'):
        exclusions = ['info']
        importer = app + '_' + plugin_name
        # print globals()
        plugin = globals()[importer]
        cls.functions = plugin.info
        plugind = plugin.__clsattr__()
        for element, value in plugind.items():
            if element.find('__') is -1 and element not in exclusions:
                if element in cls.white_list:
                    cls.config[element] = value
                # if hasattr(value, '__call__'):
                #     cls.functions['function'] = plugin.value
        return cls(mode='plugin')

    @classmethod
    def __fromfile__(cls, filename=None, section=None):
        if not filename:
            filename = cls.configfile
        if not section:
            section = cls.section

        parser = SafeConfigParser()
        parser.read(filename)
        _date_format = parser.get(section, 'dateformat').replace('#', '%')
        for name, value in parser.items(section):
            if name in cls.white_list:
                value = cls.typer(name, value, _date_format)
                cls.config[name] = value
        return cls(mode='fromfile')

    @classmethod
    def typer(cls, name, value, dateformat):
        """ Turns a config input into python data_objects. """
        try:
            value = value.replace('|', r'%')
        except:
            pass
        try:
            if cls.white_list[name] == 'i':
                value = int(value)
            elif cls.white_list[name] == 'f':
                value = float(value)
            elif cls.white_list[name] == 'b':
                value = bool(value)
            elif cls.white_list[name] == 'd':
                value = transform_string_date(value, dateformat)
            elif cls.white_list[name] == 's':
                # placeholder for any string specific formatting
                pass
            else:
                try:
                    value = ast.literal_eval(value)
                    if name == 'sql':
                        try:
                            value[0] = value[0].replace('|', r'%')
                            value[1] = value[1].replace('|', r'%')
                        except:
                            pass
                except:
                    pass
        except:
            pass
        return value

    def __init__(self, mode, kwargs):
        # this isn't really used... I NEVER REALLY INIT
        # I just pass along my class and function properties
        self.initFactory(mode, kwargs)
        self.fillLocals()

    def initFactory(self, mode, kwargs):
        """
        adds all kwargs to self
        :param mode: which class method was used to initialize class
        :param kwargs: items to add to self
        :return: self
        """
        if mode in ['fromfile', 'plugin']:
            # for k, v in self.__class__.config.items():
            #     self.__dict__[k] = v
            [setattr(self, k, v) for k, v in self.__class__.config.items()]
            [setattr(self, k, v) for k, v in self.__class__.functions.items()]
        elif mode in ['normal']:
            [setattr(self, k, v) for k, v in kwargs.items()]
            # for k, v in kwargs.items():
            #     self.__dict__[k] = v
        else:
            pass
        return self

    def fillLocals(self):
        """
        sets nonexisting whitelist items to self.item = None
        :return: self
        """
        for item in self.__class__.white_list:
            if item not in self.__dict__:
                setattr(self, item, None)
        return self


# class Setup(Config):
#     """ An example root object for Config inheriting classes.
#     1) Receives any number of arguments from a file or user and stores them as instance variables.
#     2) Includes a number of universally useful functions for setup files.
#     """
#
#     whitelist = {'http_port': 'i',
#                  'http_server': 's'}
#
#     def __init__(self, mode=False, **kwargs):
#         Config.__init__(self, kwargs)
#         pass

# move to utils

class Menu(object):
    """
    Abstracts menu data_objects to small dictionaries representing each items or menu.
    """

    def __init__(self):
        pass
        
    def run_menu(self, d, endfunction=None):
        """
        starts menu
        :param d: main menu dictionary
        :param endfunction: an optional clean_up_app function
        :return: None
        """
        last_d = []
        while True:
            flag = self.process_menu(d)
            # print flag
            if flag is None:
                try:
                    d = last_d[-1]
                    del(last_d[-1])
                except:
                    if endfunction:
                        self.end_menu(endfunction)
                    break
            elif not flag:
                pass
            else:
                last_d.append(d)
                d = flag

    def process_menu(self, d):
        """ Accepts a menu dictionary and processes it. """
        N, T, P, F, R = self.unpack_menu(d)
        if T == 'menu':
            if R:
                mm, end = 'menu', 'Exit'
            else:
                mm, end = 'sub-menu', 'Back'
            options = '%s\n %s %s\n%s\nWhat would you like to do?\n' % ('-'*50, P, mm, '-'*50)
            for index, item in enumerate(F, start=1):
                n, t, p, f, r = self.unpack_menu(item)
                options += '\t%s: %s\n' % (index, p)
            options += '\t%s: %s\n' % (end[0], end)
            raw = clean(raw_input(options))
            try:
                if (raw.lower() == 'b' and not R) or (raw.lower() == 'e' and R):
                    return None
                elif 0 < int(raw) <= len(F):
                    return F[int(raw)-1]
                else:
                    raise Exception("Invalid input")
            except:
                return 0
        elif T == 'function':
            e = eval("%s" % F, locals(), globals())
            return None

    @staticmethod
    def unpack_menu(item):
        """ Returns fields from item dict. """
        n = item['name']
        t = item['type']
        p = item['printed']
        f = item['funct']
        try:
            r = item['root']
            return n, t, p, f, r
        except:
            return n, t, p, f, None

    def end_menu(self, item):
        """ Evaluates an end function. """
        if item:
            e = eval("%s" % item, locals(), globals())

### menu example and test items. Leave in the code.
##e = {'name':'e',
##     'type':'function',
##     'printed':'do e',
##     'funct':'2+2'}
##
##f = {'name':'f',
##     'type':'function',
##     'printed':'do f',
##     'funct':'3+3'}
##
##g = {'name':'g',
##     'type':'function',
##     'printed':'do g',
##     'funct':'4+4'}
##
##a = {'name':'a',
##     'type':'menu',
##     'printed':'sub a',
##     'root':False,
##     'funct':[e,f,g]}
##
##b = {'name':'b',
##     'type':'function',
##     'printed':'do b',
##     'funct':'2+2'}
##
##c = {'name':'c',
##     'type':'function',
##     'printed':'do c',
##     'funct':'3+3'}
##
##d = {'name':'d',
##     'type':'function',
##     'printed':'do d',
##     'funct':'4+4'}
##
##main = {'name':'main',
##        'type':'menu',
##        'printed':'main',
##        'root': True,
##        'funct':[a,b,c,d]}
##
##m = Menu()
##m.run(main) # or m.run(main,'cleanupfunctionstring')


