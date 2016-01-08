__author__ = 'drew bailey'
__version__ = 0.1

"""
Setup methods __init__.
Root methods dealing with master tables creation, custom class __init__ methods (such as __fromfile__ or __plugin__,
menus, task creation, etc...
Classes are inherited but classes requiring custom initializations and setup functions.
"""

from .setup import Menu, Config
from tasksetup import task_setup

__all__ = ['__author__',
           '__version__',
           'Config',
           'Menu',
           'task_setup']
