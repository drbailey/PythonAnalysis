__author__ = 'drew bailey'
__version__ = 0.1

""" All rpt prints run through these data_objects. """

from ..config import VOLUME


class Broadcaster(object):
    """
    Build out later. Warnings, errors, etc..?
    """
    def __init__(self):
        pass

    @staticmethod
    def shout(msg, clamor=10):
        if VOLUME > clamor:
            print msg


bc = Broadcaster()


def broadcast(msg, clamor=10):
    bc.shout(msg=msg, clamor=clamor)
