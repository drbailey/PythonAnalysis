__author__ = 'drew bailey'
__version__ = 0.1

""" Miscellaneous utility functions. """


def sample_size(n):
    """
    Sloppy workaround for determining sample size for a reasonable type estimate.
    Not very scientific...
    """
    if n < 30:
        return int(n)
    sqrt = n ** 0.5
    if sqrt < 30:
        return 30
    if sqrt > 500:
        return 500
    return int(sqrt)
