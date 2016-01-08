__author__  = 'drew bailey'
__version__ = 0.1

"""
VERY EXPERIMENTAL. FIRST LOOK AT THIS TYPE OF PLUGIN PARSER.
Use structured data methods instead of building out a parser for better results.

Plugin classes containing variables, functions, and function information to pass to parser.
THESE CLASSES ARE NOT CALLED BUT ACT AS INPUT FOR PARSER_HANDLER.
    variables should be those required to initialize target app class
    info dictionary should be named info and have at least four nested keys for each function key
        -function object
        -rule_index -> call this function to supplement which rule index
        -field_num -> the number of fields to add to the output
        -header_index -> the index positions of the values to add
    functions should be @staticmethod, this class is not called
"""


class parser_cpi:
    """ Parses a hypothetical Pricing Model Report """
    task_name = 'TestParse'
    header = [('pull_end_dt', True),
              ('div_num', True),
              ('comp_name', True),
              ('comp_sto_num', True),
              ('zone_name', True),
              ('sto_num', True),
              ('zone_num', True),
              ('comp_zone_num', True),
              ('weight_each', True),
              ('base_shelf', True),
              ('catg_num', False),
              ('scat_num', False),
              ('catg_desc', False),
              ('alb_ext_rtl', False),
              ('comp_ext_rtl', False),
              ('alb_gross', False),
              ('comp_gross', False),
              ('alb_gper', False),
              ('comp_gper', False),
              ('alb_mix', False),
              ('comp_mix', False),
              ('cpi_index', False),
              ('total', False),
              ('dept_name', False)]
    linerules = ["(?i)^RUN\s+.+ENDING\s+(\d+[-:]\d+[-:]\d+)",
                 "(?i)^(\d+)\s+(\w+)\s+(\d+)\s+(\w+)\s+(\d+)\s.+(\d+)\s.+'\s(\d+)\s+(.+)\s+([B|S]\s+TO\s+[B|S]).*$",
                 "(?i)^ (\d*)\s+(\d*)\s+([\w -]+\w)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s+([\d,.]+)\s*$"]
    ### ASK FOR FILE AND LINE TRIGGERS? DON'T WRITE IF BOTH ARE NOT True ###
    ### currently using this trigger for both, not a good idea in general ###
    trigger = 22  # 22 is cpi index
    datamode = 'latest'

    @classmethod
    def __clsattr__(cls):
        return dict(vars(cls))

    def __init__(self):
        pass

    @staticmethod
    def totalsCPI(groups):
        """
        evaluates rule output to generate two additional fields
        :param groups: groups() from rule index 2 rule
        :return: [total level, just finished department name]
        """
        import re
        dd, tl = None, None
        key = ['department', 'category', 'zone']
        for k in key:
            for group in groups:
                r1 = '(?i)'+k+'\s+total'
                r2 = '(?i)'+k+'\s+total\s+for\s+([\s\w]+\w)'
                m1 = re.search(r1, group)
                m2 = re.search(r2, group)
                if m2: dd = m2.groups()[0]
                if m1:
                    if k is key[0]: tl = 1
                    elif k is key[1]: tl = 2
                    elif k is key[2]: tl = 3
        return [tl, dd]

    info = {'totalsCPI':
             {'function': totalsCPI,
              'rule_index': 2,
              'field_num': 2,
              'header_index': [23, 24]}}