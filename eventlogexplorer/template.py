import os
import jinja2
from datetime import datetime

### Singleton Ginja2 environment as you only need one instance for the program
class Template:
    __environment = None
    def __kb(self, input):
        return int(input/1024)
    def __mb(self, input):
        return int(input/1024/1024)
    def __comma(self, input):
        return '{:,}'.format(input) if (isinstance(input, int) or isinstance(input, float)) else input
    def __dt(self, input):
        return format(input, '%Y-%m-%d %H:%M:%S') if isinstance(input, datetime) else input
    def __tms(self, ms):
        if ms < 1000:
            return '{:.0f} ms'.format(ms)
        min, sec = divmod(ms/1000, 60)
        hour, min = divmod(min, 60)
        return '{}{}{}'.format('{:.0f} h'.format(hour) if hour else '', ' {:.0f} m'.format(min) if  min else '',
                               ' {:.1f} s'.format(sec) if sec else '')
    def __init__(self):
        if Template.__environment is None:
            #loader = jinja2.FileSystemLoader(os.getcwd() + '/templates/')
            #loader = jinja2.FileSystemLoader(searchpath='./')
            loader = jinja2.PackageLoader('eventlogexplorer', 'templates')
            env = jinja2.Environment(autoescape=True,loader=loader,trim_blocks = True,lstrip_blocks = True)
            #env.trim_blocks = True
            #env.lstrip_blocks = True
            env.filters['kb'] = self.__kb
            env.filters['mb'] = self.__mb
            env.filters['comma'] = self.__comma
            env.filters['dt'] = self.__dt
            env.filters['tms'] = self.__tms
            Template.__environment = env
    def get(self, format):
        return Template.__environment.get_template(format)

### Another implementation
# class Ginja2Environment(type):
#     def kb(self, input):
#         return int(input/1024)
#     def mb(self, input):
#        return int(input/1024/1024)
#    def comma(self, input):
#        return '{:,}'.format(input) if (isinstance(input, int) or isinstance(input, float)) else input
#    def dt(self, input):
#        return format(input, '%Y-%m-%d %H:%M:%S') if isinstance(input, datetime) else input
#    def tms(self, ms):
#        if ms < 1000:
#            return '{:.0f} ms'.format(ms)
#        min, sec = divmod(ms/1000, 60)
#        hour, min = divmod(min, 60)
#        return '{}{}{}'.format('{:.0f} h'.format(hour) if hour else '', ' {:.0f} m'.format(min) if  min else '',
#                               ' {:.1f} s'.format(sec) if sec else '')
#    env = None
#    def __init__(cls, name, bases, attrs, **kwargs):
#        super().__init__(name, bases, attrs)
#        cls._instance = None
#
#    def __call__(cls, *args, **kwargs):
#        if cls._instance is None:
#            cls._instance = super().__call__(*args, **kwargs)
#            loader = jinja2.FileSystemLoader(os.getcwd() + '/templates/')
#            cls._instance.env = jinja2.Environment(autoescape=True,loader=loader,trim_blocks = True,lstrip_blocks = True)
#            #cls._instance.env.trim_blocks = True
#            #cls._instance.env.lstrip_blocks = True
#            cls._instance.env.filters['kb'] = cls.kb
#            cls._instance.env.filters['mb'] = cls.mb
#            cls._instance.env.filters['comma'] = cls.comma
#            cls._instance.env.filters['dt'] = cls.dt
#            cls._instance.env.filters['tms'] = cls.tms
#        return cls._instance
#
##class Environment(metaclass=Ginja2Environment):
#    pass
#
#def getTemplate(formatFilename):
#    return Environment().env.get_template(formatFilename)

