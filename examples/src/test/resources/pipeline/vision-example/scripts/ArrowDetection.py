__author__ = 'rodneykinney'

from random import *

def findArrows():
    return [arrow() for i in range(randint(0,4))]

def arrow():
    pos = (uniform(0,100), uniform(0,100))
    size = (uniform(0,10), uniform(0,10))
    return {'headtopleft': {'X':pos[0], 'Y':pos[1]},
            'headbottomright': {'X':pos[0]+size[0], 'Y':pos[1]+size[1]}}
