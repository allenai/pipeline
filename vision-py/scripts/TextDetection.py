__author__ = 'rodneykinney'

from random import *
import string

def findText():
    return [text() for i in range(randint(0,4))]

def randomword(length):
    return ''.join(choice(string.lowercase) for i in range(length))

def text():
    pos = (uniform(0,100), uniform(0,100))
    size = (uniform(0,10), uniform(0,10))
    return {'topleft': {'X':pos[0], 'Y':pos[1]},
            'bottomright': {'X':pos[0]+size[0], 'Y':pos[1]+size[1]},
            'text': randomword(randint(3,8))}
