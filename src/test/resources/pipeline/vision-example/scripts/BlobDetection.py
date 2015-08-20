__author__ = 'rodneykinney'

from random import *

def findBlobs():
    return [blob() for i in range(randint(0,4))]

def blob():
    pos = (uniform(0,100), uniform(0,100))
    size = (uniform(0,10), uniform(0,10))
    return {'topleft': {'X':pos[0], 'Y':pos[1]},
            'bottomright': {'X':pos[0]+size[0], 'Y':pos[1]+size[1]},
            'score': random()}
