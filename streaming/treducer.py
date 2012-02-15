#!/usr/bin/env python
import sys
from pymongo_hadoop import BSONReducer

def reducer(key, values):
    print >> sys.stderr, "Processing Key: %s" % key
    _count = 0
    for v in values:
        _count += v['count']
    return {'_id': key, 'count': _count}

BSONReducer(reducer)
