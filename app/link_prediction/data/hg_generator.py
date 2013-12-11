#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random

from pymongo import Connection

#conn = Connection('localhost', 30000)
# for test
conn = Connection()
db = conn.calc

vertex_cols = ('weibo_user', 'user_tag', 'wei_qun', 'micro_blog', 'tag')
hedge_col = 'relations'

def generate(hg_f, expect_f):
    all_size = 0
    for col_name in vertex_cols:
        col = getattr(db, col_name)
        all_size += col.count()

    print 'start to write vertices'

    hg_f.write('# Number of vertices:\n')
    hg_f.write('%d\n\n' % all_size)

    hg_f.write('# List for Global ID of each vertex:\n')
    for col_name in vertex_cols:
        col = getattr(db, col_name)
        for itm in col.find():
            hg_f.write('%d\n' % itm['id'])
    hg_f.write('\n')

    print 'start to write hyperedges'

    hedge_size = getattr(db, hedge_col).count()
    R1_size = getattr(db, hedge_col).find({'t': 1}).count()
    R6_size = getattr(db, hedge_col).find({'t': 6}).count()

    hg_f.write('# Number of hyperedges:\n')
    hg_f.write(str(hedge_size - R1_size / 2))
    hg_f.write('\n\n')

    hg_f.write('# Sum of size of vertices in each hyperedge\n')
    hedge_v_size = 2 * (hedge_size - R6_size) + 3 * R6_size
    hg_f.write('%d\n\n' % hedge_v_size)

    hg_f.write('# List for each hyperedge, format: HEdgeID   Size  VID1  VID2\n')

    R1_hedges = list(getattr(db, hedge_col).find({'t': 1}))
    random.shuffle(R1_hedges)
    for idx, hedge in enumerate(R1_hedges):
        if idx < (R1_size / 2):
            expect_f.write('%d   %d  %d\n' % (hedge['id'], hedge['user'], hedge['follow']))
        else:
            hg_f.write('%d   2  %d  %d\n' % (hedge['id'], hedge['user'], hedge['follow']))

    for itm in getattr(db, hedge_col).find({'t': 2}):
        hg_f.write('%d   2  %d  %d\n' % (itm['id'], itm['user'], itm['tag']))
    for itm in getattr(db, hedge_col).find({'t': 3}):
        hg_f.write('%d   2  %d  %d\n' % (itm['id'], itm['user'], itm['qid']))
    for itm in getattr(db, hedge_col).find({'t': 4}):
        hg_f.write('%d   2  %d  %d\n' % (itm['id'], itm['user'], itm['forward']))
    for itm in getattr(db, hedge_col).find({'t': 5}):
        hg_f.write('%d   2  %d  %d\n' % (itm['id'], itm['user'], itm['like']))
    for itm in getattr(db, hedge_col).find({'t': 6}):
        hg_f.write('%d   3  %d  %d  %d\n' % (itm['id'], itm['user'], itm['mblog'], itm['tag']))

if __name__ == "__main__":
    with open('hypergraph.txt', 'w') as hg_f:
        with open('expect.txt', 'w') as expect_f:
            generate(hg_f, expect_f)