#!/usr/bin/env python
# -*- coding: utf-8 -*-

import random

from pymongo import Connection

#conn = Connection('localhost', 30000)
# for test
conn = Connection()
db = conn.calc

hedge_col = 'relations'

LIMIT_SIZE = 100
P = 1

def generate(hg_f, init_f, p=1):
    users = {}
    def write_init(itm):
        if itm['user'] not in users:
            init_f.write('v%d rank 1 t u\n' % itm['user'])
            users[itm['user']] = True
            
    ps = {}
    def get_ps(id):
        if id not in ps:
            pid = random.randint(0, p-1)
            ps[id] = pid
            return pid
        else:
            return ps[id]
    
    for itm in getattr(db, hedge_col).find({'t': 1}).limit(LIMIT_SIZE):
        hg_f.write('%d %d/%d %d/%d\n' % (itm['id'], 
            itm['user'], get_ps(itm['user']),
            itm['follow'], get_ps(itm['follow'])))
        write_init(itm)
        
    for itm in getattr(db, hedge_col).find({'t': 2}).limit(LIMIT_SIZE):
        hg_f.write('%d %d/%d %d/%d\n' % (itm['id'], 
            itm['user'], get_ps(itm['user']),
            itm['tag'], get_ps(itm['tag'])))
        write_init(itm)
        
    for itm in getattr(db, hedge_col).find({'t': 3}).limit(LIMIT_SIZE):
        hg_f.write('%d %d/%d %d/%d\n' % (itm['id'], 
            itm['user'], get_ps(itm['user']), 
            itm['qid'], get_ps(itm['qid'])))
        write_init(itm)
        
    for itm in getattr(db, hedge_col).find({'t': 4}).limit(LIMIT_SIZE):
        hg_f.write('%d %d/%d %d/%d\n' % (itm['id'], 
            itm['user'], get_ps(itm['user']),
            itm['forward'], get_ps(itm['forward'])))
        write_init(itm)
        
    for itm in getattr(db, hedge_col).find({'t': 5}).limit(LIMIT_SIZE):
        hg_f.write('%d %d/%d %d/%d\n' % (itm['id'], 
            itm['user'], get_ps(itm['user']), 
            itm['like'], get_ps(itm['like'])))
        write_init(itm)
        
    for itm in getattr(db, hedge_col).find({'t': 6}).limit(LIMIT_SIZE):
        hg_f.write('%d %d/%d %d/%d %d/%d\n' % (itm['id'], 
            itm['user'], get_ps(itm['user']), 
            itm['mblog'], get_ps(itm['mblog']), 
            itm['tag'], get_ps(itm['tag'])))
        write_init(itm)

if __name__ == "__main__":
    with open('hg.txt', 'w') as hg_f:
        with open('init.txt', 'w') as init_f:
            generate(hg_f, init_f, p=P)