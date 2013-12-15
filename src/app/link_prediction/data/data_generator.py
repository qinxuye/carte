#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pymongo import Connection

#conn = Connection('localhost', 30000)
# for test
conn = Connection()
db = conn.linkpred
micro_blog_col = db.micro_blog
weibo_user_col = db.weibo_user

new_db = conn.calc
# weibo user
new_user_col = new_db.weibo_user
new_user_col.create_index("uid")
# user tag
new_utag_col = new_db.user_tag
new_utag_col.create_index("name")
# wei qun
new_wqun_col = new_db.wei_qun
new_wqun_col.create_index("qid")
# micro blog
new_mblog_col = new_db.micro_blog
new_mblog_col.create_index("mid")
# mblog tag includes links
new_tag_col = new_db.tag
new_tag_col.create_index("name")

# relations
R1, R2, R3, R4, R5, R6 = range(1, 7)
new_relations = new_db.relations

_id = 0
def id_generate():
    global _id
    _id += 1
    return _id

def generate():
    uids = {}
    utags = {}
    quns = {}
    mids = {}

    print 'start to perform users for R2, R3, R5'
    for wuser in weibo_user_col.find():
        uid = wuser['uid']
        if uid not in uids:
            guid = id_generate()
            new_user = {'id': guid, 'uid': uid}
            new_user_col.insert(new_user)
            uids[uid] = guid

            if 'info' in wuser and 'tags' in wuser['info']:
                for utag in wuser['info']['tags']:
                    if utag not in utags:
                        gutagid = id_generate()
                        new_utag_col.insert({'id': gutagid, 'name': utag})
                        utags[utag] = gutagid
                    # R2
                    relation = {'t': R2, 'user': guid, 'tag': utags[utag], 'id': id_generate()}
                    new_relations.insert(relation)

            if 'qids' in wuser:
                for qid in wuser['qids']:
                    if qid not in quns:
                        gqid = id_generate()
                        new_wqun_col.insert({'id': gqid, 'qid': qid})
                        quns[qid] = gqid
                    # R3
                    relation = {'t': R3, 'user': guid, 'qid': quns[qid], 'id': id_generate()}
                    new_relations.insert(relation)

            if 'likes' in wuser:
                for like in wuser['likes']:
                    if like not in mids:
                        gmid = id_generate()
                        new_mblog = {'id': gmid, 'mid': like}
                        new_mblog_col.insert(new_mblog)
                        mids[like] = gmid
                    # R5
                    relation = {'t': R5, 'user': guid, 'like': mids[like], 'id': id_generate()}
                    new_relations.insert(relation)
                
    del utags, quns

    print 'start to perform users for R1'
    for wuser in weibo_user_col.find():
        uid = wuser['uid']
        guid = uids[uid]
        for fuser in wuser['follows']:
            fuid = fuser['uid']
            if fuid in uids:
                gfuid = uids[fuid]
                # R1
                relation = {'t': R1, 'user': guid, 'follow': gfuid, 'id': id_generate()}
                new_relations.insert(relation)

    tags = {}
    print 'start to perform mblogs for R4, R6'
    for mblog in micro_blog_col.find():
        uid = mblog['uid']
        if uid not in uids:
            continue

        if 'omid' in mblog:
            mid = mblog['omid']
            if mblog['omid'] not in mids:
                gmid = id_generate()
                new_mblog = {'id': gmid, 'mid': mid}
                new_mblog_col.insert(new_mblog)
                mids[mid] = gmid
            # R4
            relation = {'t': R4, 'user': uids[uid], 'forward': mids[mid], 'id': id_generate()}
            new_relations.insert(relation)
                
        if ('tags' in mblog and len(mblog['tags']) > 0) or\
           ('links' in mblog and len(mblog['links']) > 0):
            mid = mblog['mid']
            if mid not in mids:
                gmid = id_generate()
                new_blog = {'id': gmid, 'mid': mid}
                new_mblog_col.insert(new_mblog)
                mids[mid] = gmid
            
            for t in ['tags', 'links']:
                for tag in mblog[t]:
                    if tag not in tags:
                        gtagid = id_generate()
                        new_tag = {'id': gtagid, 'name': tag}
                        new_tag_col.insert(new_tag)
                        tags[tag] = gtagid
                    # R6
                    relation = {'t': R6, 'user': uids[uid], 'mblog': mids[mid], 'tag': tags[tag], 'id': id_generate()}
                    new_relations.insert(relation)

if __name__ == '__main__':
    generate()