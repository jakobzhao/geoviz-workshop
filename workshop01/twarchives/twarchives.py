#!/usr/bin/python
# -*- coding: gbk -*-

import os
import bz2
import fnmatch
import json
from pymongo import MongoClient, errors

from settings import user, psw


client = MongoClient("mapious.ceoas.oregonstate.edu", 27017, username=user, password=psw)

db = client["geoviz"]

# twfile = open("2013.json", "w")

path = "data/14"

for root, dir, files in os.walk(path):
        for item in fnmatch.filter(files, "*"):
            print "..." + item
            if "bz2" not in item:
                continue
            bz_file = bz2.BZ2File(root + "/" + item)
            try:
                lines = bz_file.readlines()
                # exit(-1)
            except:
                print "decompress error!"
            for line in lines:
                if '"geo":{' in line and json.loads(line)['geo'] != None:
                    [lat, lng] = json.loads(line)['geo']['coordinates']
                    if lat > 32.47269502206151 and lat < 33.6031820405205 and lng >  -80.8154296875 and lng < -79.20318603515625:
                        print line
                        # twfile.write(json.dumps(json.loads(line)) + ",\n")
                        try:
                            db.tweets.insert_one(json.loads(line))
                        except errors.DuplicateKeyError:
                            print 'This post has already been inserted.'

# twfile.close()
print "completed"
