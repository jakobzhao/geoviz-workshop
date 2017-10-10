#!/usr/bin/python
# -*- coding: gbk -*-

import os
import bz2
import fnmatch
import json
from pymongo import MongoClient, errors


client = MongoClient("localhost", 27017)
db = client["msa"]


# twfile = open("2013.json", "w")

for root, dir, files in os.walk("../tweets/wt"):
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
