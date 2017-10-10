#!/usr/bin/python
# -*- coding: gbk -*-

import os
import bz2
import fnmatch

twfile = open("archives.json", "w")

path = "data/14"

for root, dir, files in os.walk(path):
        for item in fnmatch.filter(files, "*"):
            print "..." + item
            if "bz2" not in item:
                continue
            bz_file = bz2.BZ2File(root + "/" + item)
            try:
                lines = bz_file.readlines()
            except:
                print "decompress error!"
            for line in lines:
                if 'love' in line:
                    twfile.write(line)
                    print line

twfile.close()
print "completed"
