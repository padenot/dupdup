#!/usr/bin/env python

import os
import sys
import hashlib
from collections import defaultdict
from time import time
import math
import json
import argparse

parser = argparse.ArgumentParser(description='Find duplicate files.')

parser.add_argument("path", type=str, nargs='?', default=os.getcwd(),
                    help="path to search for duplicates")
parser.add_argument("-o", "--output", type=str, help="output file")
parser.add_argument("-i", "--interval", type=int, default=1,
                    help="interval at which print progress in seconds")

args = parser.parse_args()

BLOCKSIZE = 2**16

filelist = []
hashed = defaultdict(list)
total_files = 0
wasted_space = 0
dupes = 0

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "{}{}".format(s, size_name[i])

start = time()

print "analysing files under {}".format(args.path)

for dirname, dirnames, filenames in os.walk(args.path):
    for filename in filenames:
        filelist.append(os.path.join(dirname, filename))

total_files = len(filelist)
print "{} files to analyse...".format(total_files)

last_print_time = time()

for idx,f in enumerate(filelist):
    t = time()
    if t - last_print_time > args.interval:
        wasted = convert_size(wasted_space)
        sys.stdout.write("\r\033[0K");
        sys.stdout.write("{} ({}/{}) (dups: {}, {})".format(f, idx, total_files, dupes, wasted))
        sys.stdout.flush()
        last_print_time = t

    hasher = hashlib.md5()

    try:
        ff = open(f)
    except:
        print "skipping {}".format(f), sys.exc_info()[0]
        continue

    buf = ff.read(BLOCKSIZE)
    while len(buf) > 0:
        hasher.update(buf)
        buf = ff.read(BLOCKSIZE)

    digest = hasher.hexdigest()

    hashed[digest].append(f)
    if len(hashed[digest]) > 1:
        dupes = dupes + 1
        wasted_space += os.fstat(ff.fileno()).st_size

sys.stdout.write("\r")

duplicates = defaultdict(list)

for digest,dups in hashed.iteritems():
    if len(dups) > 1:
        print digest
        duplicates[digest] = dups
        for d in dups:
            print "\t", d

if args.output:
    with open(args.output, 'w') as outfile:
        json.dump(duplicates, outfile)

end = time()
print "elapsed: {} seconds.".format(end - start)
print "wasted space: {}.".format(convert_size(wasted_space))
if args.output:
    print "json report in {}.".format(args.output)
