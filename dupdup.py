#!/usr/bin/env python

import os
import sys
import hashlib
from collections import defaultdict
from time import time
import math
import json
import argparse
import stat

parser = argparse.ArgumentParser(description='Find duplicate files.')

parser.add_argument("path", type=str, nargs='*', default=[os.getcwd()],
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

start = time()
last_print_time = time()

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "{}{}".format(s, size_name[i])

def print_stats(f, idx, total_files, dupes, wasted):
    global last_print_time
    t = time()
    if t - last_print_time > args.interval:
        wasted = convert_size(wasted_space)
        sys.stdout.write("\r\033[0K");
        sys.stdout.write("{} ({}/{}) (dups: {}, {})".format(f, idx, total_files, dupes, wasted))
        sys.stdout.flush()
        last_print_time = t

def print_walk(path):
    global last_print_time
    t = time()
    if t - last_print_time > args.interval:
        sys.stdout.write("\r\033[0K");
        sys.stdout.write("{}".format(path))
        sys.stdout.flush()
        last_print_time = t

# hash a portion or the totality of the file, and insert it into the dict
# `dups`. Returns the size of the file if it's a suspected duplicate, 0
# otherwise.
def hash_and_insert(f, dups, complete):
    hasher = hashlib.md5()

    try:
        # ignore links and pipes and whatnot
        if stat.S_ISREG(os.stat(f).st_mode):
            ff = open(f)
        else:
            return
    except:
        print "skipping {}".format(f), sys.exc_info()[0]
        return

    buf = ff.read(BLOCKSIZE)
    hasher.update(buf)
    while len(buf) > 0 and complete:
        buf = ff.read(BLOCKSIZE)
        hasher.update(buf)

    digest = hasher.hexdigest()
    dups[digest].append(f)
    if len(dups[digest]) > 1:
        size = os.fstat(ff.fileno()).st_size
    else:
        size = 0

    return size


print "analysing files under {}".format(args.path)

i = 0
for path in args.path:
    for dirname, dirnames, filenames in os.walk(path):
        for filename in filenames:
            full = os.path.join(dirname, filename)
            filelist.append(full)
            i+=1
            if i > 1000:
                i = 0
                print_walk(full)

total_files = len(filelist)
print "{} files to analyse...".format(total_files)

candidates = defaultdict(list)

for idx,f in enumerate(filelist):
    wasted = hash_and_insert(f, candidates, False)

    print_stats(f, idx, total_files, dupes, wasted)
    if wasted > 0:
        dupes = dupes + 1
        wasted_space += wasted

sys.stdout.write("\r")
print "first pass done, {} suspected dupes, {} wasted".format(dupes, convert_size(wasted_space))

total_files = dupes
wasted_space = dupes = total_idx = 0

hashed = defaultdict(list)

for digest,path in candidates.iteritems():
    if len(path) > 1:
        total_idx = total_idx + 1
        for p in path:
            wasted = hash_and_insert(p, hashed, True)

            if wasted > 0:
                dupes = dupes + 1
                wasted_space += wasted

            print_stats(p, total_idx, total_files, dupes, wasted)

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
