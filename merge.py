#!/usr/bin/env python

# With a `source` and a `destination` directory, find all the files in `source`
# that are not in `destination`, based on the contents of the file (regardless
# of their name), and writes a file containing `mkdir` and `mv` commands to
# effectively merge the directories.

import os
import sys
import hashlib
from collections import defaultdict
from time import time
import math
import json
import argparse
import stat
import errno
import os

parser = argparse.ArgumentParser(description='Merge a source directory into a destination directory, taking file content into account')

parser.add_argument("-i", "--inputpath", type=str, help="input dir")
parser.add_argument("-o", "--outputpath", type=str, help="output dir")
parser.add_argument("-f", "--outputfile", type=str, help="output file for report", default="merge_script.sh")
parser.add_argument("-t", "--interval", type=int, default=1,
                    help="interval at which print progress in seconds")

args = parser.parse_args()

BLOCKSIZE = 2**16
last_print_time = time()

def convert_size(size_bytes):
   if size_bytes == 0:
       return "0B"
   size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
   i = int(math.floor(math.log(size_bytes, 1024)))
   p = math.pow(1024, i)
   s = round(size_bytes / p, 2)
   return "{}{}".format(s, size_name[i])

# Print whether or not we've found duplicate files in the source or destination
# themselves, while processing them.
def print_stats(f, idx, total_files, dupes, wasted):
    global last_print_time
    t = time()
    if t - last_print_time > args.interval:
        wasted = convert_size(wasted)
        sys.stdout.write("\r\033[0K")
        sys.stdout.write("{} ({}/{}) (dups: {}, {})".format(f, idx, total_files, dupes, wasted))
        sys.stdout.flush()
        last_print_time = t

# Print the status of the directory walk
def print_walk(path):
    global last_print_time
    t = time()
    if t - last_print_time > args.interval:
        sys.stdout.write("\r\033[0K");
        sys.stdout.write("{}".format(path))
        sys.stdout.flush()
        last_print_time = t

# Like `mkdir -p`
def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

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

# Given a file list, hash an insert into a candidates list the full path of the file.
def analyze_directory(file_list):
    candidates = defaultdict(list)
    dupes = 0
    wasted_space = 0
    for idx,f in enumerate(file_list):
        wasted = hash_and_insert(f, candidates, True)

        print_stats(f, idx, total_files, dupes, wasted_space)
        if wasted > 0:
            dupes = dupes + 1
            wasted_space += wasted

    return candidates

# List all files under `path` and put their full path in a list, that is
# returned.
def list_directory(path):
    print "analysing files under {}".format(path)
    i = 0
    filelist = []
    for dirname, dirnames, filenames in os.walk(path):
        for filename in filenames:
            full = os.path.join(dirname, filename)
            filelist.append(full)
            print_walk(full)

    return filelist

# With a source duplicate candidates list, and a destination candidates list,
# find the files in the source that are not in the destination. Create commands
# to create the directory hierarchies as needed, and create the commands to
# move the files at the right place.
def process_candidates(candidates_source, candidates_dest):
    output_commands = []

    for digest, path in candidates_source.iteritems():
        if digest not in candidates_dest:
            new_path = path[0].replace(args.inputpath, args.outputpath)
            if os.path.isfile(new_path):
                sys.stderr.write("File conflict: {} exists\n".format(new_path))
                new_path = "conflict-"+new_path

            output_commands.append("mkdir -p \"{}\"\n".format(os.path.dirname(new_path)))
            output_commands.append("mv \"{}\" \"{}\"\n".format(path[0], new_path))

    output_commands = list(set(output_commands))
    output_commands.sort()

    return output_commands

filelist_source = list_directory(args.inputpath)
filelist_dest = list_directory(args.outputpath)

total_files = len(filelist_source)
print "Source: {} files to analyse...".format(total_files)
candidates_source = analyze_directory(filelist_source)

total_files = len(filelist_dest)
print "Dest: {} files to analyse...".format(total_files)
candidates_dest = analyze_directory(filelist_dest)

output_commands = process_candidates(candidates_source, candidates_dest)

output_file = open(args.outputfile, "w");
for line in output_commands:
    output_file.write(line)
