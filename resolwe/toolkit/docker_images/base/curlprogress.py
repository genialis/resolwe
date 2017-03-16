#!/usr/bin/env python3
"""Utility to reformat cURL progress to process progress."""
import argparse
import sys

parser = argparse.ArgumentParser(description="Reformat cURL progress to process progress.")
parser.add_argument("--scale", type=float, default=1., help="max progress at the end of transfer")
parser.add_argument("--nprints", type=int, default=100, help="number of progress reports")
args = parser.parse_args()

nprints = args.nprints
scale = args.scale
step = scale / nprints
milestone = step

while True:
    line = sys.stdin.readline()

    if not line:
        break

    perc = float(line) * scale / 100.

    if perc >= milestone:
        print('{{"proc.progress":{}}}'.format(perc))
        milestone += step

print('{{"proc.progress":{}}}'.format(scale))
