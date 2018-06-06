#!/usr/bin/env python3
"""Parse tabular file."""
import argparse
import csv
import gzip
import os

import xlrd


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Parse tabular file")
    parser.add_argument('input_file', help="Tabular file.")
    parser.add_argument('output_file', help="Output file.")
    return parser.parse_args()


def main():
    """Run the program."""
    args = parse_arguments()

    ext = os.path.splitext(args.input_file)[-1].lower()
    with gzip.open(args.output_file, mode='wt') as outfile:
        csvwriter = csv.writer(outfile, delimiter=str('\t'), lineterminator='\n')

        try:
            if ext in ('.tab', '.txt', '.tsv'):
                with open(args.input_file) as infile:
                    for line in infile:
                        outline = line.strip().split('\t')
                        csvwriter.writerow(outline)
            elif ext == '.csv':
                with open(args.input_file) as infile:
                    for line in infile:
                        outline = line.strip().split(',')
                        csvwriter.writerow(outline)
            elif ext in ('.xls', '.xlsx'):
                workbook = xlrd.open_workbook(args.input_file)
                worksheet = workbook.sheets()[0]
                for rownum in range(worksheet.nrows):
                    csvwriter.writerow(worksheet.row_values(rownum))
            else:
                print('{"proc.error":"File extension not recognized."}')
        except:  # noqa
            print('{"proc.error":"Corrupt or unrecognized file."}')
            raise


if __name__ == "__main__":
    main()
