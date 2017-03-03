#!/usr/bin/env python

import threading
import argparse
import logging
import json
import os

import GraphToNeo4j

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('inputfile', metavar='FILE', help='Input file name', default="graph.json")
    parser.add_argument('--node_csv', metavar='FILE', help='Nodes csv file')
    parser.add_argument('--edge_csv', metavar='FILE', help='Edges csv file')
    args = parser.parse_args()

    GraphToNeo4j.json2csv(args.inputfile, args.node_csv, args.edge_csv)
