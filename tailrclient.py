#!/usr/bin/env python
"""
Create a file for each resource for a set of DBpedia versions from the dumps in order to diff the resources.

Versions for processing are configured in datasets.json

$Id$
"""

import os, errno
import sys, getopt
import datetime
import gzip
from collections import defaultdict
import logging
logger = logging.getLogger('tailrclient.' + __name__)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)

srcpath = "/Users/magnus/Datasets/dyldo/"


def main(argv):
	logging.warn("Hello.")

	processFile(os.path.join(srcpath, '2012-05-06', 'data.nq.gz'))

def processFile(fsrcpath):
	if (not os.path.isfile(fsrcpath)):
		logging.error("--- File not found: " + fsrcpath)
		return
	logging.info("## Process file " + fsrcpath + " ##")
	
	maxlines = 1000
	i = 0
	currentGraph = ''
	graphContents = defaultdict(set)

	with gzip.open(fsrcpath, 'r') as f:
		for line in f:
			line = line.strip()
			# ignore empty and commented lines
			if (line.strip() and line.strip()[0] != '#'):
				s, p, o, g = processQuad(line)
				if (currentGraph != g):
					if (currentGraph != ''):
						logging.debug("-- Write " + currentGraph + "\n" + "\n".join(graphContents[currentGraph]))
						graphContents[currentGraph].clear()
					if (g in graphContents.keys()):
						logging.error("-- Duplicate graph: " + g)
					currentGraph = g

				graphContents[currentGraph].add(s + " " + p + " " + o + " .")

			if (i == maxlines):
				break
			i = i + 1
		logging.debug("-- Write " + currentGraph + "\n" + "\n".join(graphContents[currentGraph]))

def processQuad(quad):
	#logging.debug("+++ Quad: " + quad)
	spog = quad.strip(' .').split()
	s, p , o, g = spog[0], spog[1], " ".join(spog[2:-1]), spog[-1]
	#logging.debug("+++ SPOG = " + s + " | " + p + " | " + o + " | " + g)
	return s, p, o, g

if __name__ == "__main__":
	main(sys.argv[1:])