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
import requests
import grequests
import time
from collections import defaultdict
import logging
logger = logging.getLogger('tailrclient.' + __name__)
logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)

srcpath = "/Users/Oleg1/Documents/Wille/Studium/LinkedData/tailr_concurrent-upload"

currentConnections = 0

failedRequestsUrls = {}


concurrencyLimit = 100
#config
userName = "olegsfinest"
repoName = "test6"
tailrToken = "6c8369343aa0754741225b6f161bb7f0bcd1d388"
contentType = "application/n-triples"

header = {'Authorization':"token "+tailrToken, 'Content-Type':contentType}
apiURI = "http://tailr.s16a.org/api/"+userName+"/"+repoName



def main(argv):
	logging.warn("Hello.")

	startTime = time.time()
	processFile(os.path.join(srcpath, 'data_sorted.nq.gz'))
	logging.info("## " + str(len(failedRequestsUrls))+ " requests failed")
	logging.info("## This took "+str(time.time() - startTime)+" seconds")

def processFile(fsrcpath):
	if (not os.path.isfile(fsrcpath)):
		logging.error("--- File not found: " + fsrcpath)
		return
	logging.info("## Process file " + fsrcpath + " ##")

	numberOfGraphs = 0
	
	# maxlines = 100000
	# i = 0
	currentGraph = ''
	graphContents = defaultdict(set)

	pool = grequests.Pool(concurrencyLimit)

	with gzip.open(fsrcpath, 'r') as f:
		for line in f:
			line = line.strip()
			# ignore empty and commented lines
			if (line.strip() and line.strip()[0] != '#'):
				s, p, o, g = processQuad(line)
				if (currentGraph != g):
					if (currentGraph != ''):
						# logging.debug("-- Write " + currentGraph + "\n" + "\n".join(graphContents[currentGraph]))
						logging.debug("-- Pushing: " + currentGraph + "\n")
						global currentConnections
						currentConnections = currentConnections + 1
						logging.debug(" ================ Current Connections: " + str(currentConnections) + "\n")

						# TODO fetch date of resource somehow, otherwise server will use its own time
						params={'key':currentGraph}
						#asynchronus put-request
						req = grequests.put(apiURI, params=params, headers=header, data=("\n".join(graphContents[currentGraph])), hooks={'response': printResponse})
						grequests.send(req, pool)

						graphContents[currentGraph].clear()
						numberOfGraphs += 1
					if (g in graphContents.keys()):
						logging.error("-- Duplicate graph: " + g)
					currentGraph = g

				graphContents[currentGraph].add(s + " " + p + " " + o + " .")

			# if (i == maxlines):
			# 	break
			# i = i + 1
		# logging.debug("-- Write " + currentGraph + "\n" + "\n".join(graphContents[currentGraph]))
		print("\n")
		logging.info("## Finished pushing " + str(numberOfGraphs) + " graphs")

def processQuad(quad):
	#logging.debug("+++ Quad: " + quad)
	spog = quad.strip(' .').split()
	s, p , o, g = spog[0], spog[1], " ".join(spog[2:-1]), spog[-1]
	#logging.debug("+++ SPOG = " + s + " | " + p + " | " + o + " | " + g)
	return s, p, o, g


def printResponse(response, *args, **kwargs) :
	# print (response.url +" returned status code: " + str(response.status_code))
	global currentConnections
	currentConnections = currentConnections - 1
	#if not ok, store the uri with http-code
	if response.status_code != 200:
		failedRequestsUrls[response.url] = response.status_code


if __name__ == "__main__":
	main(sys.argv[1:])
