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


concurrencyLimit = 150
#config
userName = "olegsfinest"
repoName = "test9"
tailrToken = "6c8369343aa0754741225b6f161bb7f0bcd1d388"
contentType = "application/n-triples"

header = {'Authorization':"token "+tailrToken, 'Content-Type':contentType}
apiURI = "http://tailr.s16a.org/api/"+userName+"/"+repoName

urlOutputfileName = "graphUrls.md"



def main(argv):
	logging.warn("Hello.")

	startTime = time.time()
	processFile(os.path.join(srcpath, 'data_sorted.nq.gz'))
	logging.info("## " + str(len(failedRequestsUrls))+ " requests failed: ")
	printFailedRequests()
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
					if (g in graphContents.keys()):
						logging.error("-- Duplicate graph: " + g)
					elif (currentGraph != ''):
						# logging.debug("-- Write " + currentGraph + "\n" + "\n".join(graphContents[currentGraph]))

						key = currentGraph
						if checkForBrackets(key):
							key = cutoffBrackets(key)

						push(key, ("\n".join(graphContents[currentGraph])), pool)

						addUrl(currentGraph)
						graphContents[currentGraph].clear()
						numberOfGraphs += 1

						# # Nummber of Connections Logging
						# global currentConnections
						# currentConnections = currentConnections + 1
						# logging.debug(" === Current Connections: " + str(currentConnections) + "\n")

					
					currentGraph = g

				graphContents[currentGraph].add(s + " " + p + " " + o + " .")

			# if (i == maxlines):
			# 	break
			# i = i + 1
		# logging.debug("-- Write " + currentGraph + "\n" + "\n".join(graphContents[currentGraph]))

		# wait for all reqests to finish
		pool.join()

		print("\n")
		logging.info("## Finished pushing " + str(numberOfGraphs) + " graphs")
		logging.info("## " + str(numberOfGraphs - len(failedRequestsUrls))+ " requests were succesfull")

def processQuad(quad):
	#logging.debug("+++ Quad: " + quad)
	spog = quad.strip(' .').split()
	s, p , o, g = spog[0], spog[1], " ".join(spog[2:-1]), spog[-1]
	#logging.debug("+++ SPOG = " + s + " | " + p + " | " + o + " | " + g)
	return s, p, o, g


def push(key, payload, pool):
	logging.debug("+++++ Pushing: " + key + "\n")

	# TODO fetch date of resource somehow, otherwise server will use its own time
	params={'key':key}
	#asynchronus put-request
	req = grequests.put(apiURI, params=params, headers=header, data=payload, hooks={'response': printResponse})
	grequests.send(req, pool)


def pushWithFile(key, filePath, pool):
	params={'key':key}
	req = grequests.put(apiURI, params=params, headers=header, data=open(filePath, 'rb'), hooks={'response': printResponse})
	grequests.send(req, pool)

def printResponse(response, *args, **kwargs) :
	# print (response.url +" returned status code: " + str(response.status_code))
	global currentConnections
	currentConnections = currentConnections - 1
	#if not ok, store the uri with http-code
	if response.status_code != 200:
		failedRequestsUrls[response.url] = response.status_code

def addUrl(url):
	with open(urlOutputfileName, 'a') as file:
		file.write(url+"\n")

# currently not used
# this would require a list with all urls. urls are currently appended to file right away with addUrl()
def saveUrls(urls):
	with open(urlOutputfileName, 'a') as file:
		for url in urls:
			file.write(url+"\n")


def checkForBrackets(s):
	if s[0] == "<" and s[-1] == ">":
		return True
	return False

def cutoffBrackets(s):
	return s[1:len(s)-1]

def printFailedRequests():
	for k, v in failedRequestsUrls:
		print(k + " returned status-code: "+ v)

if __name__ == "__main__":
	main(sys.argv[1:])
