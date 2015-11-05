#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Create a file for each resource for a set of DBpedia versions from the dumps in order to diff the resources.

Versions for processing are configured in datasets.json

$Id$
"""

import os, errno
import sys, getopt
sys.path.append("/home/paulw/pythonpackages/lib/python2.7/site-packages")
import datetime
import gzip
import requests
import grequests
import time
from collections import defaultdict
import logging
import threading
import urllib
from copy import deepcopy
logger = logging.getLogger('tailrclient.' + __name__)
logging.basicConfig(filename='tailrclient.log', format='%(asctime)s %(levelname)s:%(message)s', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler())

# config variables
import config


#config

currentConnections = 0

failedRequestsUrls = {}
failedRequests = 0


srcpath = config.srcpath
concurrencyLimit = config.concurrencyLimit
#config
userName = config.userName
repoName = config.repoName
tailrToken = config.tailrToken
contentType = config.contentType

dataFilename = config.dataFilename

header = {'Authorization':"token "+tailrToken, 'Content-Type':contentType}
apiURI = "http://tailr.s16a.org/api/"+userName+"/"+repoName
urlPrefixLength = len(apiURI)+len('?key=')

# # in this file every key, that was failed to push will be saved
failedRequestsOutputfilename = config.failedRequestsOutputfilename
# # in this file every quad containing to a key, that was failed to push to will be saved
failedRequestsTriplesFilename = config.failedRequestsTriplesFilename



# currentConnections = 0

# failedRequestsUrls = {}
# failedRequests = 0



# request response objects do not have the original payload with them
# therefore we thore the payloads, until the request is finished
tmpgraphContents = {} 

#responses come asynchronus, too, therefore avoid two failed responses to write to the file
fileWriteLock = threading.Lock()
fileWriteUrlLock = threading.Lock()

def main(argv):
	startTime = time.time()
	processFile(os.path.join(srcpath, dataFilename))
	logging.info("## " + str(failedRequests)+ " requests failed: ")
	# printFailedRequests()
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

						saveTriplesTemporary(key, graphContents[currentGraph])


						push(key, ("\n".join(graphContents[currentGraph])), pool)


						# # would store every key that was pushed to
						# addUrlToFile(currentGraph, urlOutputfileName)

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
		logging.info("## " + str(numberOfGraphs - failedRequests)+ " requests were succesfull")

def processQuad(quad):
	#logging.debug("+++ Quad: " + quad)
	spog = quad.strip(' .').split()
	s, p , o, g = spog[0], spog[1], " ".join(spog[2:-1]), spog[-1]
	#logging.debug("+++ SPOG = " + s + " | " + p + " | " + o + " | " + g)
	return s, p, o, g


def testTriples(key):
	global tmpgraphContents
	print "\n 4: tmpgraphContents["+key+"] in testTriples"
	print tmpgraphContents[key]




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

def saveTriplesTemporary(key, payload):
	global tmpgraphContents
	tmpgraphContents[key] = deepcopy(payload)


def deleteTemporaryTriples(key):
	global tmpgraphContents
	del tmpgraphContents[key]



def printResponse(response, *args, **kwargs) :
	# print (response.url +" returned status code: " + str(response.status_code))
	
	# global currentConnections
	# currentConnections = currentConnections - 1

	# url in response is url encoded unicode -> convert back to normal string
	# the response object also has no list of the params, so we have to manually cut the key out
	url = urllib.unquote(response.url[urlPrefixLength:len(response.url)])

	if response.status_code != 200:
		logging.error("-- "+url +" returned status-code: "+str(response.status_code))

		#If the request failed, save the urls and the related triples to files
		fileWriteUrlLock.acquire()
		try:
			addUrlToFile(url, failedRequestsOutputfilename, "; http-status-code: "+str(response.status_code))
		finally:
			fileWriteUrlLock.release()

		fileWriteLock.acquire()
		try:
			addQuadsToFile(url, failedRequestsTriplesFilename)
		finally:
			fileWriteLock.release()
		
		global failedRequests
		failedRequests += 1

	# in any way delete the temporary saved triples. Anything that went wrong is in a file by now
	deleteTemporaryTriples(url)


def addUrlToFile(url, filepath, annotation=""):
	with open(filepath, 'a') as file:
		file.write(url+" "+annotation+"\n")

# currently not used
# this would require a list with all urls. urls are currently appended to file right away with addUrl()
def saveUrls(urls):
	with open(urlOutputfileName, 'a') as file:
		for url in urls:
			file.write(url+"\n")

def addQuadsToFile(key, filepath):
	global tmpgraphContents
	with open(filepath, 'a') as file:
		for item in tmpgraphContents[key]:
			file.write(item[0:len(item)-1]+"<"+key+"> .\n")

def addQuadsToFileFromSet(tripleSet, key, filepath):
	global tmpgraphContents
	with open(filepath, 'a') as file:
		for item in tripleSet:
			file.write(item[0:len(item)-1]+"<"+key+"> .\n")

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
