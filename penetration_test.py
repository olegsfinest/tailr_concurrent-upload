import requests
import grequests
import time
import os
current_path = os.path.dirname(os.path.abspath(__file__))

# ===========================================
# configuration-variables 
# ===========================================

userName = "olegsfinest"
repoName = "test6"
tailrToken = "6c8369343aa0754741225b6f161bb7f0bcd1d388"
contentType = "application/n-triples"
urlListFilePath = "urls2.txt"
key = "http://rdf.data-vocabulary.org/rdf.xml"

concurrency_limit = 30	

# ===========================================
# configuration variables for penetration test
# ===========================================

# number of lines that will be added every iteration
lineSteps = 100000
# if true, the file will be emtied. If False. the script will start with the current number of lines
startNew = True
filePath = current_path+"/penet.nt"



def readURIs ():
	# TODO exception handling for empty file and empty lines
	for line in open(urlListFilePath,'r'):
		urls.append(line)

def exception_handler(request, exception):
	print "Request failed"

def printResponse(response, *args, **kwargs) :
	print (response.url +" returned status code: " + str(response.status_code))
	print ("This took "+str(time.time() - startTime)+" seconds")
	if response.status_code != 200:
		failedRequestsUrls[response.url] = response.status_code
	# print kwargs
	# print args


def push():
	allRequests = set()
	for url in urls:
		allRequests.add( grequests.put(apiURI, params={'key':url}, headers=header, data=open(filePath, 'rb'), hooks={'response': printResponse}) )
		# params={'key':url,'datetime':datetime}
	# allRequests = (grequests.put(apiURI, params={'key':url}, headers=header, data=open(filePath, 'rb'), hooks={'response': printResponse}) for url in urls)

	responses = grequests.map(allRequests, stream=False, size=concurrency_limit)





# ============================
# helper functions
# ============================
def file_len(filename):
	with open(filename) as f:
		numberOfLines = 0
		for i, l in enumerate(f):
			numberOfLines += 1
	return numberOfLines + 1




header = {'Authorization':"token "+tailrToken, 'Content-Type':contentType}
apiURI = "http://tailr.s16a.org/api/"+userName+"/"+repoName

urls = []
readURIs()
failedRequestsUrls = {}


# clear file
if startNew:
	open("/Users/Oleg1/Documents/Wille/Studium/LinkedData/tailr_concurrent-upload/penet.nt", 'w').close()

lineCount = file_len(filePath)

# penetration testing
while len(failedRequestsUrls) == 0:
	# add 2500 lines to file
	print("\nAdding "+str(lineSteps)+" lines")
	for i in range(0, lineSteps):
		# just some unique triple  
		tmpStr = "<http://dbpedia.org/resource/Queens_of_the_Stone_Age> <http://www.w3.org/2002/07/owl#sameAs>  <http://commons.dbpedia.org/resource/Queens_of_the_Stone_Age"+str(i)+"> .\n"
		#append newline and new triple

		with open(filePath, 'a') as file:
			file.write(tmpStr)
		lineCount += 1

	print ("\n \nTrying with "+ str(lineCount) + "lines now")
	# push
	startTime = time.time()
	push()


print("\n" + str(len(failedRequestsUrls)) + " requests failed")




