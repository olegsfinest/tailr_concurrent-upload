import requests
import grequests
import time


# ===========================================
# configuration-variables 
# ===========================================
userName = "olegsfinest"
repoName = "test5"
tailrToken = "6c8369343aa0754741225b6f161bb7f0bcd1d388"
contentType = "application/n-triples"
urlListFilePath = "urls.txt"
#workaround for testing
filePath = "/Users/Oleg1/Documents/Wille/Studium/LinkedData/tailr_concurrent-upload/tmpfile.nt"

useOwnDate = False

concurrency_limit = 30	


def push():
	allRequests = set()
	for url in urls:
		if useOwnDate:
			params = {'key':url,'datetime':datetime}
		else:
			params={'key':url}
		allRequests.add( grequests.put(apiURI, params=params, headers=header, data=open(filePath, 'rb'), hooks={'response': printResponse}) )
	
	# could also be done in one line, but would be more difficult to configure	
	# allRequests = (grequests.put(apiURI, params={'key':url}, headers=header, data=open(filePath, 'rb'), hooks={'response': printResponse}) for url in urls)

	startTime = time.time()
	print (str(len(allRequests)) + " keys registered for pushing. ")
	print("Starting Put-Requests:")
	responses = grequests.map(allRequests, stream=False, size=concurrency_limit)



def readURIs ():
	# TODO exception handling for empty file and empty lines
	for line in open(urlListFilePath,'r'):
		urls.append(line)

def exception_handler(request, exception):
	print "Request failed"

def printResponse(response, *args, **kwargs) :
	print (response.url +" returned status code: " + str(response.status_code))
	print ("This took "+str(time.time() - startTime)+" seconds")
	#if not ok, store the uri with http-code
	if response.status_code != 200:
		failedRequestsUrls[response.url] = response.status_code
	# print kwargs
	# print args



# currently not needed 
# When dumps are added for past dates, these can be set
year = "2013"
month = "06"
day = "11"
hour = "13"
minutes = "12"
seconds = "02"
datetime = year+"-"+month+"-"+day+"-"+hour+":"+minutes+":"+seconds






header = {'Authorization':"token "+tailrToken, 'Content-Type':contentType}
apiURI = "http://tailr.s16a.org/api/"+userName+"/"+repoName




urls = []
readURIs()
failedRequestsUrls = {}
#initialize startTime
startTime = time.time()
push()

print("\n" + str(len(failedRequestsUrls)) + " requests failed")





# # single request
# with open(filePath2, 'rb') as payload:
# 	r = requests.put(apiURI, params=params, headers=header, data=payload)

