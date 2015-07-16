import requests

tailrToken = "6c8369343aa0754741225b6f161bb7f0bcd1d388"

filePath = "/Users/Oleg1/Documents/Wille/Studium/LinkedData/tailr_concurrent-upload/test.nt"
filePath2 = "/Users/Oleg1/Documents/Wille/Studium/LinkedData/tailr_concurrent-upload/test2.nt"


userName = "olegsfinest"
repoName = "testRepo"
key = "http://rdf.data-vocabulary.org/rdf.xml"
key2 = "http://kent.zpr.fer.hr:8080/educationalProgram/all/person"

year = "2015"
month = "07"
day = "12"
hour = "00"
minutes = "00"
seconds = "00"
datetime = year+"-"+month+"-"+day+"-"+hour+":"+minutes+":"+seconds


# params = {'key':key,'datetime':datetime}
params = {'key':key2}



contentType = "application/n-triples"

header = {'Authorization':"token "+tailrToken, 'Content-Type':contentType}


apiURI = "http://tailr.s16a.org/api/"+userName+"/"+repoName


with open(filePath2, 'rb') as payload:

	r = requests.put(apiURI, params=params, headers=header, data=payload)

print (r.url)
print (r.reason)