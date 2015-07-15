import requests

tailrToken = "6c8369343aa0754741225b6f161bb7f0bcd1d388"




userName = "olegsfinest"
repoName = "testRepo"
key = "http://kent.zpr.fer.hr:8080/educationalProgram/all/person"

year = "2015"
month = "07"
day = "12"
hour = "00"
minutes = "00"
seconds = "00"
datetime = year+"-"+month+"-"+day+"-"+hour+":"+minutes+":"+seconds

params = {'key':key,'datetime':datetime}


apiURI = "http://tailr.s16a.org/api/"+userName+"/"+repoName


r = requests.put(apiURI, params=params)

print (r.url)