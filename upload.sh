#!/bin/bash

# export the token for later use
export TOKEN="94c209f79c54ebc87a684e4b9b8f11f65d8bf50e"

# create a new revision of a resource with an RDF description
curl -X PUT \
  -H "Authorization: token 94c209f79c54ebc87a684e4b9b8f11f65d8bf50e" \
  -H "Content-Type: application/n-triples" \
  --data-binary @/Users/Oleg1/Documents/Wille/Studium/LinkedData/tailr_concurrent-upload/test.nt \
  "http://tailr.s16a.org/api/olegsfinest/test3?key=http://rdf.data-vocabulary.org/rdf.xml"


  # &datetime=2015-07-12-00%3A00%3A00