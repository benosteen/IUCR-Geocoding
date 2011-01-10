"""Helper functions

Consists of functions to typically be used within templates, but also
available to Controllers. This module is available to templates as 'h'.
"""
# Import helpers as desired, or define your own, ie:
#from webhelpers.html.tags import checkbox, password

from hashlib import md5

from redis import Redis

import re

def geoannotate(rset):
  ha = [md5(re.sub("\\n", ", ", x['address'])).hexdigest() for x in rset]
  r = Redis(db=1)
  pipeline = r.pipeline()
  for h in ha:
    pipeline.get(h)
  values = pipeline.execute()
  annotated = []
  for ind in xrange(len(rset)):
    if values[ind]:
      rset[ind]['lat'], rset[ind]['lon'], rset[ind]['typem'], rset[ind]['geo'], rset[ind]['partial'] = values[ind].split(" ")
    else:
      rset[ind]['lat'], rset[ind]['lon'], rset[ind]['typem'], rset[ind]['geo'], rset[ind]['partial'] = ("","","no_match","NONE","")  
  del r
  return rset

nss = {"prism": "http://prismstandard.org/namespaces/1.2/basic/",
       "dc": "http://purl.org/dc/elements/1.1/",
       "dcterms": "http://purl.org/dc/terms/",
       "ov":"http://open.vocab.org/terms/",
       "foaf":"http://xmlns.com/foaf/0.1/",
       "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
           } 

