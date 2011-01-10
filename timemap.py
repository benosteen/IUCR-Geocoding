import logging

from pylons import request, response, session, tmpl_context as c, app_globals as g
from pylons.controllers.util import abort, redirect_to

from iucr.lib.base import BaseController, render

log = logging.getLogger(__name__)

import json

from iucr.lib.helpers import nss, geoannotate   # see lib_helper.py for these

SELECT_TEMPLATE = """PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

SELECT DISTINCT ?c ?name ?address ?doi ?title ?date WHERE {
 ?doi <http://purl.org/dc/elements/1.1/title> ?title . 
 ?doi <http://purl.org/dc/elements/1.1/date> ?date .
 ?doi <http://purl.org/dc/terms/creator> ?c .
 ?c <http://xmlns.com/foaf/0.1/name> ?name .
 ?c <http://open.vocab.org/terms/recordedAddress> ?address .
 FILTER(?date > "%s" && ?date < "%s")
} LIMIT 400"""

""" OUTPUT example:
([ {'title':... , start:"....", point:{lat:, lon: }, options:{'description': } },  ])
"""

import re

from rdflib import URIRef, Literal

HTML_T = """<strong>%s</strong>, <em>%s</em><br/><small>%s</small> <a href="%s">link</a><br/> Match type: %s """ 
# title, date, address, link, geocode info html template for the map popup

dp = re.compile(r"^[0-9]{4}-?[0-9]{2}-?[0-9]{2}$") # date validator for the form XXXX-XX-XX

class TimemapController(BaseController):
    def index(self):
        return render("timemap.html")

    def slice(self):
        params = request.GET
        if "callback" in params and "from" in params and "to" in params:
            fromdate = params['from']
            todate = params['to']
            callback = params['callback']
        else:
            return {}
        dset = []
        if dp.match(fromdate) != None and dp.match(todate) != None:
            resultset = g.s.sparql(SELECT_TEMPLATE % (fromdate, todate), accept="application/xml")
            annotated = geoannotate(resultset)
            for line in annotated:
                if line['typem'] != "no_match":
                    datapoint = {'title':line['name'],
                             'start':line['date'],
                             'point':{'lat':line['lat'],
                                      'lon':line['lon']}}
                    # add description
                    matches = "%s - %s - Partial: %s" % (line['typem'], line['geo'], line['partial'])
                    description = HTML_T % (line['title'], line['date'], line['address'], "http://purl.org/openbibliosets/describe/%s" % line['doi'], matches)
                    datapoint['options'] = {'description':description}
                    dset.append(datapoint)
            
            response.headers['Content-Type'] = "application/json"
            response.charset = "utf8"
            resp = u"%s(%s)" % (callback, json.dumps(dset))
            return resp.encode("utf-8")
