import httplib2

from urllib import urlencode

h = httplib2.Http()

from time import sleep

from lxml import etree

import re

import os

try:
  import json
except ImportError:
  import simplejson as json

class OverqueryLimit(Exception):
  pass

def result_to_ds(result_list):
  result = result_list[0]
  if len(result_list) != 1:
    print "Gets here"
    result = None
    # priority list: 
    priority = ['street_address', 'establishment', 'postal_code', 'locality', 'route', 'point_of_interest']
    for t in priority:
      if result == None:
        for r in result_list:
          if r.find("type").text == t:
            result = r
            break
  ds = {}
  if result is None:
    return ds
  rtype = result.find("type")
  if rtype is not None:
    ds['type'] = rtype.text
  faddr = result.find("formatted_address")
  if faddr is not None:
    ds['addr'] = faddr.text
  for address_comp in result.findall("address_component"):
    if "country" in [x.text for x in address_comp.findall("type")]:
      name = address_comp.find("long_name")
      code = address_comp.find("short_name")
      if name is not None:
        ds['country'] = name.text
      if code is not None:
        ds['country_code'] = code.text
  lat, lng = result.find("geometry/location/lat"), result.find("geometry/location/lng")
  if (lat is not None and lng is not None):
    ds['lat'] = lat.text
    ds['lng'] = lng.text
  gtype = result.find("geometry/location_type")
  if gtype is not None:
    ds['geomatch'] = gtype.text
  match = result.find("partial_match")
  if match is not None:
    ds['partial_match'] = match.text
  return ds

def geocode_call(addr):
  # rate limit :(
  addr = re.sub("\s*,?\s*\n\s*\s*", ", ", addr)
  addr = re.sub("\\\\\"", "", addr)
  addr = re.sub("\\\\\'", "", addr)
  params = {'address':addr, 'sensor':'false'}
  url = "http://maps.googleapis.com/maps/api/geocode/xml?" + urlencode(params)
  (resp, content) = h.request(url, "GET")
  d = etree.fromstring(content)
  statusxml = d.xpath("//GeocodeResponse/status")
  assert len(statusxml) == 1
  status = statusxml[0].text
  if status == "OVER_QUERY_LIMIT":
    raise OverqueryLimit()
  return status, d, content

def gc(addr, hashval):
  if os.path.isfile("%s.json" % hashval):
    with open("%s.json" % hashval, "r") as fh:
      ds = json.load(fh)
    if ds['type'] != "ZERO_RESULTS" or ds['type'] != "FAIL":
      return ds
  if os.path.isfile("%s.xml" % hashval):
    try:
      d = etree.parse("%s.xml" % hashval)
      statusxml = d.xpath("//GeocodeResponse/status")
      status = statusxml[0].text
      if status != "ZERO_RESULTS":
        result = d.xpath("//GeocodeResponse/result")
        if result:
          ds = result_to_ds(result)
          if ds:
            with open("%s.json" % hashval, "w") as f:
              f.write(json.dumps(ds))
              print "Wrote %s.json" % hashval
              return ds
        else:
          # bad xml?
          pass
      else:
        return {}
    except Exception, e:
      print e
      return {}

  status, d, content = geocode_call(addr)
  # if no results, try lopping off the first line
  if status == "ZERO_RESULTS":
    # try losing the first chunk and trying again:
    if addr.find(",") > -1:
      minus_addr = addr.split(",", 1)[1].strip()
      # rate limit
      sleep(3)
      status, d, content = geocode_call(minus_addr)

  if status == "ZERO_RESULTS":
    # Fail
    with open("%s.xml" % hashval, "w") as f:
      f.write(content)
    return {}
  elif status == "OK":
    # cache addr lookup
    with open("%s.xml" % hashval, "w") as f:
      f.write(content)
    result = d.xpath("//GeocodeResponse/result")
    ds = result_to_ds(result)
    if ds:
      with open("%s.json" % hashval, "w") as f:
        f.write(json.dumps(ds))
    sleep(9)
  return ds

def cache_json():
  for xmlfile in [x for x in os.listdir(".") if x.endswith("xml")]:
    print xmlfile
    if not os.path.isfile("%s.json" % xmlfile[:-4]):
      d = etree.parse(xmlfile)
      statusxml = d.xpath("//GeocodeResponse/status")
      status = statusxml[0].text
      if status not in ["OVER_QUERY_LIMIT", "ZERO_RESULTS", "UNKNOWN_ERROR"]:
        result = d.xpath("//GeocodeResponse/result")
        ds = result_to_ds(result)
        with open("%s.json" % xmlfile[:-4], "w") as f:
          f.write(json.dumps(ds))
          print "Wrote %s.json" % xmlfile[:-4]

def get_address_hash():
  with open("addresses", "r") as f:
    addr = {}
    for line in f:
      hashval, addr_line = line.split(" ", 1)
      addr[hashval] = addr_line[7:-3]
  return addr

def gather():
  g = get_address_hash()
  hashvals = g.keys()
  ind = 0
  while(ind<len(g.keys())):
    try:
      print "Looking up '%s' - %s" % (g[hashvals[ind]], hashvals[ind])
      ds = gc(g[hashvals[ind]], hashvals[ind])
      ind = ind + 1
    except OverqueryLimit:
      # Hit the barrier, sleep for an hour and retry
      print "Hit rate limit, sleeping for an hour from..."
      from datetime import datetime
      print datetime.now().isoformat()
      sleep(60*60)
