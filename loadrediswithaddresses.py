import csv
from redis import Redis

f = open("address_table.csv", "r")
cr = csv.reader(f)
headers = cr.next()

def spacedelimit(row):
  # lat, lon, type, geomatch, partial match
  return " ".join([row[7], row[8], row[1], row[2], row[3]])

r = Redis(db=1)

p = r.pipeline()

row = cr.next()
while(row):
  r.set(row[0], spacedelimit(row)) # row[0] => hash key
  row = cr.next()

p.execute()

