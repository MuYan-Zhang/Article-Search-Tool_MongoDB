import json
import os
import pymongo
from pymongo import MongoClient
from pymongo import TEXT

def main():
  # Input parameters
  input_file = input("Enter input file name/path: ")
  port_num = input("Enter the port number the MongoDB server is running: ")
  
  # Initialize DB
  client = MongoClient("mongodb://localhost:{}".format(port_num))
  db = client["291db"]

  collist = db.list_collection_names()
  if "dblp" in collist:
    db.dblp.drop()
  dblp = db["dblp"]

  # json mongoimport
  command = r'mongoimport --port={} --db=291db --collection=dblp --file={}'.format(port_num, input_file)
  os.system(command)
  
  # Inserts a new field: the $year field converted to string so that text index will also apply to the str_year field
  dblp.update_many(
  { },
  [
    {"$set": {"str_year1": { "$toString": "$year" }}}
  ]
  )

  # Indexing to make querying in later stages efficient
  dblp.create_index([("title", TEXT), ("authors", TEXT), ("abstract", TEXT), ("venue", TEXT), ("str_year", TEXT)], default_language = "none")
  dblp.create_index("id")
  dblp.create_index([("references", pymongo.DESCENDING)])


if __name__ == "__main__":
  main()