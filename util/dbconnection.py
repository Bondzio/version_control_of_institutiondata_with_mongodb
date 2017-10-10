from pymongo import MongoClient
import sys
#db connection
try:
    client = MongoClient()
    print(client)
except:
    print ("Unexpected error:", sys.exc_info()[0])
