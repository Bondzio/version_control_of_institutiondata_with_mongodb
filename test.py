import xml.dom.minidom as  md
import urllib.request
import os
import sys
from pymongo import MongoClient
import time
 
now = str(time.strftime("%c"))
#db connection
try:
    client = MongoClient()
    print(client)
except:
    print ("Unexpected error:", sys.exc_info()[0])

##parser
db=client.kms

##database backup incremental
'''
x=db.personal.find({})
print(x)

for c in x:
    try:  
        if db.personal_archive.find({"_id":{ "$eq" :c.get("_id")},"$and":[{"pevz:vorname":{ "$ne" :c.get("pevz:vorname")}, "pevz:nachname":{ "$ne" :c.get("pevz:nachname")}}]}):
            print("id matched")
            db.personal_archive_differential.insert(c)
    except:
        print("Exception")
        continue
    
print("executed")


##difference between 1st version and downloaded

x=db.personal.find({})
print(x)

for c in x:
    try:  
        y= db.personal_first_copy.find({"_id":{ "$eq" :c.get("_id")}})
        length = set(x.items()) & set(y.items())
        print(length)
           # db.personal_first_copy.insert(c)
    except:
        print("Exception", sys.exc_info()[0])
        continue
    
print("executed")
'''

##difference data 
x=db.personal.find({})
count = 0
value =dict()
for c in x:
    try:    
        y=db.personal_archive_differential.find({"_id":{ "$eq" : c.get("_id")}})
        temp_c = c.keys()
        for a in y:
            
            for temp in temp_c:
                if temp != "timestamp":
                    if ((a.get("_id") == c.get("_id")) and (a.get(temp) != c.get(temp))):
                        #print(temp)
                        if c.get(temp) == '#':
                            value["newValue:" + temp]= "-deleted-"
                        else:
                            value["newValue:" + temp]= c.get(temp)
                        value["previousValue:" + temp]= a.get(temp)
                        value["_id"]= c.get("_id")
                        value["timestamp"]= a.get("timestamp")
                        value["newtimestamp"]= c.get("timestamp")
                        print(value)
                        count = count + 1
                        #value.clear()
                    else:
                        continue
    except:
        #print("Exception", sys.exc_info())
        continue
print("Total values changed from first version:" + str(count))

'''
f =db.personal.find({"_id":{ "$eq" : "18201363"}})
for a in f:
    print(a)
'''

   

##minimize storage space using differential backup (done)

##difference between 1st version and downloaded (done) 

##incremental means difference between two consecutive version (id, field, value)

##how svn tracks changes & look for paper on for version control(https://git-scm.com/book/en/v2/Git-Internals-Plumbing-and-Porcelain)

#HANDLE CHANGES IN DATABASE DIFERENT WAYS(List possible solution) #complete backup dump everyday

#wiki changes tracking(study) - (MEDIA-WIKI: INSTALL) #DIFFCHECKER.COM

##download more sets of data(day1,day2,day3,day4) -> everyday complete download(5 days)

## changed values from archive diff data display
