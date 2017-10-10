#from util import dbconnection

#db=dbconnection.client.kms

from pymongo import MongoClient
import sys
import datetime
import time
from builtins import type

try:
    client = MongoClient()
    print(client)
except:
    print ("Unexpected error:", sys.exc_info()[0])
val=[]
db=client.kms

'''
##No of persons in one room
val=db.personal_detail.aggregate([
    {"$match": { "pevz:raum": {"$exists": "true" }}},
    {"$group":{"_id":"$pevz:raum"}}
    ])
print("-- List of rooms --")
for x in val:
    print(x)


val=db.personal_detail.aggregate([
    {"$match": { "pevz:raum": {"$exists": "true" }}},
    {"$group":{"_id":"$pevz:raum", "No Of People":{"$sum":1}}}
    ])
print("-- No. of people working in a room --")
for x in val:
    print(x)

# No of person in one room sorted
val=db.personal_detail.aggregate([
    {"$match": { "pevz:raum": {"$exists": "true" }}},
    {"$group":{"_id":"$pevz:raum", "No Of People":{"$sum":1}}},
    {"$sort": {"No Of People":-1}}
    
    ])
print("-- No of person in one room sorted --")
for x in val:
    print(x)
    
#No of people working in every organization
val=db.personal_detail.aggregate([
    {"$match": { "pevz:name": {"$exists": "true" }}},
    {"$group":{"_id":"$pevz:name", "No Of People working":{"$sum":1}}},
    {"$sort": {"No Of People working":-1}}
    
    ])
print("-- No of people working in every organization --")
for x in val:
    print(x)

#No of Male/Female
val=db.personal.aggregate([
    {"$match": { "pevz:anrede": {"$exists": "true" }}},
    {"$group":{"_id":"$pevz:anrede", "No Of People ":{"$sum":1}}},
    {"$sort": {"No Of People ":-1}}
    
    ])
print("-- No of Male/Female --")
for x in val:
    print(x)

# List of people whose gender is missing -
val=db.personal.find( { "pevz:anrede": "-" } )
print("-- List of people whose gender is missing --")
for x in val:
    print(x)
   
#Total number of person
val=db.personal.find({"_id":{"$ne": 0}}).count()
print("---Total number of person---\n "+str(val))


#Publication details by First and Last Name
val=db.pubdata.find( { "first_name": "Zoe","last_name": "Clark" } )
print("---Publication details by First and Last Name---")
for x in val:
    print(x)

#Persons with number of publications in descending order
val=db.pubdata.aggregate([{ "$group": { "_id": "$unibi", "No of Publications": { "$sum": 1 } }},{"$sort": {"No of Publications":-1}} ])
print("---Persons with number of publications in descending order---")
for x in val:
    print(x)
'''
#vineet ------------------------------------------------------------------------------------------------------------------------------------


personal_archive_find = db.personal_archive.find({}) 
counter = personal_archive_find.count()
for doc in personal_archive_find:
    x=db.personal.find({"_id":{ "$eq" :doc.get("_id")}})
    for z in x:
        if z:
            counter = counter - 1

print("\nTotal new values added to database: " + str(counter))

##difference data
f = db.personal_archive_differential.find({})
tag = ""
max = 0
for a in f:   
    a_keys = a.keys()
    for key in a_keys:
        if key != "timestamp" and key != "_id" and key != "id" and key != "pevz:aenderung":
            count = db.personal_archive_differential.count({key: {'$exists': 'true'}})
            if count > max:
                max = count
                tag = key
                    
print("\nThe column " + str(tag) + " has been changed " + str(max) + " times.")


f = db.personal_archive_differential.find({})
val = ""
max = 0
for a in f:   
    value = a.get("id")
    count = db.personal_archive_differential.count({"id": { "$eq": value }})
    if count > max:
        max = count
        val = value
                    
print("\nThe id " + str(val) + " has been changed " + str(max) + " times.")

personal_find = db.personal.find({}) 
total = personal_find.count() 

personal_find_dr = db.personal.find({"pevz:titel": {"$regex":'Dr.'}})
counter = personal_find_dr.count() 


personal_find_archive_dr = db.personal_archive.find({"pevz:titel": {"$regex":'Dr.'}})
counter_archive = personal_find_archive_dr.count() 


print("\nTotal number of people in CITEC: " + str(total))

print("\nTotal number of doctorates in CITEC: " + str(counter))


personal_find = db.personal.find({}) 
counter_archive = 0
for doc in personal_find:
    x=db.personal_archive_differential.find({"id":{ "$eq" :doc.get("_id")},"pevz:titel": {"$regex":'Dr.' }})
    for z in x:
        if z:
            counter_archive = counter_archive + 1
print("\nTotal number of recently awarded doctorates in CITEC: " + str(counter_archive))


gtDate = datetime.datetime.strptime("Mon Oct  7 21:19:24 2017","%a %b %d %H:%M:%S %Y")
count = 0
listOfPersons = []
personal_archive_find = db.personal_archive.find({})
for x in personal_archive_find:
    date = x.get("timestamp")
    timeStamp= datetime.datetime.strptime(date,"%a %b %d %H:%M:%S %Y")
   
    if timeStamp > gtDate:
        count = count + 1
        f = x.get("pevz:vorname") +" " +x.get("pevz:nachname")
        listOfPersons.append(f)
        
print("\nNo. of Person who joined Bielefeld University after: " + str(gtDate)[0:10] + " is: " + str(count))

print("\nList of persons: " +str(listOfPersons))

listOfPersonsLeft = []
personal_find = db.personal.find({}) 
counter = personal_find.count()
for doc in personal_find:
    x=db.personal_archive.find({"_id":{ "$eq" :doc.get("_id")}})
    for z in x:
        if z:
            try:
                temp = z.get("pevz:vorname") +" " +z.get("pevz:nachname")
                if type(temp) == str:
                    listOfPersonsLeft.append(temp)
                    counter = counter - 1
            except:
                continue
print("\nTotal values archived in database: " + str(counter))
'''
print("\nList of persons who have left Bielefeld University: " )
newList=[]
for y in listOfPersonsLeft:
    try:
        print(y)
        newList.append(y)
    except:
        continue
print(str(newList))
'''
