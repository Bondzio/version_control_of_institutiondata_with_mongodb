import xml.dom.minidom as  md
import urllib.request
import os
import sys
#from util import dbconnection
from pymongo import MongoClient
import sys
#db connection
try:
    client = MongoClient()
    print(client)
except:
    print ("Unexpected error:", sys.exc_info()[0])

##parser
db=client.kms
#db=dbconnection.client.kms
from lxml import etree
from io import StringIO, BytesIO
import xml.etree.ElementTree
#e = xml.etree.ElementTree.parse(StringIO('C:\Users\HighSchool\Downloads\Publication1585315.xml'))
#print(e.tostring())
from xml.dom import minidom                      
#Loop here to parse every file and store the data from the variables(published_year, genre, title, 
#details of each person(first_name, last_name, author_role,unibi)) declared below. store the data of each person using the 
#loop 'for m in j.iter():' 

#lists="https://pub.uni-bielefeld.de/pub_index.txt"
filename="PublicationData/index.txt"
#urllib.request.urlretrieve (lists,filename)
#print (filename)



perm_dic={}
with open(filename) as f:
    x = f.readlines() 
urls=[]
count=0
for url in x:
    urls.append(url.strip())
    urlstemp = urls[count]
    urls[count]=urlstemp[-7:]
    #print(url)
    document ="https://pub.uni-bielefeld.de/sru?version=1.1&operation=searchRetrieve&query=id="
    #urllib.request.urlretrieve (document+urls[count],"PublicationData/Publication"+urls[count]+".xml")
    count=count+1
    
for url in urls:
    perm_dic.clear()
    print(url)
    file="PublicationData//Publication"+url+".xml"
    print(file)
    xmldoc = minidom.parse(file)  
    
    root = etree.fromstring(xmldoc.toxml())
    for i in root.iter('{http://www.loc.gov/zing/srw/}records'):
        for j in i.iter():
            #print j.tag
            if(j.tag == '{http://www.loc.gov/mods/v3}dateIssued'):
                #title of the publication
                published_year=j.text
                #print (published_year) 
                perm_dic['published_year']=published_year
            if(j.tag == '{http://www.loc.gov/mods/v3}genre'):
                #title of the publication
                genre=j.text
                #print (genre)
                perm_dic['genre']=genre
            if(j.tag == '{http://www.loc.gov/mods/v3}title'):
                #title of the publication
                #and j.getparent.tag == '{http://www.loc.gov/mods/v3}titleInfo'
                #RelatedItem has the same title and title info tag so it was overwriting the orignal title name due
                #to which we check the parents to make sure it is different
                t=j.getparent()
                u=t.getparent()
                if(u.tag != '{http://www.loc.gov/mods/v3}relatedItem' ):
                    title=j.text
                #print (title)
                perm_dic['title']=title
            #iterating through the name of each people 
            if(j.tag == '{http://www.loc.gov/mods/v3}name' and j.attrib.get('type')=='personal'):
                #details of all people in the publication can be fetched here, just print j.text but we are only interested in 
                #the details of the author who were from uni bielefeld
                for k in j.iter():
                    #Details of people in university of bielefeld
                    if(k.attrib.get('type')=='unibi'):
                        for m in j.iter():
                            if(m.attrib.get('type') == 'given'):
                                first_name=m.text
                            if(m.attrib.get('type') == 'family'):
                                last_name=m.text
                            if(m.attrib.get('type') == 'text'):
                                author_role=m.text
                            if(m.attrib.get('type') == 'unibi'):
                                unibi=m.text
                        #print (first_name, last_name, author_role,unibi)
                        perm_dic['first_name']=first_name
                        perm_dic['last_name']=last_name
                        perm_dic['author_role']=author_role
                        perm_dic['unibi']=unibi
                        try:
                            db.pubdata.insert(perm_dic)
                        except:
                            continue
                        #print(perm_dic)