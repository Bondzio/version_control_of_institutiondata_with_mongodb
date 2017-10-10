import xml.dom.minidom as  md
import urllib.request
import os
import sys
from pymongo import MongoClient

#db connection
try:
    client = MongoClient()
    print(client)
except:
    print ("Unexpected error:", sys.exc_info()[0])

##parser
db=client.kms
##parser
class InsertDataFromXml:
    
    def __init__(self):
        self.perm_dic={}
    def print_node(self,root):
        if root.childNodes:
            for node in root.childNodes:
                doc={}
                if node.nodeType == node.ELEMENT_NODE :
                    print(self.perm_dic)
                    if node.tagName=="pevz:kontakt":
                        if(self.perm_dic and len(self.perm_dic)>=1) and '_id' in self.perm_dic.keys():                
                            db.personal_detail.insert(self.perm_dic)
                            self.perm_dic={}    
                    if(node.hasAttribute('id')):
                            for m in db.personal_detail.find({"_id":node.attributes['id'].value}):
                                doc=m
                                print('Id already exist'+node.attributes['id'].value)    
                            if( not doc):
                                self.perm_dic['_id']=node.attributes['id'].value
                                print(self.perm_dic['_id'])
                    if (node.childNodes and not doc  and not node.hasAttribute('id')):
                        self.perm_dic[node.tagName]=node.firstChild.data
                    
                    self.print_node(node)
                    

## getting the list of ids from db
personIdParams=list(db.personal.find({},{"_id":1}))
i=0
person=[]
while i<len(personIdParams):
    person.append(personIdParams[i]["_id"])
    i+=1


##TODO Implement delay in scraping (delete files from data)
##scrap files from ekvv to local
'''for personId in person:
    document ='http://ekvv.uni-bielefeld.de/ws/pevz/PersonKontaktdaten.xml?persId='+personId
    urllib.request.urlretrieve (document,"PersonKontaktdaten/persId"+personId+".xml")'''
##read local xml and store to db
for personId in person:
    print(personId)
    dom = md.parse("PersonKontaktdaten/persId"+personId+".xml")
    
    if dom.getElementsByTagName('pevz:kontakt').length>0:
        print('--------------'+personId+'-----------------------')
        root = dom.documentElement
        tmp=InsertDataFromXml()
        tmp.print_node(root)

'''
#os.system('mongoexport -d kms -c personal_detail -o /home/bando/Desktop/output.txt')
#db.personal_detail.insert(ast.literal_eval(json.dumps(re[n]))  )
#print(perm_dic)    
#for doc in db.personal_detail.find():
   # print(doc)   '''     