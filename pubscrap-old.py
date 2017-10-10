from lxml import etree
##parser

tree = etree.parse('PublicationData/Publication1585315.xml')
#doc = tree.getroot()
print(tree)
print( tree.xpath('//version'))
 #class InsertDataFromXml:
    
#    def __init__(self):
#        self.perm_dic={}
#    def print_node(self,root):



##get list of URLs
'''
lists="https://pub.uni-bielefeld.de/pub_index.txt"
filename="PublicationData/index.txt"
urllib.request.urlretrieve (lists,filename)


with open(filename) as f:
    x = f.readlines()
print (x[2]) 
urls=[]
for url in x:
    urls.append(url.strip())
# (urls) 
'''
## getting the list of ids from db
#personIdParams=list(db.personal.find({},{"_id":1}))
#i=0
#person=[]
#while i<len(personIdParams):
#    person.append(personIdParams[i]["_id"])
#    i+=1

'''
##TODO Implement delay in scraping (delete files from data)
##scrap files from ekvv to local
#
for url in urls:
    document ="https://pub.uni-bielefeld.de/sru?version=1.1&operation=searchRetrieve&query=id="
    id=url.split("/")
    pubId=id[4].strip('\n')
    urllib.request.urlretrieve (document+pubId,"PublicationData/Publication"+pubId+".xml")
    '''
##read local xml and store to db
'''
person=[1610981,1611059,1611073,1611080,1611167,1611220,1611270]
for personId in person:
    print(personId)
    dom = md.parse("PersonKontaktdaten/persId"+personId+".xml")
    
    if dom.getElementsByTagName('pevz:kontakt').length>0:
        print('--------------'+personId+'-----------------------')
        root = dom.documentElement
        tmp=InsertDataFromXml()
        tmp.print_node(root)
'''