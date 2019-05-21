#!/usr/bin/env python
"""
Query the Knowledge Graph API https://developers.google.com/knowledge-graph/

"""

import argparse
import datetime
import requests
import json
import urllib


def main(query):
    #api_key = open('.api_key').read()
    # api_key = 'AIzaSyDCjMM5M-m9pyXLgL-Dn9IjKshR_-S_fho'
    api_key = 'AIzaSyBj0Iaj1r8W9qyinffu9J3vpNmGL5t_Xvg'
    # api_key = 'AIzaSyCVF5bJdujvRr4HT-_cfz5DjLxGI_1ZcJQ'
    header = {'User-agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36'}
    service_url = 'https://kgsearch.googleapis.com/v1/entities:search'

    params = {
        'query': query,
        'limit': 1,
        'indent': True,
        'key': api_key,

        }

    url = service_url + '?' + urllib.parse.urlencode(params)  # TODO: use requests
    #response = json.loads(urllib.request.urlopen(url).read())
    request=urllib.request.Request(url,headers=header)
    print(url)
    response=urllib.request.urlopen(request,timeout=20).read()
    response=json.loads(response)

    # Parsing the response  TODO: log all responses
    print('Displaying results...' + ' (limit: ' + str(params['limit']) + ')\n')
    if (response['itemListElement'] == []):
        return 'null'
    else:
        for element in response['itemListElement']:
            try:
                types = str(", ".join([n for n in element['result']['@type']]))
            except KeyError:
                types = "N/A"

            try:
                desc = str(element['result']['description'])
            except KeyError:
                desc = "N/A"

            try:
                name = str(element['result']['name'])
            except KeyError:
                name = query

            try:
                #detail_desc = str(element['result']['detailedDescription']['articleBody'])[0:100] + '...'
                detail_desc = str(element['result']['detailedDescription']['articleBody'])
            except KeyError:
                detail_desc = "N/A"

            try:
                mid = str(element['result']['@id'])
            except KeyError:
                mid = "N/A"

            try:
                url = str(element['result']['url'])
            except KeyError:
                url = "N/A"

            try:
                score = str(element['resultScore'])
            except KeyError:
                score = "N/A"

            #print(element['result']['name'] +name \
            desc = desc.replace('\n',' ')
            desc = desc.replace('\t', ' ')
            detail_desc = detail_desc.replace('\n',' ')
            detail_desc = detail_desc.replace('\t', ' ')
            print(' - name:  ' +name \
                    + '\n' + ' - entity_types: ' + types \
                    + '\n' + ' - description: ' + desc \
                    + '\n' + ' - detailed_description: ' + detail_desc \
                    + '\n' + ' - identifier: ' + mid \
                    + '\n' + ' - url: ' + url \
                    + '\n' + ' - resultScore: ' + score \
                    + '\n')
            return types,desc,detail_desc,score

if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('query', help='The search term to query')
    # args = parser.parse_args()
    # main(args.query)
    poi=list()
    with open("E:\\POIData\\poi_name3.txt",encoding='utf-8') as adictfile:
        for line in adictfile:
            toks = line.strip()
            poi.append(toks)
    print ("____________________________________")
    fileObject = open('E:\\POIData\\poi_des20.txt', 'a+',encoding='utf-8')
    for query in poi:
        print(query)
        list = main(query)
        if (list == 'null'):
            continue
        else:
            types,desc,detail_desc,score = list
            fileObject.write(query+'\t'+str(types)+'\t'+str(desc)+'\t'+str(detail_desc)+'\t'+str(score)+'\n')
        # fileObject.write(str(list)+'\n')
"""
Sample result: https://kgsearch.googleapis.com/v1/entities:search?query=taylor+swift&key=[]&limit=1&indent=True

{
  "@context": {
    "@vocab": "http://schema.org/",
    "goog": "http://schema.googleapis.com/",
    "EntitySearchResult": "goog:EntitySearchResult",
    "detailedDescription": "goog:detailedDescription",
    "resultScore": "goog:resultScore",
    "kg": "http://g.co/kg"
  },
  "@type": "ItemList",
  "itemListElement": [
    {
      "@type": "EntitySearchResult",
      "result": {
        "@id": "kg:/m/0dl567",
        "name": "Taylor Swift",
        "@type": [
          "Thing",
          "Person"
        ],
        "description": "Singer-songwriter",
        "image": {
          "contentUrl": "http://t1.gstatic.com/images?q=tbn:ANd9GcQmVDAhjhWnN2OWys2ZMO3PGAhupp5tN2LwF_BJmiHgi19hf8Ku",
          "url": "https://en.wikipedia.org/wiki/Taylor_Swift",
          "license": "http://creativecommons.org/licenses/by-sa/2.0"
        },
        "detailedDescription": {
          "articleBody": "Taylor Alison Swift is an American singer-songwriter.
          Raised in Wyomissing, Pennsylvania, she moved to Nashville, Tennessee, at the age of 14
          to pursue a career in country music. ",
          "url": "http://en.wikipedia.org/wiki/Taylor_Swift",
          "license": "https://en.wikipedia.org/wiki/Wikipedia:Text_of_Creative_Commons_Attribution-ShareAlike_3.0_Unported_License"
        },
        "url": "http://www.taylorswift.com/"
      },
      "resultScore": 884.364868
    }
  ]
}
"""


# #-*- coding: utf-8 -*
# import json
# import urllib.request, urllib.parse, urllib.error
# import requests
#
# class GooleKGAPI(object):
#     def __init__(self):
#         #self.api_key = open('.api_key').read()
#         self.api_key = 'AIzaSyDCjMM5M-m9pyXLgL-Dn9IjKshR_-S_fho'
#     def getResult(self,query):
#         header = {'User-agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.86 Safari/537.36'}
#         service_url = 'https://kgsearch.googleapis.com/v1/entities:search'
#         params = {
#             'query': query,
#             'limit': 1,
#             'indent': True,
#             'key': self.api_key,
#         }
#         url = service_url + '?' + urllib.parse.urlencode(params)
#         resp = requests.get(url=url, headers=header, params=params)
#         data = json.loads(resp.text)
#         print(url)
#         print(data)
#         if (data['itemListElement'] == []):
#             # print('null')
#             return 'null'
#         else:
#             # print(data['itemListElement'][0]['result']['description'] + '.' + data['itemListElement'][0]['result']['detailedDescription']['articleBody'])
#             return (data['itemListElement'][0]['result']['description'] + '.' +
#                     data['itemListElement'][0]['result']['detailedDescription']['articleBody'])
#
#         # return(data)
#         # request = urllib.request.Request(url,headers=header)
#         # response = urllib.request.urlopen(request,timeout=20).read()
#         # response = json.load(response)
#         # print(response)
#         # return response
#
#  #_________________________________________
#         # #https://kgsearch.googleapis.com/v1/entities:search?query=taylor+swift&key=API_KEY&limit=1&indent=True
#         # url = service_url + '?' + urllib.parse.urlencode(params)
#         # #request = urllib.request(url, headers = headers)
#         # #response=urllib.urlopen(url).read()
#         # response = json.loads(urllib.request.urlopen(url).read())
#         # print (response)
#         # return response
# #_________________________________________________
#
#         #for element in response['itemListElement']:
#             #print (element['result']['name'] + ' (' + str(element['resultScore']) + ')')
#             #print (element['result'] + ' (' + str(element['resultScore']) + ')')
# #________________________________________________
# ##test
# if __name__ == "__main__":
#     poi=list()
#     with open("E:\\KGPOI\\poi_index.txt") as adictfile:
#         for line in adictfile:
#             toks = line.strip()
#             poi.append(toks)
#     #print (poi.values())
#     print ("____________________________________")
#     #print(poi)
#     fileObject = open('E:\\KGPOI\\new_poi_des.txt', 'a+',encoding='utf-8')
#     gkg = GooleKGAPI()
#     for i in range(len(poi)):
#         s = poi[i].split(',')
#         poi_id = s[0]
#         poi_name = s[1]
#         dictpoi = gkg.getResult(poi_name)
#         fileObject.write(poi_id+'\t'+poi_name+ '\t' + str(dictpoi) + '\n')
#     #dictpoi=dict()
#     # example => bill gates
#     # for i in range(len(poi)):
#     #     dictpoi=gkg.getResult(poi[i])
#     #     fileObject.write(str(dictpoi)+'\n')
#
#     #fileObject.close()