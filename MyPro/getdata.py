import json
import pymysql
import requests

#获取每个poi的评分情况
def nyc_rating(db):
    # cursor = db.cursor()
    # sql = "SELECT dataset_tsmc2014_nyc.VenueID FROM dataset_tsmc2014_nyc"
    # try:
    #     cursor.execute(sql)
    #     # 获取POI_id
    #     results = cursor.fetchall()
    #     i = 1
    #     for row in results:
    #         poi_id = row[0]
    #         #爬取数据
    #         print(i)
            poi_id = '4b5b981bf964a520900929e3'
            url = 'https://api.foursquare.com/v2/venues/'+poi_id
            params = dict(
                client_id='VDQ3JEP1TGWYDLGFXHJ4CLL3EGRAVEPW4FJQQB1TMQZGE4TK',
                client_secret='IHOWVKBEINA4GGZCIF4UUQM2101U100OQKHJWVWIJ2YYXEIZ',
                v='20190427',
                sort='popular'
            )
            resp = requests.get(url=url, params=params)
            data = json.loads(resp.text)
            print(data)
            if (data['meta']['code'] == 200):
                if (data.__contains__('rating')):
                    # get values
                    with open('nycrating.txt', 'a+', encoding='utf-8') as fp:
                        fp.write(poi_id + '\t')
                        fp.write(data['response']['venue']['rating'])
                        fp.write('\n')
    #         i = i + 1
    #
    # except Exception as e:
    #     db.rollback()
    #     print(str(e))

#获取每个poi的一个最受欢迎的tip
def nyc_tip(db):
    cursor = db.cursor()
    sql = "SELECT dataset_tsmc2014_nyc.VenueID FROM dataset_tsmc2014_nyc"
    try:
        cursor.execute(sql)
        # 获取POI_id
        results = cursor.fetchall()
        i = 1
        for row in results:
            poi_id = row[0]
            #爬取数据
            print(i)
            url = 'https://api.foursquare.com/v2/venues/'+poi_id+'/tips'
            params = dict(
                client_id='VDQ3JEP1TGWYDLGFXHJ4CLL3EGRAVEPW4FJQQB1TMQZGE4TK',
                client_secret='IHOWVKBEINA4GGZCIF4UUQM2101U100OQKHJWVWIJ2YYXEIZ',
                v='20190427',
                sort='popular'
            )
            resp = requests.get(url=url, params=params)
            data = json.loads(resp.text)
            print(data)
            if (data['meta']['code'] == 200):
                if (data['response']['tips']['items'] == []):
                    continue
                else:
                    # get values
                    with open('nyctip.txt', 'a+', encoding='utf-8') as fp:
                        fp.write(poi_id + '\t')
                        fp.write(str(data['response']['tips']['items'][0]['text']))
                        fp.write('\n')
            i = i + 1

    except Exception as e:
        db.rollback()
        print(str(e))

if __name__ == "__main__":
    db = pymysql.connect("localhost", "root", "123456", "poi", charset='utf8')
    cursor = db.cursor()
    nyc_rating(db)
    # nyc_tip(db)
    cursor.close()