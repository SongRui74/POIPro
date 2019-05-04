import json
import requests
import pymysql

def create_poi_detail(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS poi_detail")  # 习惯性
    sql = """CREATE TABLE poi_detail (
             id  VARCHAR(100),
             name  VARCHAR(100),
             lat FLOAT,
             lng FLOAT,
             city  VARCHAR(255),
             state  VARCHAR(255),
             country  VARCHAR(255),
             categories  VARCHAR(255),
             rating FLOAT)"""
    cursor.execute(sql)  # 根据需要创建一个表格


#poi_detail: id，name，lat, lng, city, state, country,categories(name), rating
def POI_detail(db):
    cursor = db.cursor()
    sql = "select check_in.VenueId from check_in"
    try:
        cursor.execute(sql)
        # 获取POI_id
        results = cursor.fetchall()
        num = 0
        for row in results:
            poi_id = row[0]
            #爬取数据
            url = 'https://api.foursquare.com/v2/venues/' + poi_id
            params = dict(
                client_id='VDQ3JEP1TGWYDLGFXHJ4CLL3EGRAVEPW4FJQQB1TMQZGE4TK',
                client_secret='IHOWVKBEINA4GGZCIF4UUQM2101U100OQKHJWVWIJ2YYXEIZ',
                v='20190427'
            )
            resp = requests.get(url=url, params=params)
            data = json.loads(resp.text)
            print(data)
            # get values
            result = []
            poi_categories = ""
            # 判断是否含有类别信息
            if "categories" in data:
                count = len(data['response']['venue']['categories'])
                print(count)
                for i in count:
                    poi_categories = poi_categories + data['response']['venue']['categories'][i]['name'] + ","

            # 判断是否含有rating值
            if "rating" in data:
                result.append((data['response']['venue']['id'], data['response']['venue']['name'],
                               data['response']['venue']['location']['lat'],
                               data['response']['venue']['location']['lng'],
                               data['response']['venue']['location']['city'],
                               data['response']['venue']['location']['state'],
                               data['response']['venue']['location']['country'],poi_categories,
                               data['response']['venue']['rating']))
            else:
                result.append((data['response']['venue']['id'], data['response']['venue']['name'],
                               data['response']['venue']['location']['lat'],
                               data['response']['venue']['location']['lng'],
                               data['response']['venue']['location']['city'],
                               data['response']['venue']['location']['state'],
                               data['response']['venue']['location']['country'], poi_categories,-1.0))
            print(result)
            sql_detail = "insert into poi_detail(id,name,lat,lng,city,state,country,categories,rating)" \
                        "value(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            num = num + 1
            print(num)
            cursor = db.cursor()
            cursor.executemany(sql_detail, result)
            db.commit()

    except Exception as e:
        db.rollback()
        print(str(e))

#5.4  写入50条
# if __name__ == "__main__":
#      db = pymysql.connect("localhost", "root", "123456", "poi", charset='utf8')
#      cursor = db.cursor()
#      create_poi_detail(db)
#      POI_detail(db)
#      cursor.close()

def create_poi_tip(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS poi_tip")  # 习惯性
    sql = """CREATE TABLE poi_tip (
             id  VARCHAR(100),
             tip1 TEXT,
             tip2 TEXT)"""
    cursor.execute(sql)  # 根据需要创建一个表格

def POI_tip(db):
    cursor = db.cursor()
    sql = "select check_in.VenueId from check_in"
    try:
        cursor.execute(sql)
        # 获取POI_id
        results = cursor.fetchall()
        for row in results:
            poi_id = row[0]
            #爬取数据
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
                # get values
                result = []
                if "items" in data:
                    tipa = data['response']['tips']['items'][0]['text']
                    tipb = data['response']['tips']['items'][1]['text']
                    tipa = tipa.replace("'","\\\'")
                    tipa = tipa.replace('"','\\\"')
                    tipb = tipb.replace("'","\\\'")
                    tipb = tipb.replace('"','\\\"')
                    result.append((poi_id,tipa,tipb))
                    print(result)
                    sql_detail = "insert into poi_tip(id,tip1,tip2)value(%s, %s, %s)"

                    cursor = db.cursor()
                    cursor.executemany(sql_detail, result)
                    db.commit()


    except Exception as e:
        db.rollback()
        print(str(e))

if __name__ == "__main__":
      db = pymysql.connect("localhost", "root", "123456", "poi", charset='utf8')
      cursor = db.cursor()
      create_poi_tip(db)
      POI_tip(db)
      cursor.close()

#poi_similar: poi_id, count, similar_id
def create_poi_similar(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS poi_similar")  # 习惯性
    sql = """CREATE TABLE poi_similar(
             id  VARCHAR(100),
             count INT,
             similar_id VARCHAR(100))"""
    cursor.execute(sql)  # 根据需要创建一个表格

def POI_similar(db):
    cursor = db.cursor()
    sql = "select check_in.VenueId from check_in"
    try:
        cursor.execute(sql)
        # 获取POI_id
        results = cursor.fetchall()
        num = 1
        for row in results:
            if(num <= 1537):
                num = num + 1
            else:
                poi_id = row[0]
                #爬取数据
                url = 'https://api.foursquare.com/v2/venues/'+poi_id+'/similar'
                params = dict(
                    client_id='VDQ3JEP1TGWYDLGFXHJ4CLL3EGRAVEPW4FJQQB1TMQZGE4TK',
                    client_secret='IHOWVKBEINA4GGZCIF4UUQM2101U100OQKHJWVWIJ2YYXEIZ',
                    v='20190427'
                )
                resp = requests.get(url=url, params=params)
                data = json.loads(resp.text)
                print(data)
                if(data['meta']['code'] == 200):
                    # get values
                    result = []
                    count = data['response']['similarVenues']['count']
                    for i in range(count):
                        result.append((poi_id,count,data['response']['similarVenues']['items'][i]['id']))
                        print(result)
                        sql_detail = "insert into poi_similar(id,count,similar_id)value(%s, %s, %s)"
                        print(num)
                        num = num + 1
                        cursor = db.cursor()
                        cursor.executemany(sql_detail, result)
                        db.commit()

    except Exception as e:
        db.rollback()
        print(str(e))

# if __name__ == "__main__":
#       db = pymysql.connect("localhost", "root", "123456", "poi", charset='utf8')
#       cursor = db.cursor()
#       create_poi_similar(db)
#       POI_similar(db)
#       cursor.close()

#poi_next: poi_id, next_id, distance 返回数量不超过5个
def create_poi_next(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS poi_next")  # 习惯性
    sql = """CREATE TABLE poi_next(
             id  VARCHAR(100),
             next_id VARCHAR(100),
             distance FLOAT)"""
    cursor.execute(sql)  # 根据需要创建一个表格

def POI_next(db):
    cursor = db.cursor()
    sql = "select check_in.VenueId from check_in"
    try:
        cursor.execute(sql)
        # 获取POI_id
        results = cursor.fetchall()
        for row in results:
            poi_id = row[0]
            #爬取数据
            url = 'https://api.foursquare.com/v2/venues/'+poi_id+'/nextvenues'
            params = dict(
                client_id='VDQ3JEP1TGWYDLGFXHJ4CLL3EGRAVEPW4FJQQB1TMQZGE4TK',
                client_secret='IHOWVKBEINA4GGZCIF4UUQM2101U100OQKHJWVWIJ2YYXEIZ',
                v='20190427'
            )
            resp = requests.get(url=url, params=params)
            data = json.loads(resp.text)
            print(data)
            if (data['meta']['code'] == 200):
                # get values
                result = []
                count = data['response']['nextVenues']['count']
                for i in range(count):
                    result.append((poi_id,data['response']['nextVenues']['items'][i]['id'],data['response']['nextVenues']['items'][i]['location']['distance']))
                    print(result)
                    sql_detail = "insert into poi_next(id,next_id,distance)value(%s, %s, %s)"

                    cursor = db.cursor()
                    cursor.executemany(sql_detail, result)
                    db.commit()

    except Exception as e:
        db.rollback()
        print(str(e))

if __name__ == "__main__":
      db = pymysql.connect("localhost", "root", "123456", "poi", charset='utf8')
      cursor = db.cursor()
      create_poi_next(db)
      POI_next(db)
      cursor.close()

#poi_near: poi_id, near_id, distance  要设置参数near，返回结果数量，距离半径
def create_poi_near(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS poi_near")  # 习惯性
    sql = """CREATE TABLE poi_near(
             id  VARCHAR(100),
             near_id VARCHAR(100),
             distance FLOAT)"""
    cursor.execute(sql)  # 根据需要创建一个表格


#poi_tip: poi_id, tip_id,text
def create_poi_tip(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS poi_tip")  # 习惯性
    sql = """CREATE TABLE poi_tip(
             id  VARCHAR(100),
             tip_id VARCHAR(100),
             text TEXT)"""
    cursor.execute(sql)  # 根据需要创建一个表格