import json
import pymysql

# 读取review数据，并写入数据库
# 导入数据库成功，总共6685900条记录
def prem_re(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS review")  # 习惯性
    sql = """CREATE TABLE review (
             review_id  VARCHAR(100),
             user_id  VARCHAR(100),
             business_id VARCHAR(200),
             stars INT,
             text VARCHAR(10000) NOT NULL,
             useful INT,
             funny INT,
             cool INT)"""
    cursor.execute(sql)  # 根据需要创建一个表格


def reviewdata_insert(db):

    with open('E:/dataset/yelp_dataset/review.json', encoding='utf-8') as f:
        i = 0
        while True:
            i += 1
            print(u'正在载入第%s行......' % i)
            try:
                lines = f.readline()  # 使用逐行读取的方法
                review_text = json.loads(lines)  # 解析每一行数据
                result = []
                result.append((review_text['review_id'], review_text['user_id'],
                               review_text['business_id'],review_text['stars'], review_text['text'],
                               review_text['useful'],review_text['funny'], review_text['cool']))
                print(result)

                insert_re = "insert into review(review_id, user_id, business_id, stars, text, useful," \
                            "funny, cool) values (%s, %s, %s, %s,%s, %s,%s, %s)"
                cursor = db.cursor()
                cursor.executemany(insert_re, result)
                db.commit()
            except Exception as e:
                db.rollback()
                print(str(e))
                break

# 读取checkin数据，并写入数据库
# 导入数据库成功，总共6条记录
def prem_ch(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS checkin")  # 习惯性
    sql = """CREATE TABLE checkin (
             business_id VARCHAR(200),
             date VARCHAR(10000))"""
    cursor.execute(sql)  # 根据需要创建一个表格


def checkindata_insert(db):

    with open('E:/dataset/yelp_dataset/checkin.json', encoding='utf-8') as f:
        i = 0
        while True:
            i += 1
            print(u'正在载入第%s行......' % i)
            try:
                lines = f.readline()  # 使用逐行读取的方法
                review_text = json.loads(lines)  # 解析每一行数据
                result = []
                result.append((review_text['business_id'], review_text['date']))
                print(result)

                insert_re = "insert into checkin(business_id, date) values (%s, %s)"
                cursor = db.cursor()
                cursor.executemany(insert_re, result)
                db.commit()
            except Exception as e:
                db.rollback()
                print(str(e))
                break

# 读取tip数据，并写入数据库
# 导入数据库成功，总共1223094条记录
def prem_tip(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS tip")  # 习惯性
    sql = """CREATE TABLE tip (
             text VARCHAR(10000) NOT NULL,
             date VARCHAR(100),
             compliment_count INT,
             business_id VARCHAR(200),
             user_id  VARCHAR(100))"""
    cursor.execute(sql)  # 根据需要创建一个表格


def tipdata_insert(db):

    with open('E:/dataset/yelp_dataset/tip.json', encoding='utf-8') as f:
        i = 0
        while True:
            i += 1
            print(u'正在载入第%s行......' % i)
            try:
                lines = f.readline()  # 使用逐行读取的方法
                review_text = json.loads(lines)  # 解析每一行数据
                result = []
                result.append((review_text['text'], review_text['date'],
                               review_text['compliment_count'],review_text['business_id'], review_text['user_id']))
                print(result)

                insert_re = "insert into tip(text, date, compliment_count, business_id, " \
                            "user_id) values (%s, %s, %s, %s,%s)"
                cursor = db.cursor()
                cursor.executemany(insert_re, result)
                db.commit()
            except Exception as e:
                db.rollback()
                print(str(e))
                break

# 读取user数据，并写入数据库
# 导入数据库成功，总共3209条记录
def prem_user(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS user")  # 习惯性
    sql = """CREATE TABLE user (
             user_id  VARCHAR(100),
             name  VARCHAR(100),
             review_count INT,
             yelping_since VARCHAR(100),
             friends VARCHAR(100), 
             useful INT,
             funny INT,
             cool INT,
             fans INT,
             average_stars FLOAT)"""
    cursor.execute(sql)  # 根据需要创建一个表格


def userdata_insert(db):

    with open('E:/dataset/yelp_dataset/user.json', encoding='utf-8') as f:
        i = 0
        while True:
            i += 1
            print(u'正在载入第%s行......' % i)
            try:
                lines = f.readline()  # 使用逐行读取的方法
                review_text = json.loads(lines.replace("'", '"'))  # 解析每一行数据
                fri = review_text['friends']
                lst = fri.split(",")

                k = len(lst)
                j = 0
                while j < k:
                    result = []
                    result.append((review_text['user_id'], review_text['name'],
                                review_text['review_count'], review_text['yelping_since'],
                                lst[j], review_text['useful'], review_text['funny'],
                                review_text['cool'], review_text['fans'], review_text['average_stars']))
                    print(result)

                    insert_re = "insert into user(user_id,name,review_count,yelping_since," \
                                "friends,useful,funny,cool,fans,average_stars) " \
                                "values (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s)"

                    cursor = db.cursor()
                    cursor.executemany(insert_re, result)
                    db.commit()

                    j = j+1

            except Exception as e:
                db.rollback()
                print(str(e))
                break

# 读取business数据，并写入数据库
# 导入数据库成功，总共192609条记录
def prem_business(db):
    cursor = db.cursor()
    cursor.execute("SELECT VERSION()")
    data = cursor.fetchone()
    print("Database version : %s " % data)  # 结果表明已经连接成功
    cursor.execute("DROP TABLE IF EXISTS business")  # 习惯性
    sql = """CREATE TABLE business (
             business_id  VARCHAR(100),
             name  VARCHAR(100),
             address VARCHAR(1000),
             city VARCHAR(100),
             state VARCHAR(100),
             latitude FlOAT,
             longitude FLOAT,
             stars FLOAT,
             review_count INT,
             is_open INT,
             categories VARCHAR(100))"""
    cursor.execute(sql)  # 根据需要创建一个表格


def businessdata_insert(db):

    with open('E:/dataset/yelp_dataset/business.json', encoding='utf-8') as f:
        i = 0
        while True:
            i += 1
            print(u'正在载入第%s行......' % i)
            try:
                lines = f.readline()  # 使用逐行读取的方法
                review_text = json.loads(lines)  # 解析每一行数据
                cate = review_text['categories']
                if cate is None: #若类别信息为空
                    cate = "null"
                    result = []
                    result.append((review_text['business_id'], review_text['name'],
                                   review_text['address'], review_text['city'], review_text['state'],
                                   review_text['latitude'], review_text['longitude'],
                                   review_text['stars'], review_text['review_count'],
                                   review_text['is_open'], cate))

                    print(result)

                    insert_re = "insert into business(business_id,name,address,city," \
                                "state,latitude,longitude,stars,review_count,is_open,categories) " \
                                "values (%s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s)"

                    cursor = db.cursor()
                    cursor.executemany(insert_re, result)
                    db.commit()

                elif cate.find(","): #含多个类别
                    lst = cate.split(",")
                    j = 0
                    while j < len(lst):
                        result = []
                        result.append((review_text['business_id'], review_text['name'],
                                   review_text['address'], review_text['city'], review_text['state'],
                                   review_text['latitude'], review_text['longitude'],
                                   review_text['stars'], review_text['review_count'],
                                   review_text['is_open'], lst[j]))

                        print(result)

                        insert_re = "insert into business(business_id,name,address,city," \
                                    "state,latitude,longitude,stars,review_count,is_open,categories) " \
                                    "values (%s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s)"

                        cursor = db.cursor()
                        cursor.executemany(insert_re, result)
                        db.commit()

                        j = j + 1
                else:
                    result = []
                    result.append((review_text['business_id'], review_text['name'],
                                   review_text['address'], review_text['city'], review_text['state'],
                                   review_text['latitude'], review_text['longitude'],
                                   review_text['stars'], review_text['review_count'],
                                   review_text['is_open'], cate[0]))

                    print(result)

                    insert_re = "insert into business(business_id,name,address,city," \
                                "state,latitude,longitude,stars,review_count,is_open,categories) " \
                                "values (%s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s)"

                    cursor = db.cursor()
                    cursor.executemany(insert_re, result)
                    db.commit()

            except Exception as e:
                db.rollback()
                print(str(e))
                break

# 起到一个初始化或者调用函数的作用
if __name__ == "__main__":
    db = pymysql.connect("localhost", "root", "123456", "test", charset='utf8')
    cursor = db.cursor()
    prem_tip(db)
    tipdata_insert(db)
    cursor.close()