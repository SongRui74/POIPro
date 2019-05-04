#!/usr/bin/python3
import pymysql


def connect():
    # 打开数据库连接
    db = pymysql.connect("localhost", "root", "123456", "test")
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    # 使用 execute()  方法执行 SQL 查询
    cursor.execute("SELECT VERSION()")
    # 使用 fetchone() 方法获取单条数据.
    data = cursor.fetchone()
    print("Database version : %s " % data)
    # 关闭数据库连接
    db.close()


def insert():
    # 打开数据库连接
    db = pymysql.connect("localhost", "root", "123456", "test")
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 插入语句
    sql = """INSERT INTO TEST(id, name, num)
         VALUES (3, 'C', 3)"""
    try:
        # 执行sql语句
        cursor.execute(sql)
        # 提交到数据库执行
        db.commit()
    except:
        # 如果发生错误则回滚
        db.rollback()

    # 关闭数据库连接
    db.close()

def detail():
    # 打开数据库连接
    db = pymysql.connect("localhost", "root", "123456", "test")
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # SQL 查询语句
    sql = "SELECT * FROM test"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        for row in results:
            id = row[0]
            name = row[1]
            num = row[2]
            # 打印结果
            print("id=%s,name=%s,num=%s" % \
                  (id, name, num))
    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()

detail()
