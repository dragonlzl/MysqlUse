# -*- coding:utf-8 -*-
#!/usr/bin/python3

import pymysql

#获取某个字段的数据列表
def get_db_date(num = 0):
    '''
    获取某个字段的数据列表
    :param num: 字段号码，0-34
    :return: 字段数据list
    '''
    id_list = []
    # 打开数据库连接
    db = pymysql.connect(host="xxxx",user="xxxx",  password="xxxxx",db="xxxxxx",port=3306)

    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()

    #关键字冲突要加``

    sql = "SELECT * FROM xxxxxx WHERE `xxxxx` > 0 and `type` = 5 order by xxxxxxx desc"
    try:
        # 使用 execute()  方法执行 SQL 查询
        cursor.execute(sql)
        results = cursor.fetchall()
        if 0 <= num < len(results[0]):
            for row in results:
                id_list.append(row[num])
            #print(len(id_list))
        else:
            print("num out of the list")

    except Exception as e:
        raise e
    finally:
        db.close()
    return id_list


def update_png(times = 0,png = 'xxxxxx'):

    '''
    批量换图片
    :param times: 需要该多少个数据
    :param png: 图片url
    :return:
    '''

    id_list = get_db_date()
    png_list = get_db_date(9)

    db = pymysql.connect(host="xxxxxxx", user="xxxx",
                         password="xxxxx", db="xxxxxx", port=3306)

    # 使用 cursor() 方法创建一个游标对象 cursor

    try:
        if 0 <= times < len(id_list):
            cursor = db.cursor()

            for i in range(times):

                if png_list[i] != png:

                    sql = "UPDATE xxxxx SET preview = '%s' WHERE id = %d" % (png,id_list[i])

                    # 使用 execute()  方法执行 SQL 更新
                    cursor.execute(sql)
                    #提交
                    db.commit()
                    print("id =",id_list[i],"change success")

                else:
                    print("id =",id_list[i],"no change")
            print("......Finsh......")
        else:
            print("times out of the list")
    except Exception as e:
        db.rollback()  #错误回滚
        raise e
    finally:
        db.close()

def get_work_id():

    '''

    :return: 该表的work_id字段list
    '''

    works_list=[]
    db = pymysql.connect(host="xxxxxx", user="xxxx",
                         password="xxxxx", db="xxxxx", port=3306)

    cursor = db.cursor()

    sql = "SELECT xxxxx FROM xxxxxxxxx"
    try:
        # 使用 execute()  方法执行 SQL 查询
        cursor.execute(sql)
        results = cursor.fetchall()
        for row in results:
            works_list.append(row[0])

    except Exception as e:
        raise e
    finally:
        db.close()
    return works_list

def insert_date(times=0):

    '''
    mysql插入
    :param times: 插入的数据数
    :return:
    '''

    works_list = get_work_id()
    id_list = get_db_date()

    db = pymysql.connect(host="xxxxx", user="xxxxx",
                         password="xxxxxx", db="xxxxxx", port=3306)

    try:
        if 0 <= times < len(works_list):
            cursor = db.cursor()

            for i in range(times):

                if id_list[i] not in works_list:

                    sql = "INSERT INTO table_name(work_id,sort,status,recommended_time) VALUE('%d',NULL,1,NULL)" % id_list[i]

                    # 使用 execute()  方法执行 SQL 插入
                    cursor.execute(sql)
                    # 提交
                    db.commit()
                    print("id =", id_list[i], "insert success")

                else:
                    print("id =", id_list[i], "has already exist")
            print("......Finsh......")
        else:
            print("times out of the list")
    except Exception as e:
        db.rollback()  # 错误回滚
        raise e
    finally:
        db.close()


if __name__ == '__main__':
    #insert_date(50)
    #update_png(50)
    pass
