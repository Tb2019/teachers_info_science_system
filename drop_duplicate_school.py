import pandas as pd
import pymysql
from itertools import chain
from loguru import logger
from utils import df2mysql, local_engine, sf_engine

# # 为update_rel_table函数创建全局变量
# school_id = None  # 去重目标院校
# table = 'search_teacher'  # 需要去重的数据所在的表名
# rel_table_name = 'search_college_teacher_rel'  # 学院-教师表名
# # 数据库连接信息(要求教师信息表和学院-教师表共用一个连接，即两张表在一个库中)
# host = '192.168.2.12'
# user = 'root'
# password = 'Shufang_@919'
# db = 'alpha_search'
# sql_conn = pymysql.connect(host=host, user=user, password=password, db=db, port=3306, charset='utf8')

def from_mysql_to_df(school_id, engine):
    # conn = pymysql.connect(host='localhost', user='root', password='123456', db='alpha_search', port=3306, charset='utf8')
    sql = f"""select * from {table} where school_id = {school_id}"""
    data = pd.read_sql(sql=sql, con=engine)
    return data


def get_split(element):
    try:
        element = element.split(',')
        return element
    except:
        return [element]


def del_from_mysql(table, id_del):
    values = ','.join(['%s'] * len(id_del))
    # conn = pymysql.connect(host='localhost', user='root', password='123456', db='alpha_search', port=3306,
                           # charset='utf8')
    cursor = sql_conn.cursor()
    sql = 'delete from {table} where id in ({values})'.format(table=table, values=values)
    # print(sql)
    try:
        cursor.execute(sql, id_del)
        sql_conn.commit()
    except:
        sql_conn.rollback()


def get_most_info(df):
    global search_college_teacher_rel
    search_college_teacher_rel_temp = pd.DataFrame()
    if len(df) == 1:
        search_college_teacher_rel_temp['college_id'] = df['college_id']
        search_college_teacher_rel_temp['teacher_id'] = df['id']
        search_college_teacher_rel = pd.concat([search_college_teacher_rel, search_college_teacher_rel_temp], axis=0)
        return None

    df['info_num'] = df.notnull().sum(axis=1)
    df.sort_values(by='info_num', inplace=True, ascending=False)
    df.drop('info_num', axis=1, inplace=True)

    # 从教师表删除重复记录
    id_del = tuple(df.id.values)[1:]
    del_from_mysql(table, id_del)

    # 保留删除的id信息
    search_college_teacher_rel_temp['college_id'] = df['college_id']
    search_college_teacher_rel_temp['teacher_id'] = df.iloc[0]['id']
    search_college_teacher_rel = pd.concat([search_college_teacher_rel, search_college_teacher_rel_temp], axis=0)

    return None


def drop_duplicates(df):
    global search_college_teacher_rel
    search_college_teacher_rel_temp = pd.DataFrame()
    if len(df) == 1:
        search_college_teacher_rel_temp['college_id'] = df['college_id']
        search_college_teacher_rel_temp['teacher_id'] = df['id']
        search_college_teacher_rel = pd.concat([search_college_teacher_rel, search_college_teacher_rel_temp], axis=0)
        return None

    df['duplicate'] = None
    get_split_info = df[['phone', 'email']].applymap(get_split).values.tolist()
    info_set_list = [*map(lambda x: set(chain.from_iterable(x)), get_split_info)]
    #     info_set_list = df[['phone', 'email']].applymap(get_set).values.tolist()
    # print(info_set_list)
    for i in range(len(df)):
        if df.iloc[i].duplicate == None:
            for j in range(i + 1, len(df)):
                if info_set_list[i] & info_set_list[j]:
                    df.duplicate.iloc[i] = i
                    df.duplicate.iloc[j] = i
                else:
                    df.duplicate.iloc[i] = i
        if i == len(df) - 1:
            break

    #         df.drop_duplicates('duplicate', inplace=True)  # 简单处理，保留第一条记录，不一定信息量最多
    df = df.groupby('duplicate', as_index=False, group_keys=False, dropna=False).apply(get_most_info)
    #     df.drop('duplicate', axis=1, inplace=True)

    return None


# def truncate_this_school_in_rel(school_id):
#     sql = f'''delete from search_college_teacher_rel where school_id={school_id}'''
#     cursor = sql_conn.cursor()
#     try:
#         cursor.execute(sql)
#         sql_conn.commit()
#     except:
#         sql_conn.rollback()


# def update_rel_table(schoolid):
#     global school_id
#     school_id = schoolid
#     # 由于一个学校的数据是分阶段完成的，所以学校数据不会一次就导入完整。所以每导入一个学院就进行一次去重操作，产生rel表的数据，所以需要先删除rel表中已经存在的该学校的数据
#     # 因此该去重逻辑需要在每个学院完成后运行一次，写为函数
#     try:
#         logger.info(f'尝试清空rel表中school_id={school_id}的数据，并保存新数据...')
#         truncate_this_school_in_rel(school_id)
#         logger.info('清空并保存完成')
#     except:
#         logger.info('这是该学校的第一个学院数据，无旧数据需要清空')
#
#     search_college_teacher_rel = pd.DataFrame()  # 生成rel表的数据
#
#     data = from_mysql_to_df(school_id=school_id, engine=sf_engine)
#     data.groupby('name', as_index=False, group_keys=False, dropna=False).apply(drop_duplicates)
#
#     df2mysql(engine=sf_engine, df=search_college_teacher_rel, table_name=rel_table_name)
#
#     sql_conn.close()


if __name__ == '__main__':
    # school_id = None  # 去重目标院校
    # table = None  # 需要去重的数据所在的表名
    # rel_table_name = None  # 学院-教师表名
    #
    # # 数据库连接信息(要求教师信息表和学院-教师表共用一个连接，即两张表在一个库中)
    # host = None
    # user = None
    # password = ''
    # db = None
    # sql_conn = pymysql.connect(host=host, user=user, password=password, db=db, port=3306, charset='utf8')
    #
    # search_college_teacher_rel = pd.DataFrame()  # 生成rel表的数据
    #
    # data = from_mysql_to_df(school_id=school_id)
    # data.groupby('name', as_index=False, group_keys=False, dropna=False).apply(drop_duplicates)
    #
    # df2mysql(engine=local_engine, df=search_college_teacher_rel, table_name=rel_table_name)
    school_id = 105  # 去重目标院校
    table = 'search_teacher_update'  # 需要去重的数据所在的表名
    rel_table_name = 'search_college_teacher_rel_update'  # 学院-教师表名

    # 数据库连接信息(要求教师信息表和学院-教师表共用一个连接，即两张表在一个库中)
    host = '192.168.2.12'
    user = 'root'
    password = 'Shufang_@919'
    db = 'alpha_search'
    sql_conn = pymysql.connect(host=host, user=user, password=password, db=db, port=3306, charset='utf8')

    # # 由于一个学校的数据是分阶段完成的，所以学校数据不会一次就导入完整。所以每导入一个学院就进行一次去重操作，产生rel表的数据，所以需要先删除rel表中已经存在的该学校的数据
    # # 因此该去重逻辑需要在每个学院完成后运行一次，写为函数
    # try:
    #     logger.info(f'尝试清空rel表中school_id={school_id}的数据，并保存新数据...')
    #     truncate_this_school_in_rel(school_id)
    #     logger.info('清空并保存完成')
    # except:
    #     logger.info('这是该学校的第一个学院数据，无旧数据需要清空')

    search_college_teacher_rel = pd.DataFrame()  # 生成rel表的数据

    data = from_mysql_to_df(school_id=school_id, engine=sf_engine)
    data.groupby('name', as_index=False, group_keys=False, dropna=False).apply(drop_duplicates)

    df2mysql(engine=sf_engine, df=search_college_teacher_rel, table_name=rel_table_name)

    sql_conn.close()
