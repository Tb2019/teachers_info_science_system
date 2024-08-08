import pymysql


def get_totle_data(database: str, table: str, data_kind: str):
    conn = pymysql.connect(host=host, port=port, password=password, user=user, database=database, charset='utf8')
    sql = 'select count(*) from {table}'.format(table=table)
    cursor = conn.cursor()
    cursor.execute(sql)
    num = cursor.fetchone()[0]
    print(f'共有{data_kind}数据{num}条')
    conn.close()
    return num

if __name__ == '__main__':
    host = '192.168.2.12'
    port = 3306
    user = 'root'
    password = 'Shufang_@919'

    # num_teacher_old = get_totle_data(database='alpha_search', table='search_teacher', data_kind='教师_YGQ')
    # num_teacher_old = 12776
    num_teacher_new = get_totle_data(database='alpha_search', table='search_teacher', data_kind='教师')
    num_equipment = get_totle_data(database='alpha_search', table='search_instrument_souke', data_kind='设备_souke')
    num_equipment_2 = get_totle_data(database='alpha_search', table='search_instrument_info', data_kind='设备_info')
    totle = num_teacher_new + num_equipment + num_equipment_2
    print(f'总数据{totle}条')