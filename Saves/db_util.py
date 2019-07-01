# coding=UTF-8
import pymysql
from collections import OrderedDict
from Luo_Wang.Utils import logutils
from Luo_Wang.Saves import Mysql_settings


def con2db_mysql(host, db_name, user, pass_wd, port=3306, charset='utf8'):
    while True:
        try:
            logutils.info('Connecting to the database: %s >>' % db_name)
            con = pymysql.connect(
                host=host
                , db=db_name
                , user=user
                , passwd=pass_wd
                , port=port
                , charset=charset)
            cur = con.cursor()
            cur.execute('SET NAMES utf8mb4')
            cur.execute('SET CHARACTER SET utf8mb4')
            cur.execute('SET character_set_connection=utf8mb4')
            logutils.info('Success')
            return con, cur
        except :
            logutils.error('Failure, Trying to reconnect>>')


def con2db(db_name):

    host = Mysql_settings.host
    user = Mysql_settings.user
    pass_wd = Mysql_settings.passwd
    connection = con2db_mysql(host, db_name, user, pass_wd)
    return connection


def	primary_column_name(db_con,db_cur,_db_name, _table_name):

	sql = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (_db_name, _table_name)
	db_cur.execute(sql)
	res_temp = db_cur.fetchall()

	column_list = list()
	for each in res_temp:

		column_list.append(each[0])
	return column_list


def	all_column_name(db_con,db_cur,_db_name, _table_name):
	sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (
	_db_name, _table_name)
	db_cur.execute(sql)
	res_temp = db_cur.fetchall()
	column_list = list()
	for each in res_temp:
		column_list.append(each[0])
	return column_list





'''
def setting(sys_type, db_type, module_name, version_code):
    con, cur = db_con.con2db('MySQL', "setting")
    sql = "SELECT * FROM setting.crawler_collection_setting WHERE " \
          "sys_type = '%s' and " \
          "database_type = '%s' and " \
          "module_name = '%s' and " \
          "version_code = '%s'" % (sys_type, db_type, module_name, version_code)
    setting_info = query(con, cur, sql)
    return setting_info


def query(con, cur, sql): 
    cur.execute(sql)
    index = cur.description
    result = []
    for res in cur.fetchall():
        row = OrderedDict()
        for i in range(len(index)):
            row[index[i][0]] = res[i]
        result.append(row)
    con.close()
    return result


def get_cookie(db_name, table_name):
    group_info = group.info("MySQL", "moka", "onekeyload")
    host = group_info.get("host")
    user = 'caolm'
    pass_wd = 'caolm123456'
    con, cur = db_con.con2db_mysql(host, db_name, user, pass_wd)
    sql = "SELECT * FROM onekeyload.rb_account_cookie"
    exist_list = get_exist_list(db_name, table_name)

    cookie_list = []
    try:
        sql = "SELECT token,Cookie,original_username FROM wechat_token_cookie"
        cur.execute(sql)
        res = cur.fetchall()
        for line in res:
            if line[2] not in exist_list:
                cookie_list.append(line)
        con.close()
        return cookie_list
    except :
        logutil.error('Failure')
        con.close()
        return


def sql_res(db_name, sql):
    connection = db_con.con2db('MySQL', db_name)
    con = connection[0]
    cur = connection[1]
    _res = query(con, cur, sql)
    return _res


def username_list():
    connection = db_con.con2db_silence('MySQL', "mp_weixin")
    con = connection[0]
    cur = connection[1]
    sql = "SELECT user_name FROM mp_weixin.mp_user"
    cur.execute(sql)
    res_temp = cur.fetchall()
    column_list = list()
    for each in res_temp:
        column_list.append(each[0])
    con.close()
    return column_list


def password(user_name):
    connection = db_con.con2db_silence('MySQL', "mp_weixin")
    con = connection[0]
    cur = connection[1]
    sql = "SELECT password_md5 FROM mp_weixin.mp_user WHERE user_name = '%s'" % user_name
    cur.execute(sql)
    res_temp = cur.fetchall()
    con.close()
    if res_temp is not None:
        return res_temp[0][0]
    else:
        return


def all_column_name(_db_name, _table_name):
    connection = db_con.con2db('MySQL', _db_name)
    con = connection[0]
    cur = connection[1]
    sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (_db_name, _table_name)
    cur.execute(sql)
    res_temp = cur.fetchall()
    column_list = list()
    for each in res_temp:
        column_list.append(each[0])
    con.close()
    return column_list


def all_column_list_new(con, cur, _db_name, _table_name):
    sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (_db_name, _table_name)
    cur.execute(sql)
    res_temp = cur.fetchall()
    column_list = list()
    for each in res_temp:
        column_list.append(each[0])
    return column_list


def primary_column_name(_db_name, _table_name):
    connection = db_con.con2db('MySQL', _db_name)
    con = connection[0]
    cur = connection[1]
    sql = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (_db_name, _table_name)
    cur.execute(sql)
    res_temp = cur.fetchall()
    column_list = list()
    for each in res_temp:
        print(each[0])
        column_list.append(each[0])
    return column_list


def primary_column_list_new(con, cur, _db_name, _table_name):
    sql = "SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='%s' and TABLE_NAME='%s'" % (_db_name, _table_name)
    cur.execute(sql)
    res_temp = cur.fetchall()
    column_list = list()
    for each in res_temp:
        # print(each[0])
        column_list.append(each[0])
    return column_list


def data_column_list_new(con, cur, _db_name, _table_name):
    primary_column_list = primary_column_list_new(con, cur, _db_name, _table_name)
    all_column_list = all_column_list_new(con, cur, _db_name, _table_name)
    data_arg_list = all_column_list.copy()
    for each in primary_column_list:
        data_arg_list.remove(each)
    return data_arg_list


def get_all_cookie():
    try:
        connection = db_con.con2db('MySQL', 'moka_wechat_token_cookie')
        con = connection[0]
        cur = connection[1]

        cookie_list = []

        sql = "SELECT token,Cookie FROM wechat_token_cookie"
        cur.execute(sql)
        _res = cur.fetchall()
        for line in _res:
            cookie_list.append(line)
        con.close()
        return cookie_list
    except :
        logutil.error('Failure')
        return


def get_exist_list(db_name, table_name, col_name, _date):
    connection = db_con.con2db('MySQL', db_name)
    con = connection[0]
    cur = connection[1]

    cookie_list = []
    try:
        if _date is not None:
            sql = "SELECT original_username FROM %s.%s WHERE to_days(%s)=to_days('%s')" % (db_name, table_name, col_name, _date)
        else:
            sql = "SELECT original_username FROM %s.%s" % (db_name, table_name)
        cur.execute(sql)
        res = cur.fetchall()
        for line in res:
            cookie_list.append(line[0])
        con.close()
        return cookie_list
    except :
        logutil.error('Failure')
        con.close()
        return


def get_clean_cookie(_db_name, _table_name, _col_name, __date):
    connection = db_con.con2db('MySQL', 'moka_wechat_token_cookie')
    con = connection[0]
    cur = connection[1]
    exist_list = get_exist_list(_db_name, _table_name, _col_name, __date)

    cookie_list = []
    try:
        sql = "SELECT token,Cookie,original_username FROM wechat_token_cookie"
        cur.execute(sql)
        res = cur.fetchall()
        for line in res:
            if line[2] not in exist_list:
                cookie_list.append(line)
        con.close()
        return cookie_list
    except :
        logutil.error('Failure')
        con.close()
        return


def get_need_check_accounts():
    connection = db_con.con2db('MySQL', 'web_crawler_wechat_data')
    con = connection[0]
    cur = connection[1]

    all = list()
    try:
        sql = "SELECT username,password FROM accounts WHERE (account_state<>2 or account_state is NULL) and (transfer_status<>2 or transfer_status is NULL)"
        cur.execute(sql)
        res = cur.fetchall()
        for line in res:
            all.append(line)
        return all
    except :
        logutil.error('')
        return


def get_data(_db_name, _table_name, _col_list=None, _where_info=None, limit_info=None):
    connection = db_con.con2db('MySQL', _db_name)
    con = connection[0]
    cur = connection[1]
    if _col_list is None or len(_col_list) == 0:
        col_info = "*"
    else:
        col_info = ','.join(_col_list)

    sql_01 = "SELECT %s FROM %s.%s" % (col_info, _db_name, _table_name)
    sql_02 = "SELECT %s FROM %s.%s WHERE %s" % (col_info, _db_name, _table_name, _where_info)
    sql_03 = "SELECT %s FROM %s.%s LIMIT %s" % (col_info, _db_name, _table_name, limit_info)
    sql_04 = "SELECT %s FROM %s.%s WHERE %s LIMIT %s" % (col_info, _db_name, _table_name, _where_info, limit_info)

    try:
        if (_where_info is None) and (limit_info is None):
            sql = sql_01
        elif (_where_info is not None) and (limit_info is None):
            sql = sql_02
        elif (_where_info is None) and (limit_info is not None):
            sql = sql_03
        elif (_where_info is not None) and (limit_info is not None):
            sql = sql_04
        # print(sql)
        res_dict = query(con, cur, sql)
        return res_dict
    except :
        logutil.error('Failure')
        con.close()
        return


def get_data_silence(_db_name, _table_name, _col_list=None, _where_info=None, limit_info=None):
    connection = db_con.con2db_silence('MySQL', _db_name)
    con = connection[0]
    cur = connection[1]
    if _col_list is None or len(_col_list) == 0:
        col_info = "*"
    else:
        col_info = ','.join(_col_list)

    sql_01 = "SELECT %s FROM %s.%s" % (col_info, _db_name, _table_name)
    sql_02 = "SELECT %s FROM %s.%s WHERE %s" % (col_info, _db_name, _table_name, _where_info)
    sql_03 = "SELECT %s FROM %s.%s LIMIT %s" % (col_info, _db_name, _table_name, limit_info)
    sql_04 = "SELECT %s FROM %s.%s WHERE %s LIMIT %s" % (col_info, _db_name, _table_name, _where_info, limit_info)

    try:
        if (_where_info is None) and (limit_info is None):
            sql = sql_01
        elif (_where_info is not None) and (limit_info is None):
            sql = sql_02
        elif (_where_info is None) and (limit_info is not None):
            sql = sql_03
        elif (_where_info is not None) and (limit_info is not None):
            sql = sql_04
        # print(sql)
        res_dict = query(con, cur, sql)
        return res_dict
    except :
        logutil.error('Failure')
        con.close()
        return


def clean_data(_db_name, _table_name):
    connection = db_con.con2db('MySQL', _db_name)
    con = connection[0]
    cur = connection[1]

    sql = "truncate table %s.%s" % (_db_name, _table_name)
    try:
        cur.execute(sql)
        #logutil.info("已清空")
        con.close()
    except :
        logutil.error('Failure')
        con.close()
        return


def get_data_con(con, cur, _db_name, _table_name, _col_list, _where_info=None, limit_info=None):

    sql_01 = "SELECT %s FROM %s.%s" % (','.join(_col_list), _db_name, _table_name)
    sql_02 = "SELECT %s FROM %s.%s WHERE %s" % (','.join(_col_list), _db_name, _table_name, _where_info)
    sql_03 = "SELECT %s FROM %s.%s LIMIT %s" % (','.join(_col_list), _db_name, _table_name, limit_info)
    sql_04 = "SELECT %s FROM %s.%s WHERE %s LIMIT %s" % (','.join(_col_list), _db_name, _table_name, _where_info, limit_info)

    cookie_list = list()
    try:
        if (_where_info is None) and (limit_info is None):
            sql = sql_01
        elif (_where_info is not None) and (limit_info is None):
            sql = sql_02
        elif (_where_info is None) and (limit_info is not None):
            sql = sql_03
        elif (_where_info is not None) and (limit_info is not None):
            sql = sql_04

        cur.execute(sql)
        _res = cur.fetchall()
        for line in _res:
            line = list(line)
            res_temp = dict()
            for i in range(len(_col_list)):
                res_temp.update(
                    {_col_list[i]: line[i]}
                )
            cookie_list.append(res_temp)
        con.close()
        return cookie_list
    except :
        logutil.error('Failure')
        con.close()
        return


def cookie_dict():
    _db_name = "moka_wechat_token_cookie"
    _table_name = "wechat_token_cookie"
    _col_list = ["original_username", "token", "Cookie"]
    data = get_data(_db_name, _table_name, _col_list)
    return data


def cookie_dict_version_for(version_for, original_username):
    try:
        if version_for == "moka":
            _db_name = "moka_wechat_token_cookie"
            _table_name = "wechat_token_cookie"
            _col_list = ["original_username", "token", "Cookie"]
            _where_info = "original_username='%s'" % original_username
            data = get_data(_db_name, _table_name, _col_list, _where_info)
            return data
        elif version_for == "weibo":
            _db_name = "onekeyload_weibo"
            _table_name = "rb_account_cookie"
            _col_list = ["ghid", "token", "cookie_info"]
            _where_info = "ghid='%s'" % original_username
            connection = db_con.connect_one_key_load_silence_for(_db_name)
            data = get_data_con(connection[0], connection[1], _db_name, _table_name, _col_list, _where_info)
            if data is not None and len(data) > 0:
                for each in data:
                    _original_username = each.get("ghid")
                    Cookie = str(each.get("cookie_info")).replace('"', '').replace(':', '=').replace(',', '; ').replace('{', '').replace('}', '')
                    each["original_username"] = _original_username
                    each["Cookie"] = Cookie
            return data
        else:
            return
    except:
        return


def cookie_dict_con(_db_name, _table_name, _col_list):
    connection = db_con.connect_one_key_load(_db_name)
    con, cur = connection
    # _col_list = ["original_username", "token", "Cookie"]
    data = get_data_con(con, cur, _db_name, _table_name, _col_list)
    return data

#
# def setting(con, cur):
#     all_setting = list()
#     try:
#         sql = "SELECT username,password FROM accounts WHERE (account_state<>2 or account_state is NULL) and (transfer_status<>2 or transfer_status is NULL)"
#         cur.execute(sql)
#         res_temp = cur.fetchall()
#         for line in res_temp:
#             all_setting.append(line)
#         return all_setting
#         con.close()
#     except :
#         logutil.error('')
#         con.close()
#         return


def mengya_a_weixin_tc():
    try:
        _db_name = "mengya"
        con, cur = db_con.con2db_mengya("MySQL", _db_name)
        _table_name = "my_throw_cookie_collect"
        _col_list = ["collect_id",
                     "ghid",
                     "wechat_name",
                     "login_name",
                     "server_id",
                     "server_name",
                     "cookie",
                     "token",
                     "createtime",
                     "updatetime"]
        # _where_info = "login_name = '陈皮'"
        res = get_data_con(con, cur, _db_name, _table_name, _col_list)
        if res is not None and len(res) > 0:
            for each in res:
                cookie = each.get("cookie")
                if cookie is not None:
                    each["cookie"] = cookie.replace('"', "").replace(":", "=").replace(",", ";").replace("{", "").replace("}", "")
        return res
    except:
        return


if __name__ == "__main__":
    module_name = "ad_place_checker"
    version_code = "1.0"
    res = setting(module_name, version_code)
    print(res)

'''