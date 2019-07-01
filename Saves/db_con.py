import pymysql
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


def con2db_mysql_silence(host, db_name, user, pass_wd, port=3306, charset='utf8'):
    while True:
        try:
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
            return con, cur
        except :
            pass


def con2db(db_type, db_name):
    if db_type == 'MySQL':
        host = Mysql_settings.host
        user = Mysql_settings.user
        pass_wd = Mysql_settings.passwd
        connection = con2db_mysql(host, db_name, user, pass_wd)
        return connection


def connect_one_key_load_for(db_name):
    host = 'groups.cqmhmrzuwsw7.rds.cn-north-1.amazonaws.com.cn'
    user = 'caolm'
    pass_wd = 'caolm123456'
    connection = con2db_mysql(host, db_name, user, pass_wd)
    return connection


def connect_one_key_load_silence_for(db_name):
    host = 'groups.cqmhmrzuwsw7.rds.cn-north-1.amazonaws.com.cn'
    user = 'caolm'
    pass_wd = 'caolm123456'
    connection = con2db_mysql_silence(host, db_name, user, pass_wd)
    return connection


def con2db_silence(db_type, db_name):
    if db_type == 'MySQL':
        host = mysql_setting.MYSQL_HOST
        user = mysql_setting.MYSQL_SUPER_USER
        pass_wd = mysql_setting.MYSQL_SUPER_PASS_WD
        connection = con2db_mysql_silence(host, db_name, user, pass_wd)
        return connection


def con2db_mysql_main(db_type, db_name):
    if db_type == 'MySQL':
        host = mysql_setting.MYSQL_HOST
        user = mysql_setting.MYSQL_SUPER_USER
        pass_wd = mysql_setting.MYSQL_SUPER_PASS_WD
        connection = con2db_mysql(host, db_name, user, pass_wd)
        return connection


def con2db_mysql_backup(db_type, db_name):
    if db_type == 'MySQL':
        host = mysql_setting.BACKUP_MYSQL_HOST
        user = mysql_setting.BACKUP_MYSQL_SUPER_USER
        pass_wd = mysql_setting.BACKUP_MYSQL_SUPER_PASS_WD
        connection = con2db_mysql(host, db_name, user, pass_wd)
        return connection


def con2db_dbinfo(db_type, db_name):
    if db_type == 'MySQL':
        host = mysql_setting.BACKUP_MYSQL_HOST
        user = mysql_setting.BACKUP_MYSQL_SUPER_USER
        pass_wd = mysql_setting.BACKUP_MYSQL_SUPER_PASS_WD
        connection = con2db_mysql(host, db_name, user, pass_wd)
        return connection


def con2db_mengya(db_type, db_name):
    if db_type == 'MySQL':
        host = mysql_setting.mengya_MYSQL_HOST
        user = mysql_setting.mengya_MYSQL_USER
        pass_wd = mysql_setting.mengya_MYSQL_PASS_WD
        connection = con2db_mysql(host, db_name, user, pass_wd)
        return connection


def connect_one_key_load(db_name='onekeyload'):
    host = 'groups.cqmhmrzuwsw7.rds.cn-north-1.amazonaws.com.cn'
    user = 'caolm'
    pass_wd = 'caolm123456'
    connection = con2db_mysql_silence(host, db_name, user, pass_wd)
    return connection


if __name__ == "__main__":
    connection = con2db("MySQL", "crawl_args")
    print(connection)
