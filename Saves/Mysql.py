# coding=UTF-8
import time
import pymysql
from Luo_Wang.Saves.Mysql_settings import *
from Luo_Wang.Utils import logutils
from Luo_Wang.Saves import db_util

def connect(dbname):

	connected = False
	while connected is False:
		try:
			db_con = pymysql.connect(host=host,user=user,passwd=passwd,db=dbname,port=3306,charset='utf8mb4')
			db_cur = db_con.cursor()
			db_cur.execute('SET NAMES utf8mb4')
			db_cur.execute("SET CHARACTER SET utf8mb4")
			db_cur.execute("SET character_set_connection=utf8mb4")
			logutils.info(str(dbname) + ' 数据库连接成功。')
			return db_con, db_cur

		except Exception:
			time.sleep(1)
			connected = False
			logutils.error('%s数据库连接失败，正在尝试重新连接……' % dbname)


def select_by_clause(dbname, sql_clause):

	db_con, db_cur = connect(dbname)
	data_list = []
	try:
		db_cur.execute(sql_clause)
		data_list = db_cur.fetchall()
	except Exception:
		logutils.error("获取数据出错")
	db_con.close()
	return data_list


def insert_by_args(db_name, table_name, data_dict_list, arg_list):
	"""
			将数据批量插入数据库，传入sql参数的是字段名list
		:param data_dict_list 以字典list的形式传入
		:param arg_list 以["arg1", "arg2", "arg3"]的形式传入
		"""
	db_con, db_cur = connect(db_name)
	insertion_part1 = ','.join(arg_list)
	insertion_part2 = ','.join(["%s" for i in range(len(arg_list))])
	insert_clause = '''INSERT INTO %s (%s) VALUES (%s)''' % (table_name, insertion_part1, insertion_part2)
	logutils.debug(insert_clause)

	insert_param = []
	for data_dict in data_dict_list:
		insert_param.append(tuple((data_dict.get(arg) for arg in arg_list)))
	insert_param_set = set(insert_param)  # set去重
	insert_param = list(insert_param_set)

	try:
		logutils.info('正在插入数据...')
		db_cur.executemany(insert_clause,insert_param)
		db_con.commit()
		logutils.info("插入完成")
	except Exception:
		logutils.error('批量插入失败，进行数据插入回滚')
		db_con.rollback()
		db_con.close()
	db_con.close()


def	update_by_args(db_name, table_name,data_dict_list,pk_arg_list,arg_list):

	db_con, db_cur = connect(db_name)
	primary_column_list = db_util.primary_column_name(db_con, db_cur, db_name, table_name)
	all_column_list     = db_util.all_column_name(db_con, db_cur, db_name, table_name)
	for each in primary_column_list:
		all_column_list.remove(each)

	if pk_arg_list is None:
		pk_arg_list = primary_column_list
	if arg_list is None:
		arg_list = all_column_list

	all_arg_list = list()
	all_arg_list.extend(arg_list)
	all_arg_list.extend(pk_arg_list)

	update_part1 = '=%s,'.join(arg_list) + '=%s'
	update_part2 = '=%s and '.join(pk_arg_list) + '=%s'
	update_clause = 'UPDATE %s.%s SET %s WHERE %s' % (db_name, table_name, update_part1, update_part2)
	update_param = []

	for data_dict in data_dict_list:
		update_param.append(tuple(data_dict.get(arg) for arg in all_arg_list))
	update_param_set = set(update_param)  # set去重
	update_param = list(update_param_set)
	print(update_param)
	count = 0
	for each in update_param:
		try:
			logutils.info('%s/%s 正在更新数据...' % (count + 1, len(update_param)))
			db_cur.execute(update_clause, each)
			db_con.commit()
			count += 1
			logutils.info("更新完成")
		except Exception:
			logutils.error("更新数据时发生错误，内容：%s")

	db_con.close()
	return count

def insert_or_update_by_args(db_name, table_name, data_dict, signature_arg_list, info_arg_list):
    """
        将数据插入数据库，如果插入失败则改为更新数据，传入的sql参数是字段名list，分为信息字段名list和主键字段名list
    :param data_dict 以字典的形式传入（不是列表）
    :param signature_arg_list 以["arg1", "arg2", "arg3"]的形式传入主键字段
    :param info_arg_list 以["arg1", "arg2", "arg3"]的形式传入信息字段，不包括主键字段
    """
    db_con, db_cur = connect(db_name)

    all_arg_list = list()
    all_arg_list.extend(info_arg_list)
    all_arg_list.extend(signature_arg_list)
    insertion_part1 = ','.join(all_arg_list)
    insertion_part2 = ','.join(["%s" for i in range(len(all_arg_list))])
    insert_clause = '''INSERT INTO %s (%s) VALUES (%s)''' % (table_name, insertion_part1, insertion_part2)


    insert_param = tuple((data_dict.get(arg) for arg in all_arg_list))

    update_part1 = '=%s,'.join(all_arg_list)+ '=%s'

    update_part1 = '=%s,'.join(info_arg_list) + '=%s'
    update_part2 = '=%s and '.join(signature_arg_list) + '=%s'
    update_clause = 'UPDATE %s SET %s WHERE %s' % (table_name, update_part1, update_part2)
    update_param = tuple(data_dict.get(arg) for arg in all_arg_list)
    try:
        logutils.debug(('插入数据：%s ' % data_dict))
        db_cur.executemany(insert_clause, [insert_param])
        db_con.commit()
    except pymysql.IntegrityError:
        try:
            logutils.debug(('更新数据：%s ' % data_dict))
            logutils.info("更新完成")
            db_cur.execute(update_clause, update_param)

            db_con.commit()
        except Exception:
            logutils.error("更新数据时发生错误，数据内容：%s" % data_dict)
    except Exception as e:
        logutils.error("插入数据时发生错误，数据内容：%s" % data_dict)


if __name__ == '__main__':



	#insert_by_args()
	#select_by_clause("crawl_args",'''SELECT COLUMN_NAME FROM information_schema.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA='crawl_args' and TABLE_NAME='funcname' ''')
	#db_con,db_cur = connect("crawl_args")
	#qaaq.select_by_clause('crawl_args',"SELECT * FROM `funcname`")

	datas = {'id': '89', 'name': 'n', 'statement': 'jinlian'}
	args = ['name','statement']
	keys = ['id']
	# insert_by_args('crawl_args', 'funcname', datas, args)
	insert_or_update_by_args('crawl_args', 'funcname',datas,keys,args)