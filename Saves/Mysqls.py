# coding=UTF-8
import time
import pymysql
from Luo_Wang.Saves.Mysql_settings import *
from Luo_Wang.Utils import logutils
from Luo_Wang.Saves import db_util

def connect(dbname,user,passwd):

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

		except Exception as e:
			time.sleep(1)
			connected = False

			logutils.error('数据库连接失败{}，正在尝试重新连接……'.format(e))


def select_by_clause(dbname, user,passwd,sql_clause):

	db_con, db_cur = connect(dbname,user,passwd)
	data_list = []
	try:
		db_cur.execute(sql_clause)
		data_list = db_cur.fetchall()
	except Exception:
		logutils.error("获取数据出错")
	db_con.close()
	return data_list


def insert_by_args(db_name, table_name, user,passwd,data_dict_list, arg_list):
	"""
			将数据批量插入数据库，传入sql参数的是字段名list
		:param data_dict_list 以字典list的形式传入
		:param arg_list 以["arg1", "arg2", "arg3"]的形式传入
		"""
	db_con, db_cur = connect(db_name,user,passwd)
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
		logutils.info(insert_clause)
		logutils.info('正在插入数据...')
		logutils.info(insert_clause)
		db_cur.executemany(insert_clause,insert_param)
		db_con.commit()
		logutils.info("插入完成")
	except Exception as e:
		logutils.error(e)
		logutils.error('批量插入失败，进行数据插入回滚')
		db_con.rollback()
		db_con.close()
	#db_con.close()


def	update_by_args(db_name, table_name,user,passwd,data_dict_list,arg_list,pk_list):
	db_con, db_cur = connect(db_name, user, passwd)
	primary_column_list = db_util.primary_column_name(db_con, db_cur, db_name, table_name)
	all_column_list     = db_util.all_column_name(db_con, db_cur, db_name, table_name)
	for each in primary_column_list:
		all_column_list.remove(each)

	if pk_list is None:
		pk_list = primary_column_list
	if arg_list is None:
		arg_list = all_column_list

	all_arg_list = list()
	all_arg_list.extend(arg_list)
	all_arg_list.extend(pk_list)

	update_part1 = '=%s,'.join(arg_list) + '=%s'
	update_part2 = '=%s and '.join(pk_list) + '=%s'
	update_clause = 'UPDATE %s.%s SET %s WHERE %s' % (db_name, table_name, update_part1, update_part2)
	update_param = []

	for data_dict in data_dict_list:
		update_param.append(tuple(data_dict.get(arg) for arg in all_arg_list))
	update_param_set = set(update_param)  # set去重
	update_param = list(update_param_set)

	try:
		logutils.info('正在更新数据...' )
		db_cur.executemany(update_clause,update_param)
		db_con.commit()
		#count += 1
		logutils.info("更新完成")
	except Exception:
		logutils.error("更新数据时发生错误，内容：%s")

	db_con.close()
	#return count

def insert_or_update_by_args(db_name, table_name, user,passwd,data_dict_list, arg_list,pk_list):
	"""
		将数据插入数据库，如果插入失败则改为更新数据，传入的sql参数是字段名list，分为信息字段名list和主键字段名list
		:param data_dict 以字典的形式传入（不是列表）
		:param signature_arg_list 以["arg1", "arg2", "arg3"]的形式传入主键字段
		:param info_arg_list 以["arg1", "arg2", "arg3"]的形式传入信息字段，不包括主键字段
	"""
	db_con, db_cur = connect(db_name,user,passwd)


	insertion_part1 = ','.join(arg_list)
	insertion_part2 = ','.join(["%s" for i in range(len(data_dict_list)+1)])
	insert_clause = '''INSERT INTO %s (%s) VALUES (%s)''' % (table_name, insertion_part1, insertion_part2)
	all_arg_list = list()
	all_arg_list.extend(arg_list)
	all_arg_list.extend(pk_list)
	for data_dict in data_dict_list:
		insert_param = tuple(data_dict.get(arg) for arg in arg_list )



		update_part1 = '=%s,'.join(arg_list) + '=%s'
		update_part2 = '=%s and '.join(pk_list) + '=%s'
		update_clause = 'UPDATE %s SET %s WHERE %s' % (table_name, update_part1, update_part2)
		update_param = tuple(data_dict.get(arg) for arg in all_arg_list)


		try:
			logutils.debug(('插入数据：%s %s %s %s' % insert_param))
			db_cur.execute(insert_clause,insert_param)
			db_con.commit()
			logutils.info("插入完成")
		except pymysql.IntegrityError:
			try:

				logutils.debug(('更新数据：%s %s %s %s %s' % update_param))
				db_cur.execute(update_clause,update_param)
				db_con.commit()
				logutils.info("更新完成")
			except Exception:
				logutils.error("更新数据时发生错误，数据内容：%s" % data_dict)
		except Exception as e:
			print(e)
			logutils.error("插入数据时发生错误，数据内容：%s" % data_dict)


def	Mysql(dbname,table,user,passwd,types,data_dict_list,arg_list,key_list):


	if types.upper() == 'INSERT':

		insert_by_args(dbname, table,user,passwd,data_dict_list, arg_list)

	elif types.upper() == 'UPDATE':

		update_by_args(dbname, table,user,passwd,data_dict_list, arg_list,key_list)

	else:

		insert_or_update_by_args(dbname,table,user,passwd,data_dict_list, arg_list, arg_list[:1])


if __name__ == '__main__':

	pass
