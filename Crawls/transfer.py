# coding=UTF-8
import os
from time import time
from Luo_Wang.Crawls import current
from Luo_Wang.Utils.logutils import *
from Luo_Wang.Saves.Mysqls import Mysql

def Trans(dict_name,th_num):

    start_time = time()


    info("正在加载{}项目参数".format(dict_name['name']))
    try:
        starts_url = dict_name.get('urls')()
        if len(starts_url) >= 1:

            info("{}项目url列表获取成功".format(dict_name['name']))

        else:

            warning("{}项目url列表为空".format(dict_name['name']))
            os._exit(1)
    except Exception as e:

        error(e)
        os._exit(1)

    try:

        func_name = dict_name.get('func_name')
        info("{}处理函数获取成功".format(dict_name['name']))
        info("正在分配线程 , 线程数：%d" %th_num)
        datas = current.Current(func_name,starts_url,th_num)
        info(datas)
    except Exception as e:
        error(e)
        os._exit(1)

    Mysql_name = dict_name['save_type'].keys()[0].upper()
    Mysql_dbme = dict_name['save_type'].values()[0][0]
    Mysql_tabe = dict_name['save_type'].values()[0][1]
    Mysql_user = dict_name['save_type'].values()[0][2]
    Mysql_pawd = dict_name['save_type'].values()[0][3]
    Mysql_type = dict_name['save_type'].values()[0][4]
    Mysql_args = dict_name['save_type'].values()[0][5]
    if Mysql_name == 'MYSQL':

        info("数据库信息：{0} 库名：{1} 表名：{2} 用户：{3} 操作，{4}".format(Mysql_name,Mysql_dbme.upper(),Mysql_tabe.upper(),Mysql_user.upper(),Mysql_type.upper()))

    else:

        error("数据库信息加载失败 ，请检查param_dict存储参数")
        os._exit(1)

    Mysql(Mysql_dbme,Mysql_tabe,Mysql_user,Mysql_pawd,Mysql_type,datas,Mysql_args)
    endtime = time()
    cost = endtime-start_time
    info(cost)