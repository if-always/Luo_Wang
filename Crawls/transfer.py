# coding=UTF-8
import os
from time import time
from Luo_Wang.Crawls import current
from Luo_Wang.Utils.logutils import *


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
        res = current.Current(func_name,starts_url,th_num)

    except Exception as e:
        error(e)
        os._exit(1)

    if dict_name['save_type'].keys()[0] == 'mysql':
        info("正在连接数据库......")
    else:
        print("123")
    # #
	#
    # #     #if save_type is str:
    # #     print("*********************")
    # #     dict_name.get('save_type')(res,dict_name.get('save_path'),dict_name.get('id_name'))
    # #     logutil.info(res)
	#
    # except:
	#
    # #     logutil.error("目标处理函数获取失败")
    #     os._exit(1)
    # # end_time = time()
    # # logutil.info('Cost {} seconds'.format((end_time - start_time)))