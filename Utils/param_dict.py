'''
@Author: if_always
 Time  : 07/01 2019
 Loading param or args
'''
from Luo_Wang.Crawls import get_urls
from Luo_Wang.Crawls import function










demo = {
	'name':'Demo',
	'urls':get_urls.demo,
	'func_name':function.demo,
	'save_type':{'Mysql':['Test','demo','root','','update',['id','name','age','tf']]}

}


maoyan = {
	'name':'MaoYan',
	'urls':get_urls.maoyan,
	'func_name':function.maoyan,
	'save_type':{'Mysql':['MaoYan','movies','root','','update',['id','title','image','actor','time','score','url_i']]}
}

#demo_A = {}

if __name__ == '__main__':
	#print(demo['urls'])
	print(demo['save_type'].keys())