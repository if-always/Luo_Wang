
from Luo_Wang.Crawls import get_urls
from Luo_Wang.Crawls import function


demo = {
	'name':'Demo',
	'urls':get_urls.demo,
	'func_name':function.demo,
	'save_type':{'mysql':['test','root','']}

}

if __name__ == '__main__':
	#print(demo['urls'])
	print(demo['save_type'].keys())