import re
import json
import requests
from bs4 import BeautifulSoup
from lxml import etree
from pyquery import PyQuery as pq
from Luo_Wang.Crawls.headers import *
from Luo_Wang.Crawls.request import *



def demo(url):

	abc = [{'id':1,'name':'qwer','age':118,'tf':'qwer'},
           {'id':2,'name':'qaaq','age':118,'tf':'qaaq'},
		   {'id':3,'name':'zxcv','age':118,'tf':'qaaq'}


		   ]

	return abc



def maoyan(url):

	movie_list = []
	html = request_text(url)
	pattern = re.compile('<dd>.*?board-index.*?>(\d+)</i>.*?src="(.*?)".*?name"><a'
						 + '.*?href="(.*?)".*?>(.*?)</a>.*?star">(.*?)</p>.*?releasetime">(.*?)</p>'
						 + '.*?integer">(.*?)</i>.*?fraction">(.*?)</i>.*?</dd>', re.S)
	items = re.findall(pattern, html)
	for item in items:
		movie_dict = {}
		movie_dict['id'] = item[0]
		movie_dict['image'] = item[1]
		movie_dict['url_i'] = "https://maoyan.com" + str(item[2])
		movie_dict['title'] = item[3]
		movie_dict['actor'] = item[4].strip()[3:]
		movie_dict['times'] = item[5].strip()[5:]
		movie_dict['score'] = item[6] + item[7]
		movie_list.append(movie_dict)
	logutils.info(len(movie_list))
	return movie_list