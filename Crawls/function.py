import re
import json
import requests
from bs4 import BeautifulSoup
from lxml import etree
from pyquery import PyQuery as pq
from Luo_Wang.Crawls.headers import *




def demo(url):
	#print(1234)
	abc = {'name':'qaaq','age':17,'hobby':'games'}
	#print(abc)
	return abc