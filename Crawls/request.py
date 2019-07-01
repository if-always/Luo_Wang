# coding=UTF-8
import chardet
import requests
from Luo_Wang.Utils import logutils
from Luo_Wang.Crawls.headers import pass_useragent


def request_text(url):

	headers = pass_useragent()
	try:
		res = requests.get(url, headers=headers)
		if res.status_code == 200:
			res.encoding = chardet.detect(res.content)['encoding']
			html = res.text
			return html
		else:
			logutils.error("网页请求错误" + str(res.status_code))
			return None
	except requests.ConnectionError:
		logutils.error("网页请求失败")



def request_json(url):

	headers = pass_useragent()
	try:
		res = requests.get(url, headers=headers)

		if res.status_code == 200:
			return res.json()
		else:
			logutils.error("网页请求错误" + str(res.status_code))
			return None
	except requests.ConnectionError:
		logutils.error("网页请求错误")