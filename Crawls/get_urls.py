def demo():
	url = ['www.baidu.com']
	return url



def maoyan():

	start_urls = []
	for i in range(0, 10):
		start_urls.append('http://maoyan.com/board/4?offset=' + str(i * 10))
	return start_urls
