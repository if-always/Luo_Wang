from Luo_Wang.Utils.logutils import *
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED


def Current(func,args,nums):


    executor = ThreadPoolExecutor(max_workers=nums)

    future_tasks = [executor.submit(func, arg) for arg in args]

    wait(future_tasks, return_when=ALL_COMPLETED)


    res = (task.result() for task in future_tasks)
    

    for res in future_tasks:

        info(res.done())
        info(res.result())
    return res




if __name__ == '__main__':
    pass
    
    # def get_links(url):

    #     res = requests.get(url)
    #     return res.status_code

    # urls = [ "https://www.baidu.com" for i in range(10)]
    # start = time.time()
    # datas = Current(get_links,urls,2)
    # print(list(datas))
    # end = time.time()
    # b = end - start
    # print(b)