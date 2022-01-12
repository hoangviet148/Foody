from selenium import webdriver
from time import sleep
from selenium.webdriver.common.keys import Keys
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager
import multiprocessing
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urljoin, urlparse
import requests
from crawl_foody import crawl_foody_city_food

pairs_city_food = [['Hà Nội', 'Lẩu'], ['Hà Nội', 'Phở']]

def crawl_multi_process(pair_city_food):
    return crawl_foody_city_food(pair_city_food[0],pair_city_food[1])


if __name__ == '__main__':
    p = multiprocessing.Pool(2)
    results = p.map(crawl_multi_process, pairs_city_food)
    dataframe_result = pd.concat(results)
    dataframe_result.to_csv('test.csv', encoding='utf-8',index=False)
    p.terminate()
    p.join()