import time

from bs4 import BeautifulSoup
import pandas as pd
import requests
import os
import numpy as np
import shutil
import random
import ntpath
import glob
import multiprocessing

test_url = "https://www.foody.vn/ha-noi/xoi-hong-ngoc-lam"
path_data = "total_csv_crawl_1_1_2021.csv"


def merge(list_dict):
    tmp_dict = {}
    for dic in list_dict:
        tmp_dict = {**tmp_dict, **dic}

    return tmp_dict

def average(lis):
    return sum(lis)/len(lis)


def crawl_comment(url):
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html5lib')
    comments = []
    comments = [i.find_all('span')[0].text for i in soup.find_all(class_='rd-des')][:len(comments) - 2]
    comments = "<sep>".join(comments)
    try:
        total_mark = [i.find("span").text for i in soup.find_all('div', attrs={'class': 'microsite-top-points'})]
    except AttributeError:
        total_mark = ['no data'] * 5
    if len(total_mark) < 5:
        total_mark = ['no data'] * 5
    try:
        item_price = soup.find('span', attrs={'itemprop': 'priceRange'})
        price = [i.text.strip() for i in item_price.find_all("span") if i.text != ""]
        price = price[:-1]
        price = " ".join(price)
    except AttributeError:
        price = "no data"
    return comments, total_mark[0], total_mark[1], total_mark[2], total_mark[3], total_mark[4], price


def crawl_comment_multi_process(chunk):
    dict_tmp = {}
    for url in chunk:
        print(url)
        try:
            if 'thuong-hieu' in url:
                r = requests.get(url)
                soup = BeautifulSoup(r.content, 'html5lib')
                cmt_tmp = []
                av1 = []
                av2 = []
                av3 = []
                av4 = []
                av5 = []
                price_tmp = []
                for item in soup.find_all('div', attrs= {"class":'ldc-item-h-name'}):
                    if 'target="_blank"' in str(item) and "{{Model.Url}}" not in str(item):
                        path_store = "https://www.foody.vn" + item.find("a")['href']
                        feature = crawl_comment(path_store)
                        if 'no data' in feature:
                            continue
                        else:
                            cmt_tmp.append(feature[0])
                            av1.append(float(feature[1]))
                            av2.append(float(feature[2]))
                            av3.append(float(feature[3]))
                            av4.append(float(feature[4]))
                            av5.append(float(feature[5]))
                            price_tmp.append(feature[6])


                try:
                    dict_tmp[url] = ["<sep>".join(cmt_tmp), str(average(av1)), str(average(av2)),
                                     str(average(av3)), str(average(av4)), str(average(av5)), price_tmp[-1]]
                except ZeroDivisionError:
                    dict_tmp[url] = ["no data"]*7


            else:
                tmp = crawl_comment(url)
                dict_tmp[url] = tmp
            # print("*"*50)
            # print(f"url:{url}")
            # print(tmp)
        except requests.exceptions.MissingSchema:
            print(f"invalid url: {url}")

    return dict_tmp


def get_data(path_data="./data/link.csv"):
    total_link = []
    total_comment = []
    total_quality = []
    total_location = []
    total_price = []
    total_service = []
    total_space = []
    price = []
    metadata_link = {}
    link_need_process = []
    # folder_data = "./data/split_3/"
    # list_path_data = glob.glob(folder_data + "*.csv")
    # for path_data in list_path_data:
    name_file = ntpath.basename(path_data)[:-4]
    print(name_file)
    data = pd.read_csv(path_data)
    # data = data.dropna()
    # os.remove(path_data)
    # data.to_csv(path_data, encoding = 'utf-8', index= False)
    num_reviewer = data['S??? ng?????i review'].values
    link = data['Link'].values
    assert len(link) == len(num_reviewer)

    i = 0
    for idx, num in enumerate(num_reviewer):
        if num == '0' or num == 0:
            i = i + 1
            metadata_link[link[idx]] = ["no data"] * 7
        else:
            link_need_process.append(link[idx])

    print(path_data)
    print(f"{len(metadata_link)} rows do not have data")
    chunk_size = 50
    num_worker = 6
    chunk_link_need_process = [link_need_process[i:i + chunk_size]
                                for i in range(0, len(link_need_process), chunk_size)]

    split_chunk_link_need_process = [chunk_link_need_process[i:i + num_worker]
                                        for i in range(0, len(chunk_link_need_process), num_worker)]

    list_dict_need_process = []
    for chunk_list in split_chunk_link_need_process:
        total_dict = {}
        pool = multiprocessing.Pool(len(chunk_list))
        total_dict = pool.map(crawl_comment_multi_process, chunk_list)
        merge_dict = merge(total_dict)
        list_dict_need_process.append(merge_dict)
        print(len(list_dict_need_process))

        pool.terminate()
        pool.join()

    metadata_link = merge([metadata_link, merge(list_dict_need_process)])
    for k, v in metadata_link.items():
        total_link.append(k)
        total_comment.append(v[0])
        total_location.append(v[1])
        total_quality.append(v[2])
        total_space.append(v[3])
        total_service.append(v[4])
        total_price.append(v[5])
        price.append(v[6])

    cdata = {"Link": total_link,
                "B??nh Lu???n": total_comment,
                "?????a ch???": total_location,
                "Ch???t l?????ng": total_quality,
                "Kh??ng gian": total_space,
                "Ph???c v???": total_service,
                "G??a c???": total_price,
                "M???c gi??": price
                }

    dataframe = pd.DataFrame(cdata)

    merge_dataframe = pd.merge(data, dataframe, on="Link")
    results = merge_dataframe.to_json()
    print(results)