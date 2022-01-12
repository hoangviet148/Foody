from selenium import webdriver
from time import sleep
from selenium.webdriver.common.keys import Keys
import pandas as pd
from webdriver_manager.chrome import ChromeDriverManager

city_search = "Hải Phòng"
food_name = "Lẩu"

def crawl_foody_city_food(city_search:str, food_name:str):
    browser = webdriver.Chrome(ChromeDriverManager().install())

    browser.get("https://www.foody.vn/ha-noi")
    sleep(1)

    login = browser.find_element_by_class_name("fd-btn-login-new")
    login.click()
    sleep(1)

    txt_user = browser.find_element_by_xpath('/html/body/div/div/div[1]/div/div/div[3]/form/fieldset/div[2]/div/input')
    txt_user.send_keys("tan.np183824@sis.hust.edu.vn")
    txt_user = browser.find_element_by_xpath('/html/body/div/div/div[1]/div/div/div[3]/form/fieldset/div[3]/div/input')
    txt_user.send_keys("phuctan214")
    button_login = browser.find_element_by_id("bt_submit")
    button_login.click()
    sleep(1)

    click_city = browser.find_element_by_class_name("rn-nav-name")
    click_city.click()
    sleep(1)

    search_city = browser.find_element_by_xpath("/html/body/div[2]/header/div[2]/div/div[1]/div[2]/ul/li/div[2]/input")
    search_city.send_keys(city_search)
    sleep(1)

    city_first = browser.find_element_by_xpath("/html/body/div[2]/header/div[2]/div/div[1]/div[2]/ul/li/ul/li/ul/li[1]")
    city_first.click()
    sleep(1)

    text_search = browser.find_element_by_id("pkeywords")
    text_search.send_keys(food_name)
    text_search.send_keys(Keys.ENTER)
    sleep(1)

    total_count = browser.find_element_by_class_name("result-status-count")
    total_count = total_count.text
    res = total_count.split("\n")[0]
    res = res.replace(" kết quả", "")
    res = res.replace(",", "")
    res = res.replace(".", "")
    print(res)
    total_page = int(int(res) / 12) - 2
    print(total_page)

    SCROLL_PAUSE_TIME = 1

    # Get scroll height
    last_height = browser.execute_script("return document.body.scrollHeight")

    while True:
        # Scroll down to bottom
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        # Wait to load page
        sleep(SCROLL_PAUSE_TIME)

        # Calculate new scroll height and compare with last scroll height
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    sleep(2)

    for i in range(total_page):
        button = browser.find_element_by_xpath("//a[@rel='next']")
        browser.execute_script("arguments[0].click();", button)
        sleep(2)

    sleep(10)

    item_content = browser.find_elements_by_xpath("//div[@class='row-view-right']")
    print(len(item_content))

    total_mark = []
    total_name = []
    total_address = []
    total_num_reviewer = []

    for item in item_content:
        mark = ""
        name = ""
        address = ""
        num_reviewer = ""
        mark = item.find_element_by_class_name('status').text
        name = item.find_elements_by_tag_name('h2')[0].text
        address = item.find_element_by_class_name('result-address').text
        num_reviewer = item.find_element_by_class_name('stats').text
        num_reviewer = num_reviewer.split(" ")[0]
        total_mark.append(mark)
        total_name.append(name)
        total_address.append(address)
        total_num_reviewer.append(num_reviewer)
        print("Điểm đánh giá: " + str(mark))
        print("Tên quán: " + str(name))
        print("Địa chỉ quán: " + str(address))
        print("Số lượng người bình chọn: " + str(num_reviewer))
        print('*' * 20)

    print(len(total_name), len(total_mark), len(total_address), len(total_num_reviewer))

    cdata = {"Tên quán": total_name,
             "Điểm đánh giá": total_mark,
             "Địa chỉ quán": total_address,
             "Số người review": total_num_reviewer}

    dataframe = pd.DataFrame(cdata)

    sleep(1)
    browser.close()

    return dataframe


crawl_foody_city_food(city_search="Hải Phòng", food_name="Lẩu")
