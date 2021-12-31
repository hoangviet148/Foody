from time import sleep
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

def get_content_items(city_search="Hải Phòng", food_name="Lẩu"):

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
    res = res.replace(" kết quả","")
    res = res.replace(",","")
    res = res.replace(".","")
    total_page = int(int(res) / 12)-2


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
    
    return item_content

def get_item(item_content):

    lines = []

    for item in item_content:
        mark = item.find_element_by_class_name('status').text
        name = item.find_elements_by_tag_name('h2')[0].text
        address = item.find_element_by_class_name('result-address').text
        num_reviewer = item.find_element_by_class_name('stats').text
        num_reviewer = num_reviewer.split(" ")[0]
        line = str(name) + "," + str(mark) + "," + str(address) + "," + str(num_reviewer)
        lines.append(line)

    lines = ("\n").join(lines)

    sleep(10)
    browser.close()
    
    return lines