import json
import pyperclip
from loguru import logger
from selenium.webdriver import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from utils import change_model, restore_model, selector
import time
from cookie_pool import CookiePool
from selenium.webdriver.common.by import By
from selenium.webdriver.support.wait import WebDriverWait

options = webdriver.ChromeOptions()
options.add_experimental_option('detach', True)
# options.binary_location = r'D:\Software\chrome-win64\chrome.exe'

# # 原手工获得
# cookie_get = '''[{"domain": ".coze.com", "expiry": 1747979986, "httpOnly": true, "name": "store-country-code-src", "path": "/", "sameSite": "Lax", "secure": false, "value": "uid"}, {"domain": ".coze.com", "expiry": 1747979986, "httpOnly": true, "name": "store-idc", "path": "/", "sameSite": "Lax", "secure": false, "value": "alisg"}, {"domain": ".coze.com", "expiry": 1721627986, "httpOnly": true, "name": "ssid_ucp_v1", "path": "/", "sameSite": "None", "secure": true, "value": "1.0.0-KGVjNWU5OTU0N2QwYTdmMTEyYWNmMTY2MTJjNjY4MjM4MTU0N2RmYmIKIQiRiM7C_-S2p2YQ0ra7sgYY1J0fIAww0ra7sgY4AkDsBxADGgNzZzEiIGEyYTIwYzY3MWVmMjQ0MmEzZGYzOTI5NjJmMGVjMmYx"}, {"domain": ".coze.com", "expiry": 1721627986, "httpOnly": true, "name": "sid_ucp_v1", "path": "/", "sameSite": "Lax", "secure": true, "value": "1.0.0-KGVjNWU5OTU0N2QwYTdmMTEyYWNmMTY2MTJjNjY4MjM4MTU0N2RmYmIKIQiRiM7C_-S2p2YQ0ra7sgYY1J0fIAww0ra7sgY4AkDsBxADGgNzZzEiIGEyYTIwYzY3MWVmMjQ0MmEzZGYzOTI5NjJmMGVjMmYx"}, {"domain": ".coze.com", "expiry": 1721627986, "httpOnly": true, "name": "sessionid_ss", "path": "/", "sameSite": "None", "secure": true, "value": "a2a20c671ef2442a3df392962f0ec2f1"}, {"domain": ".coze.com", "expiry": 1721627986, "httpOnly": true, "name": "sessionid", "path": "/", "sameSite": "Lax", "secure": false, "value": "a2a20c671ef2442a3df392962f0ec2f1"}, {"domain": ".coze.com", "expiry": 1721627986, "httpOnly": true, "name": "uid_tt", "path": "/", "sameSite": "Lax", "secure": false, "value": "3a1c25ad7202bde4b83466141ea42edc3c1373cfa7f33083c64b4e67054a17e6"}, {"domain": ".coze.com", "expiry": 1747547986, "httpOnly": true, "name": "sid_guard", "path": "/", "sameSite": "Lax", "secure": false, "value": "a2a20c671ef2442a3df392962f0ec2f1%7C1716443986%7C5184000%7CMon%2C+22-Jul-2024+05%3A59%3A46+GMT"}, {"domain": ".coze.com", "expiry": 1747979986, "httpOnly": true, "name": "store-country-code", "path": "/", "sameSite": "Lax", "secure": false, "value": "sg"}, {"domain": ".coze.com", "expiry": 1747979934, "httpOnly": true, "name": "ttwid", "path": "/", "sameSite": "None", "secure": true, "value": "1%7CT7m5MaNtP30z5BHLtTCSujilv8g7V3a3Vyurla8LinY%7C1716443935%7C9a9c4b6271b7c6bef898109639b3c59e380bd7b86e3d7c6e09f192003a042f72"}, {"domain": ".coze.com", "expiry": 1719035986, "httpOnly": true, "name": "passport_auth_status", "path": "/", "sameSite": "Lax", "secure": false, "value": "031e2c1d22c69cd9386834c7f5fc8dc7%2C"}, {"domain": ".coze.com", "expiry": 1717307938, "httpOnly": false, "name": "msToken", "path": "/", "sameSite": "None", "secure": true, "value": "pF8uLf4lFGPlUJRMgK5VLXxuySbbG-9dHS_RdcU_PMbpktizpLhMrlV24so5ggHbS6_Y0fBOHROK2JIYTMtuAOkCJYbA0Afu_cATVjWJ6nzrbd71-RkYoDVVjF78Ias-"}, {"domain": ".coze.com", "expiry": 1747979986, "httpOnly": true, "name": "odin_tt", "path": "/", "sameSite": "Lax", "secure": false, "value": "e6a49eeb07fabf1ef45343715b4cd3459cb0d140bfbc57b61b1815f8bd30307ef7a826523d35e227e311ed8749befa26fbb5180d1e4dc0cdd34fbe636863e381"}, {"domain": ".coze.com", "expiry": 1721627956, "httpOnly": false, "name": "passport_csrf_token", "path": "/", "sameSite": "None", "secure": true, "value": "5ed4edb9c28c08c5b72fd60a16d76dfa"}, {"domain": ".coze.com", "expiry": 1719035986, "httpOnly": true, "name": "passport_auth_status_ss", "path": "/", "sameSite": "None", "secure": true, "value": "031e2c1d22c69cd9386834c7f5fc8dc7%2C"}, {"domain": ".www.coze.com", "expiry": 1747979991, "httpOnly": false, "name": "consent_cookie", "path": "/", "sameSite": "None", "secure": true, "value": "{%22analytics_and_performance%22:{%22active%22:true%2C%22disabled%22:false}}"}, {"domain": ".coze.com", "expiry": 1721627986, "httpOnly": true, "name": "sid_tt", "path": "/", "sameSite": "Lax", "secure": false, "value": "a2a20c671ef2442a3df392962f0ec2f1"}, {"domain": ".coze.com", "expiry": 1721627986, "httpOnly": true, "name": "uid_tt_ss", "path": "/", "sameSite": "None", "secure": true, "value": "3a1c25ad7202bde4b83466141ea42edc3c1373cfa7f33083c64b4e67054a17e6"}, {"domain": ".coze.com", "expiry": 1721627956, "httpOnly": false, "name": "passport_csrf_token_default", "path": "/", "sameSite": "Lax", "secure": false, "value": "5ed4edb9c28c08c5b72fd60a16d76dfa"}, {"domain": "www.coze.com", "expiry": 1747979935, "httpOnly": false, "name": "i18next", "path": "/", "sameSite": "Strict", "secure": false, "value": "en"}, {"domain": ".coze.com", "expiry": 1747979986, "httpOnly": true, "name": "d_ticket", "path": "/", "sameSite": "Lax", "secure": false, "value": "a5495994bfe748c52edf6923a3ee9e48d5587"}, {"domain": "www.coze.com", "expiry": 1724219938, "httpOnly": false, "name": "msToken", "path": "/", "sameSite": "Lax", "secure": false, "value": "pF8uLf4lFGPlUJRMgK5VLXxuySbbG-9dHS_RdcU_PMbpktizpLhMrlV24so5ggHbS6_Y0fBOHROK2JIYTMtuAOkCJYbA0Afu_cATVjWJ6nzrbd71-RkYoDVVjF78Ias-"}, {"domain": "www.coze.com", "httpOnly": false, "name": "s_v_web_id", "path": "/", "sameSite": "Lax", "secure": false, "value": "verify_lwiuegov_hhImObyU_4OsN_4HUl_ArGE_OoMFSk9tbd1G"}]'''
# cookie_get = json.loads(cookie_get)
# driver = webdriver.Chrome(options=options)
# driver.get('https://www.coze.com/')
# cookies = cookie_get
# for cookie in cookies:
#     driver.add_cookie(cookie)
# driver.refresh()
# time.sleep(2)

# 循环获得
pool = CookiePool('com')
keys = pool.get_keys()
# keys = ['宁仁波']
cn_com = input('修改国内机器人请输入cn,否则输入com：')
if cn_com == 'cn':
    keys = [key for key in keys if 'cn:' in key]
else:
    keys = [key for key in keys if 'com:' in key]
# print(keys)
for key in keys:
    cookie_get = pool.from_key_get_cookie(key)

    driver = webdriver.Chrome(options=options)
    if cn_com == 'cn':
        driver.get('https://www.coze.cn/home')
    else:
        driver.get('https://www.coze.com/')
    cookies = cookie_get
    for cookie in cookies:
        driver.add_cookie(cookie)
    driver.refresh()
    # time.sleep(10)
    if cn_com == 'cn':
        time.sleep(3)
    else:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#root > div:nth-child(2) > section > aside > div > div > div > div.semi-navigation-header-list-outer > div.semi-navigation-list-wrapper > ul > div > div > div:nth-child(1) > li > div')))

    # 主页问答
    # driver.find_element(by=By.XPATH, value='//textarea[@class="rc-textarea textarea--oTXB57QK8bQN2BKYJ2Bi textarea--oTXB57QK8bQN2BKYJ2Bi"]').send_keys('你可以干什么')
    # driver.find_element(by=By.XPATH, value='//button[@class="semi-button semi-button-primary semi-button-size-small semi-button-borderless send-button--tqxhKsM_tI4KvPKo9dkI semi-button-with-icon semi-button-with-icon-only"]').click()

    # 切换到个人空间
    if cn_com == 'cn':
        driver.find_element(by=By.XPATH,
                            value='//div[@class="item-inner--EUdR7GaW9jMUZmdET6Te" and contains(.//text(), "个人空间")]').click()
        time.sleep(1)
        # 选择目标机器人
        driver.find_element(by=By.XPATH, value='//a[@class="card-link--qE9ervT2yNAKfT4J70Mn"]').click()
        time.sleep(2)
    else:
        driver.find_element(by=By.CSS_SELECTOR, value='#root > div:nth-child(2) > section > aside > div > div > div > div.semi-navigation-header-list-outer > div.semi-navigation-list-wrapper > ul > div > div > div:nth-child(1) > li > div').click()
        # time.sleep(1)
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#root > div:nth-child(2) > section > section > main > div > div > div.vuk1mP3tjtI8AZHu5ttC > div > div.coz-layout-content.A9l8HaYtxRUQbdaECyjA > div > div > div > a')))
        driver.find_element(by=By.CSS_SELECTOR, value='#root > div:nth-child(2) > section > section > main > div > div > div.vuk1mP3tjtI8AZHu5ttC > div > div.coz-layout-content.A9l8HaYtxRUQbdaECyjA > div > div > div > a').click()
        time.sleep(2)

    if_over = input('结束了吗？是请输入1:')
    if if_over == '1':
        driver.close()
        continue
    else:
        # # 测试修改模型
        # driver.find_element(By.CSS_SELECTOR, selector.get('com-change-css')).click()
        # time.sleep(0.5)
        # driver.find_element(By.XPATH, selector.get('com-model-list-xpath')).click()
        # time.sleep(0.5)
        # action = ActionChains(driver)
        # driver.find_element(By.XPATH, selector.get('com-gemini-1.5-pro-xpath')).click()
        # time.sleep(0.5)
        # length_element = driver.find_element(By.XPATH, selector.get('com-text-length-xpath'))
        # # 确认修改最大输出长度成功
        # while True:
        #     try:
        #         length_element.click()
        #         length_element.send_keys(Keys.CONTROL, 'a')
        #         length_element.send_keys(Keys.DELETE)
        #         length_element.send_keys('8192')
        #         time.sleep(1)
        #         WebDriverWait(driver, 5).until(
        #             EC.presence_of_element_located((By.XPATH, selector.get('com-text-length-ensure-1-xpath'))))
        #         break
        #     except:
        #         continue
        # time.sleep(1)
        # action.send_keys(Keys.ESCAPE).perform()
        # time.sleep(0.5)
        # logger.info('第二次重新生成，使用Gemini-pro')
        # print('睡眠开始')
        # time.sleep(100)

        driver.close()
        break




# 输入框
# element = driver.find_element(by=By.XPATH, value='//textarea[@class="rc-textarea textarea--oTXB57QK8bQN2BKYJ2Bi textarea--oTXB57QK8bQN2BKYJ2Bi"]')  #.send_keys('你可以干什么')
# content = '今天天气怎么样'
# pyperclip.copy(content)
# element.send_keys(Keys.CONTROL, 'v')
# time.sleep(2)
# 发送
# driver.find_element(by=By.XPATH, value='//div[@class="textarea-actions-right--vr4WgM3FUuUicP3kJDOU"]').click()
#
# try:
#     WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#root > div:nth-child(2) > div > div > div > div > div.container--aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.wrapper-single--UMf9npeM8cVkDi0CDqZ0 > div.message-area--TH9DlQU1qwg_KGXdDYzk > div > div.scroll-view--R_WS6aCLs2gN7PUhpDB0.scroll-view--JlYYJX7uOFwGV6INj0ng > div > div > div.wrapper--nIVxVV6ZU7gCM5i4VQIL.message-group-wrapper > div > div > div:nth-child(1) > div > div > div > div > div.chat-uikit-message-box-container__message > div > div.chat-uikit-message-box-container__message__message-box__footer > div > div.message-info-text--tTSrEd1mQwEgF4_szmBb > div:nth-child(3) > div > div')))
#     print('已经出现元素')
# except:
#     print('超时或者未出现等待元素')
# content = driver.find_element(by=By.XPATH, value='//div[@class="auto-hide-last-sibling-br paragraph_4183d"]').text
# if isinstance(content, list):
#     content = ''.join(content)
# content = json.loads(content)
# print(content)




