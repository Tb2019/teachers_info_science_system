from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import json
from loguru import logger
import redis
import random
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = webdriver.ChromeOptions()
options.add_experimental_option('detach', True)

class CookiePool:
    def __init__(self, cn_com: str, host='localhost', port=6379, password='123456', db=1):
        self.cn_com = cn_com
        # self.phone_num = input('请输入你的电话号码:')
        self.db = redis.StrictRedis(host=host, port=port, password=password, db=db, decode_responses=True)
        # self.key_list = ['myself', '乐放文', '刘宇', '吴蒙恩', '张新正', '张涛', '徐金花', '王宇', '程修杰', '鲍康俊']

    def update_cookie(self, ):
        self.user_name = input('请输入你的姓名:')
        self.driver = webdriver.Chrome(options=options)
        if self.cn_com == 'cn':
            self.driver.get('https://www.coze.cn/home')

            time.sleep(1)
            self.driver.find_element(by=By.XPATH, value='//span[@class="semi-checkbox-inner-display"]').click()  # 同意协议
            self.driver.find_element(by=By.XPATH, value='//span[@class="semi-button-content"]').click()  # 选择抖音登录
            # self.driver.find_element(by=By.XPATH, value='//input[@class="semi-input semi-input-default"]').send_keys(self.phone_num)  # 填写号码
            # self.driver.find_element(by=By.XPATH, value='//span[@class="semi-button-content"]').click()  # 提交号码登录

            logger.info('请扫码登录')
            logger.info('等待扫码中...')

            while True:  # 不固定等待时间
                try:
                    sucess_index = self.driver.find_element(by=By.XPATH, value='//div[@class="logo--ygHzgHcQawt_wjEH3kdX facade--RXmt6TFgUUaz4nGd61fY"]')
                except:
                    sucess_index = None
                # try:
                #     success_bot = self.driver.find_element(by=By.XPATH,
                #                                       value='//div[@class="left-sheet-config--PLf9q929mqs4ZlidOgkc"]')
                # except:
                #     success_bot = None
                # if sucess_index or success_bot:
                if sucess_index:
                    logger.info('登录成功')
                    break
                else:
                    time.sleep(2)
        else:
            self.driver.get('https://www.coze.com/')
            time.sleep(2)
            # try:
            #     self.driver.find_element(by=By.CSS_SELECTOR, value='div > div.button-wrapper > banner-button:nth-child(3)').click()
            # except:
            #     pass
            self.driver.find_element(by=By.CSS_SELECTOR,
                                value='#root > div:nth-child(2) > div > div > div.GjMJhecDH0IAXY5UglqL > div.semi-row.DBHgS49fQaSdIcqTWN0X.aj87zumsXsAG2eYfYoAc.px-4.md\:px-6.lg\:px-5 > div.semi-col.semi-col-12.fr68NbKoRPRM9DUOOqkQ > div > button').click()
            phone_num = input('输入电话号码：')
            self.driver.find_element(by=By.CSS_SELECTOR,
                                value='#root > div:nth-child(2) > div > div > div.PcNBZFM8YkYYLzqkBhP7 > div > div.s2nbARByiH8yjwzcqFYA > span > div.semi-input-wrapper.LDr0_dN0WYQnbHJpewjx.WYouJkFwBBco8KBy87Xe.semi-input-wrapper-default > input').send_keys(phone_num)
            self.driver.find_element(by=By.CSS_SELECTOR,
                                value='#root > div:nth-child(2) > div > div > div.PcNBZFM8YkYYLzqkBhP7 > div > button.semi-button.semi-button-primary.zgSTbY8Fb3UnrxBlBag1.yJDwteqjbdkspQ0V7XOl.eg1Na7tSNArMfjcoOnY9').click()
            while True:
                try:
                    WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#root > div:nth-child(2) > section > section > main > div > div > div > div.EcePERynzs86olVhIBk6.px-4.md\:px-6 > div > div.semi-col.semi-col-16')))
                    logger.info('登录成功')
                    break
                except:
                    continue

        self.cookie_get = json.dumps(self.driver.get_cookies())
        print(self.cookie_get)
        self.save_cookie(self.cookie_get)

        if_over = input('结束请输入1：')
        if if_over == '1':
            self.driver.delete_all_cookies()
            time.sleep(1)
            self.driver.refresh()
            self.driver.close()

    def save_cookie(self, cookie):
        if self.cn_com == 'cn':
            key = 'cn:' + self.user_name
        else:
            key = 'com:' + self.user_name
            self.db.set(key, cookie)
        logger.info('cookie保存成功')

    def random_get_cookie(self):
        while True:
            key = self.db.randomkey()
            if (self.cn_com == 'cn' and 'cn:' in key) or (self.cn_com == 'com' and 'com:' in key):
                break
            else:
                continue
        # print(key)
        value = self.db.get(key)
        value = json.loads(value)  # 转化为对象
        return value

    def from_key_get_cookie(self, key):
        if ':' in key:
            value = self.db.get(key)
        else:
            value = self.db.get(self.cn_com + ':' + key)
        value = json.loads(value)  # 转化为对象
        return value

    def get_keys(self):
        keys = self.db.keys()
        return keys


if __name__ == '__main__':
    pool = CookiePool(cn_com='com')
    pool.update_cookie()
    # keys = pool.get_keys()
    # print(keys)
    # value = pool.random_get_cookie()
    # print(value)
