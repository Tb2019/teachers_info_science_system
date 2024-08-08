from selenium import webdriver
from selenium.webdriver.common.by import By
from cookie_pool import CookiePool
import time
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

options = webdriver.ChromeOptions()
# options.binary_location = r'D:\Software\chrome-win64\chrome.exe'
options.add_experimental_option('detach', True)

# options.add_argument('--headless')
# options.add_argument('--no-sandbox')
# options.add_argument('--disable-dev-shm-usage')
# user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36'
# options.add_argument(f'user-agent={user_agent}')
# options.add_argument("--window-size=1920,1080")

# 过检测
# options.add_experimental_option("excludeSwitches", ["enable-automation"])
# options.add_experimental_option('useAutomationExtension', False)

class GptParser:
    def __init__(self, cn_com):
        self.pool = CookiePool(cn_com)
        self.cn_com = cn_com

    def init_driver(self):
        cookies = self.pool.random_get_cookie()
        driver = webdriver.Chrome(options=options)

        # 过检测
        # driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
        #     "source": """
        #     Object.defineProperty(navigator, 'webdriver', {
        #       get: () => undefined
        #     })
        #   """
        # })
        # driver.execute_cdp_cmd("Network.enable", {})
        # driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {"User-Agent": "browser1"}})
        if self.cn_com == 'cn':
            driver.get('https://www.coze.cn/home')
        else:
            driver.get('https://www.coze.com/')
        for cookie in cookies:
            driver.add_cookie(cookie)
        driver.refresh()

        # time.sleep(2)
        while True:
            try:
                if self.cn_com == 'cn':
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, '//div[@class="item-inner--EUdR7GaW9jMUZmdET6Te" and contains(.//text(), "个人空间")]')))
                else:
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#root > div:nth-child(2) > section > aside > div > div > div > div.semi-navigation-header-list-outer > div.semi-navigation-list-wrapper > ul > div > div > div:nth-child(1) > li > div')))
                break
            except:
                driver.refresh()
                # time.sleep(2)
                continue

        # 切换到个人空间
        if self.cn_com == 'cn':
            driver.find_element(by=By.XPATH,
                            value='//div[@class="item-inner--EUdR7GaW9jMUZmdET6Te" and contains(.//text(), "个人空间")]').click()
        else:
            driver.find_element(By.CSS_SELECTOR, '#root > div:nth-child(2) > section > aside > div > div > div > div.semi-navigation-header-list-outer > div.semi-navigation-list-wrapper > ul > div > div > div:nth-child(1) > li > div').click()
            try:
                # 接受cookies
                time.sleep(1)
                # 2024/5/28以前
                # driver.execute_script('document.querySelector("body > cookie-banner").shadowRoot.querySelector("div > div.button-wrapper > banner-button:nth-child(3)").shadowRoot.querySelector("button").click()')
                # 2024/5/28以后
                driver.find_element(By.CSS_SELECTOR, 'body > div._Wn1JRJHd8jh44aWsWKh > div > button').click()
            except:
                print('未发现cookie元素')
                pass
        # time.sleep(1)
        while True:
            # 等待目标机器人出现
            try:
                if self.cn_com == 'cn':
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, '//a[@class="card-link--qE9ervT2yNAKfT4J70Mn"]')))
                else:
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, '//a[@class="qE9ervT2yNAKfT4J70Mn"]')))
                break
            except:
                driver.refresh()
                # time.sleep(2)
                continue
        # 选择目标机器人
        if self.cn_com == 'cn':
            driver.find_element(by=By.XPATH, value='//a[@class="card-link--qE9ervT2yNAKfT4J70Mn"]').click()
        else:
            driver.find_element(by=By.XPATH, value='//a[@class="qE9ervT2yNAKfT4J70Mn"]').click()
        # time.sleep(1)
        while True:
            try:
                if self.cn_com == 'cn':
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, '//textarea[@class="rc-textarea textarea--oTXB57QK8bQN2BKYJ2Bi textarea--oTXB57QK8bQN2BKYJ2Bi"]')))
                else:
                    WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, '//textarea[@class="rc-textarea oTXB57QK8bQN2BKYJ2Bi oTXB57QK8bQN2BKYJ2Bi"]')))
                break
            except:
                driver.refresh()
                # time.sleep(2)
                continue

        return driver
