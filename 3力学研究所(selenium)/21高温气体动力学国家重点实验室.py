# -*- coding: utf-8 -*-
import re
import time

from crawler import ReCrawler
from selenium import webdriver

institute_name = '力学研究所'
laboratory_name = '高温气体动力学国家重点实验室'
institute_id = 3
laboratory_id = 21
img_url_head = None
partition_num = '010'
start_urls = [
                'http://lhd.imech.cas.cn/rcdw/zgj/201307/t20130723_116745.html',
                'http://lhd.imech.cas.cn/rcdw/fyjy/202401/t20240131_769428.html',
              ]

a_s_xpath_str = '//div[@class="Custom_UnionStyle"]//table//tr[1]/td//a|//div[@class="TRS_Editor"]//table//tr/td//a'
target_div_xpath_str = '//div[@class="content-info"]|//div[@id="main-content"]'

options = webdriver.ChromeOptions()
options.add_experimental_option('detach', False)
# # 电话
# phone_xpath = None

# # 邮箱
# email_xpath = None

# # 职称
# job_title_xpath = None

# # 学位
# qualification_xpath = None

# # 图片
# img_xpath = None

# 研究方向
# directions_pattern_list = [
#                             re.compile(r'', re.S),
#                             re.compile(r'', re.S)
#                           ]
# directions_xpath = None

# 专利
# patent_pattern_list = [
#                                     re.compile(r'', re.S),
#                                     re.compile(r'', re.S)
#                                 ]
# patent_xpath = None

# # 科研项目
# project_pattern_list = [
#                                     re.compile(r'', re.S),
#                                     re.compile(r'', re.S)
#                                 ]
# project_xpath = None

# # 奖励/荣誉
# award_pattern_list = [
#                                     re.compile(r'', re.S),
#                                     re.compile(r'', re.S)
#                                 ]
# award_xpath = None

# # 简介
# abstracts_pattern_list = [
#                             re.compile(r'', re.S),
#                             re.compile(r'', re.S)
#                           ]
# abstracts_xpath = None

# # 办公地点
# office_address_pattern_list = [
#                                 re.compile(r'', re.S)
#                               ]
# office_address_xpath = None

# # 在职信息
# job_information_pattern_list = [
#                                 re.compile(r'', re.S)
#                               ]
# job_information_xpath = None

# # 主要任职
# responsibilities_pattern_list = [
#                                     re.compile(r'', re.S),
#                                     re.compile(r'', re.S)
#                                 ]
# responsibilities_xpath = None

# # 教育经历
# education_experience_pattern_list = [
#                                     re.compile(r'', re.S),
#                                     re.compile(r'', re.S)
#                                 ]
# education_experience_xpath = None


# # 工作经历
# work_experience_pattern_list = [
#                                     re.compile(r'', re.S),
#                                     re.compile(r'', re.S)
#                                 ]
# work_experience_xpath = None

# # 社会兼职
# social_job_pattern_list = [
#                                     re.compile(r'', re.S),
#                                     re.compile(r'', re.S)
#                                 ]
# social_job_xpath = None

class SpecialSpider(ReCrawler):
    def get_detail_page(self, index_result):
        # 方法重写时引入包
        global asyncio, aiohttp, get_response_async
        if not globals().get('asyncio'):
            import asyncio
        if not globals().get('aiohttp'):
            import aiohttp
        if not globals().get('get_response_async'):
            from utils import get_response_async

        # loop = asyncio.get_event_loop()
        # session = aiohttp.ClientSession()
        # tasks = [get_response_async(url, session, name=name) for name, url in index_result if len(name) >= 2]
        # detail_pages = loop.run_until_complete(asyncio.gather(*tasks))
        # session.connector.close()
        detail_pages = []
        driver = webdriver.Chrome(options=options)
        for name, url in index_result:
            if len(name) >= 2:
                driver.get(url)
                time.sleep(2)
                page = driver.page_source
                detail_pages.append((page, url, [('name', name)]))
                # if name == '范学军':
                #     print(page)
        driver.close()
        return detail_pages

spider = SpecialSpider(
                   institute_name=institute_name,
                   laboratory_name=laboratory_name,
                   partition_num=partition_num,
                   institute_id=institute_id,
                   laboratory_id=laboratory_id,
                   name_filter_re=r'简介',
                   start_urls=start_urls,
                   img_url_head=img_url_head,
                   a_s_xpath_str=a_s_xpath_str,
                   target_div_xpath_str=target_div_xpath_str,

                   save2target='no',
                   selenium_gpt=False,
                   cn_com='',
                   api=True,
                   # directions_pattern_list=directions_pattern_list,
                   # abstracts_pattern_list=abstracts_pattern_list,
                   # office_address_pattern_list=office_address_pattern_list,
                   # job_information_pattern_list=job_information_pattern_list,
                   # responsibilities_pattern_list=responsibilities_pattern_list,
                   # education_experience_pattern_list=education_experience_pattern_list,
                   # work_experience_pattern_list=work_experience_pattern_list,
                   # patent_pattern_list=patent_pattern_list,
                   # project_pattern_list=project_pattern_list,
                   # award_pattern_list=award_pattern_list,
                   # paper_pattern_list=paper_pattern_list,
                   # social_job_pattern_list=social_job_pattern_list,
                   # email_pattern=re.compile(r'[a-zA-Z0-9._-]+(?:@|\(at\)|\(AT\)|\[at]|\[AT])(?=.{1,10}(?:\.com|\.cn|\.net))[a-zA-Z0-9_-]+\.[0-9a-zA-Z._-]+',re.S),

                   # phone_xpath=phone_xpath,
                   # email_xpath=email_xpath,
                   # job_title_xpath=job_title_xpath,
                   # qualification_xpath=qualification_xpath,
                   # directions_xpath=directions_xpath,
                   # abstracts_xpath=abstracts_xpath,
                   # office_address_xpath=office_address_xpath,
                   # job_information_xpath=job_information_xpath,
                   # responsibilities_xpath=responsibilities_xpath,
                   # education_experience_xpath=education_experience_xpath,
                   # work_experience_xpath=work_experience_xpath,
                   # patent_xpath=patent_xpath,
                   # project_xpath=project_xpath,
                   # award_xpath=award_xpath,
                   # paper_xpath=paper_xpath,
                   # social_job_xpath=social_job_xpath,
                   # img_xpath=img_xpath,
                   )

spider.run()


