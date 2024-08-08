# -*- coding: utf-8 -*-
import csv
import os
import re
import time

from selenium import webdriver
import pandas as pd
from loguru import logger
from crawler import ReCrawler
from utils import csv_header, result_dict_2_df, csv_2_df, drop_duplicate_collage, truncate_table, df2mysql, \
    local_engine, sf_engine, save_as_json

options = webdriver.ChromeOptions()
options.add_experimental_option('detach', True)


institute_name = '物理研究所'
laboratory_name = '应用物理中心'
institute_id = 1
laboratory_id = 19
img_url_head = None
partition_num = '010'
start_urls = [
                # 'https://www.iop.cas.cn/rcjy/zgjgwry/',
                'https://www.iop.cas.cn/rcjy/tpyjy/',
                # 'https://www.iop.cas.cn/rcjy/yjdwfgj/'
                # ''
              ]

# a_s_xpath_str = '//*[@id="yjy_content"]/table/tbody/tr[34]//a'
a_s_xpath_str = '//*[@id="yjy_content"]/table/tbody/tr[30]//a'
target_div_xpath_str = '//div[@id="news_box"]'

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

class Specialspider(ReCrawler):
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
        driver.close()
        return detail_pages

    def run(self):
        if self.api:
            self.selenium_gpt = False

        file = None
        if self.selenium_gpt or self.api:
            if os.path.exists(f'{self.laboratory_id}.csv'):
                file = open(f'{self.laboratory_id}.csv', mode='a', newline='', encoding='utf-8')
                self.writter = csv.writer(file)
            else:
                file = open(f'{self.laboratory_id}.csv', mode='a', newline='', encoding='utf-8')
                self.writter = csv.writer(file)
                self.writter.writerow(csv_header)

        self.result_df = pd.DataFrame()
        self.gpt_cant = []
        self.api_cant = []

        for url in self.start_urls:
            self.driver = webdriver.Chrome(options=options)
            self.driver.get(url)
            time.sleep(1)
            index_page = self.driver.page_source
            self.driver.close()
            index_result = self.parse_index(index_page, url)

            detail_pages = self.get_detail_page(index_result)
            if self.api:
                print('接入api')
                mid_results = [self.parse_detail(detail_page, url) for detail_page in detail_pages]
                self.result_df = self.parse_by_api(mid_results)
            elif self.selenium_gpt:
                print('接入gpt')
                mid_results = [self.parse_detail(detail_page, url) for detail_page in detail_pages]
                self.parse_by_gpt(mid_results)
                print('*** too long to use gpt ***', self.gpt_cant)

            else:
                print('直接解析')
                for detail_page in detail_pages:
                    # print('detail', detail_page)
                    result = self.parse_detail(detail_page, url)
                    # result = mid_result
                    print(result)
                    # 防止网页本身为空
                    if result:
                        self.result_df = result_dict_2_df(self.result_df, result)
                    else:
                        continue
        if file:
            file.close()

        # 去重
        # result_df.drop_duplicates(inplace=True, keep='first', subset=['name', 'email'])
        result_df = drop_duplicate_collage(self.result_df)

        # 保存至数据库
        if self.api:
            if not self.api_cant:
                try:
                    result_df = csv_2_df(f'./{self.laboratory_id}.csv')
                    result_df = drop_duplicate_collage(result_df)
                    if self.save2target == 'no':
                        pass
                    elif self.save2target == 'test':
                        truncate_table(host='localhost', user='root', password='123456', database='alpha_search',
                                       port=3306,
                                       table_name='search_teacher_test')
                        df2mysql(engine=local_engine, df=result_df, table_name='search_teacher_test')
                        # elif self.save2target == 'local':
                        #     df2mysql(engine=local_engine, df=result_df, table_name='search_teacher')
                        logger.info('数据保存成功')
                    elif self.save2target == 'target':
                        df2mysql(engine=sf_engine, df=result_df, table_name='search_institute_user')
                        save_as_json(result_df, self.institute_name, self.laboratory_name)
                        logger.info('数据保存成功')
                    # 删除csv
                    # os.remove(f'./{self.college_id}.csv')
                    # logger.info('csv文件已删除')
                except:
                    wait = input('数据有问题，请在excel中修改，修改完成后请输入1：')
                    if wait == '1':
                        try:
                            result_df = csv_2_df(f'./{self.laboratory_id}.csv')
                            result_df = drop_duplicate_collage(result_df)
                            print(result_df)
                            logger.info('再次保存中...')
                            if self.save2target == 'no':
                                pass
                            elif self.save2target == 'test':
                                truncate_table(host='localhost', user='root', password='123456',
                                               database='alpha_search',
                                               port=3306,
                                               table_name='search_teacher_test')
                                df2mysql(engine=local_engine, df=result_df, table_name='search_teacher_test')
                                logger.info('数据保存成功')
                            # elif self.save2target == 'local':
                            #     df2mysql(engine=local_engine, df=result_df, table_name='search_teacher')
                            elif self.save2target == 'target':
                                df2mysql(engine=sf_engine, df=result_df, table_name='search_institute_user')
                                save_as_json(result_df, self.institute_name, self.laboratory_name)
                                logger.info('数据保存成功')
                            # 删除csv
                            # os.remove(f'./{self.college_id}.csv')
                            # logger.info('csv文件已删除')
                        except:
                            logger.warning('文件仍有错误，请修改，之后手动存储，并删除csv文件')
            else:
                logger.info('存在未能解析的数据，请手动补充数据之后再手动存储数据')
        # gpt改用csv文件存储数据至数据库
        elif self.selenium_gpt:
            if not self.gpt_cant:
                try:
                    result_df = csv_2_df(f'./{self.laboratory_id}.csv')
                    result_df = drop_duplicate_collage(result_df)
                    if self.save2target == 'no':
                        pass
                    elif self.save2target == 'test':
                        truncate_table(host='localhost', user='root', password='123456', database='alpha_search', port=3306,
                                       table_name='search_teacher_test')
                        df2mysql(engine=local_engine, df=result_df, table_name='search_teacher_test')
                    # elif self.save2target == 'local':
                    #     df2mysql(engine=local_engine, df=result_df, table_name='search_teacher')
                        logger.info('数据保存成功')
                    elif self.save2target == 'target':
                        df2mysql(engine=sf_engine, df=result_df, table_name='search_institute_user')
                        save_as_json(result_df, self.institute_name, self.laboratory_name)
                        logger.info('数据保存成功')
                    # 删除csv
                    # os.remove(f'./{self.college_id}.csv')
                    # logger.info('csv文件已删除')
                except:
                    wait = input('数据有问题，请在excel中修改，修改完成后请输入1：')
                    if wait == '1':
                        try:
                            result_df = csv_2_df(f'./{self.laboratory_id}.csv')
                            result_df = drop_duplicate_collage(result_df)
                            print(result_df)
                            logger.info('再次保存中...')
                            if self.save2target == 'no':
                                pass
                            elif self.save2target == 'test':
                                truncate_table(host='localhost', user='root', password='123456', database='alpha_search',
                                               port=3306,
                                               table_name='search_teacher_test')
                                df2mysql(engine=local_engine, df=result_df, table_name='search_teacher_test')
                                logger.info('数据保存成功')
                            # elif self.save2target == 'local':
                            #     df2mysql(engine=local_engine, df=result_df, table_name='search_teacher')
                            elif self.save2target == 'target':
                                df2mysql(engine=sf_engine, df=result_df, table_name='search_institute_user')
                                save_as_json(result_df, self.institute_name, self.laboratory_name)
                                logger.info('数据保存成功')
                            # 删除csv
                            # os.remove(f'./{self.college_id}.csv')
                            # logger.info('csv文件已删除')
                        except:
                            logger.warning('文件仍有错误，请修改，之后手动存储，并删除csv文件')
            else:
                logger.info('存在未能解析的数据，请手动补充数据之后再手动存储数据')
        else:
            if self.save2target == 'no':
                pass
            elif self.save2target == 'test':
                truncate_table(host='localhost', user='root', password='123456', database='alpha_search', port=3306, table_name='search_teacher_test')
                df2mysql(engine=local_engine, df=result_df, table_name='search_teacher_test')
            # elif self.save2target == 'local':
            #     df2mysql(engine=local_engine, df=result_df, table_name='search_teacher')
            elif self.save2target == 'target':
                df2mysql(engine=sf_engine, df=result_df, table_name='search_institute_user')
                save_as_json(result_df, self.institute_name, self.laboratory_name)
            # elif self.save2target == 'simple':
            #     df2mysql(engine=sf_engine, df=result_df, table_name='search_teacher_simple')
            #     # 保存成json至本地
            #     save_as_json(result_df, self.school_name, self.college_name)

            # update_rel_table(schoolid=self.school_id)
            # 删除csv
            # os.remove(f'./{self.college_id}.csv')
            # logger.info('csv文件已删除')


spider = Specialspider(
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


