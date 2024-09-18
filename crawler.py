import csv
import json
import os
import time
import demjson3
from bs4 import BeautifulSoup
from lxml import etree
from lxml.html import fromstring, tostring
import asyncio
import aiohttp
from retry import retry
import pyperclip
import re
import pandas as pd
from loguru import logger
from urllib import parse
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
# from drop_duplicate_school import update_rel_table
from utils import get_response, get_response_async, result_dict_2_df, df2mysql, local_engine, sf_engine, \
    drop_duplicate_collage, save_as_json, truncate_table, api_parse, clean_phone, replace_quotes_in_text, csv_header, \
    csv_2_df, selector, change_model, restore_model
from gptparser import GptParser


class ReCrawler:
    def __init__(self,
                 partition_num: str,
                 institute_name: str,
                 laboratory_name: str,
                 institute_id: int,
                 laboratory_id: int,
                 cn_com,
                 start_urls: list,
                 a_s_xpath_str: [str, None],
                 target_div_xpath_str: [str, None],
                 name_filter_re=r'\s*',
                 img_url_head: str = None,
                 directions_pattern_list: [None, list] = None,
                 abstracts_pattern_list: [None, list] = None,
                 office_address_pattern_list: [None, list] = None,
                 job_information_pattern_list: [None, list] = None,
                 responsibilities_pattern_list: [None, list] = None,
                 education_experience_pattern_list: [None, list] = None,
                 work_experience_pattern_list: [None, list] = None,
                 patent_pattern_list: [None, list] = None,
                 project_pattern_list: [None, list] = None,
                 award_pattern_list: [None, list] = None,
                 paper_pattern_list: [None, list] = None,
                 social_job_pattern_list: [None, list] = None,
                 job_title_pattern=re.compile(r'院士|教授|副教授|讲师|副?研究员|正?(?:高级)?(?:助理)?(?:工程师|实验师)|副?主任医师', re.S),
                 phone_pattern=re.compile(r'(?<!\d)1\d{10}(?!\d)|(?<!\d)\d{3,4}(?:-|——)\d{7,8}(?!\d)', re.S),
                 # email_pattern=re.compile(r'[a-zA-Z0-9._-]+@[a-zA-Z0-9_-]+\.[a-zA-Z0-9._-]+', re.S),
                 # email_pattern=re.compile(r'[a-zA-Z0-9._-]+(?:@|\(at\)|\(AT\)|\[at]|\[AT])(?=.{1,20}(?:\.com|\.cn))[a-zA-Z0-9_-]+\.[a-zA-Z._-]+', re.S),
                 # email_pattern=re.compile(r'[a-zA-Z0-9._-]+(?:@|\(at\)|\(AT\)|\[at]|\[AT])(?=.{1,10}(?:\.com|\.cn))[a-zA-Z0-9_-]+\.[0-9a-zA-Z._-]+',re.S),
                 email_pattern=re.compile(r'[a-zA-Z0-9._-]+(?:@|\(at\)|\(AT\)|\[at]|\[AT])[a-zA-Z0-9_-]+\.[a-zA-Z._-]+', re.S),
                 qualification_pattern=re.compile(r'博士|硕士|学士', re.S),
                 save2target='test',
                 api=False,
                 selenium_gpt=False,
                 # is_regular=False,
                 phone_xpath: [None, str] = None,
                 email_xpath: [None, str] = None,
                 job_title_xpath: [None, str] = None,
                 qualification_xpath: [None, str] = None,
                 directions_xpath: [None, str] = None,
                 abstracts_xpath: [None, str] = None,
                 office_address_xpath: [None, str] = None,
                 job_information_xpath: [None, str] = None,
                 responsibilities_xpath: [None, str] = None,
                 education_experience_xpath: [None, str] = None,
                 work_experience_xpath: [None, str] = None,
                 patent_xpath: [None, str] = None,
                 project_xpath: [None, str] = None,
                 award_xpath: [None, str] = None,
                 paper_xpath: [None, str] = None,
                 social_job_xpath: [None, str] = None,
                 img_xpath: [None, str] = None
                 ):
        """

        :param institute_id: 研究所id
        :param laboratory_id: 实验室id
        :param start_urls:
        :param a_s_xpath_str: 列表页超链接a标签的xpath路径
        :param target_div_xpath_str: 包含全部教师信息的div标签xpath路径
        :param directions_pattern_list:
        :param abstracts_pattern_list:
        :param name_filter_re: 过滤列表页提取到的姓名，默认过滤空白字符
        :param office_address_pattern_list:
        :param job_information_pattern_list:
        :param responsibilities_pattern_list:
        :param job_title_pattern:
        :param phone_pattern:
        :param email_pattern:
        :param qualification_pattern:
        :param save2target: 默认false，不保存至目标表；true则保存至before表
        :return:
        """
        self.partition_num = partition_num
        self.institute_name = institute_name
        self.laboratory_name = laboratory_name
        self.institute_id = institute_id
        self.laboratory_id = laboratory_id
        self.start_urls = start_urls

        self.a_s_xpath_str = a_s_xpath_str
        self.target_div_xpath_str = target_div_xpath_str

        self.name_filter_re = name_filter_re

        self.directions_pattern_list = directions_pattern_list
        self.abstracts_pattern_list = abstracts_pattern_list
        self.office_address_pattern_list = office_address_pattern_list
        self.education_experience_pattern_list = education_experience_pattern_list
        self.work_experience_pattern_list = work_experience_pattern_list
        self.patent_pattern_list = patent_pattern_list
        self.project_pattern_list = project_pattern_list
        self.award_pattern_list = award_pattern_list
        self.paper_pattern_list = paper_pattern_list
        self.social_job_pattern_list = social_job_pattern_list

        self.job_title_pattern = job_title_pattern
        self.phone_pattern = phone_pattern
        self.email_pattern = email_pattern
        self.qualification_pattern = qualification_pattern
        self.job_information_pattern_list = job_information_pattern_list
        self.responsibilities_pattern_list = responsibilities_pattern_list

        self.save2target = save2target

        self.api = api
        self.selenium_gpt = selenium_gpt
        self.cn_com = cn_com

        # self.is_regular = is_regular

        self.img_url_head = img_url_head
        self.phone_xpath = phone_xpath
        self.email_xpath = email_xpath
        self.job_title_xpath = job_title_xpath
        self.qualification_xpath = qualification_xpath
        self.directions_xpath = directions_xpath
        self.abstracts_xpath = abstracts_xpath
        self.office_address_xpath = office_address_xpath
        self.job_information_xpath = job_information_xpath
        self.responsibilities_xpath = responsibilities_xpath
        self.education_experience_xpath = education_experience_xpath
        self.work_experience_xpath = work_experience_xpath
        self.patent_xpath = patent_xpath
        self.project_xpath = project_xpath
        self.award_xpath = award_xpath
        self.paper_xpath = paper_xpath
        self.social_job_xpath = social_job_xpath
        self.img_xpath = img_xpath

    def parse_index(self, index_page, url):
        page = etree.HTML(index_page)
        a_s = page.xpath(self.a_s_xpath_str)
        for a in a_s:
            name = a.xpath('.//text()')
            if name:
                name = ''.join(name)
                if not re.match(r'[A-Za-z\s]*$', name, re.S):  # 中文名替换全部空格，英文名保留中间空格
                    name = re.sub(r'\s*', '', name)
                name = re.sub(r'^\s*(\w.*?\w)\s*$', r'\1', name)  # 去除姓名前后空白字符
                name = re.sub(self.name_filter_re, '', name)
                try:
                    link = a.xpath('./@href')[0]
                    if link in ('#',):
                        continue
                    link = parse.urljoin(url, link)
                except:
                    continue
            else:
                print('未解析到name，请检查：a_s_xpath_str')
                continue
            yield name, link

    def get_detail_page(self, index_result):
        loop = asyncio.get_event_loop()
        session = aiohttp.ClientSession()
        tasks = [get_response_async(url, session, name=name) for name, url in index_result if len(name) >= 2]
        detail_pages = loop.run_until_complete(asyncio.gather(*tasks))
        session.connector.close()
        return detail_pages

    def parse_detail(self, detail_page, url):
        if detail_page:
            page, origin_url, info_s = detail_page
            page_tree = etree.HTML(page)
            try:
                target_div = page_tree.xpath(self.target_div_xpath_str)[0]
            except:
                print('未发现内容标签', origin_url)
                return None
            all_content = ''.join(
                [re.sub(r'\s*', '', i) for i in target_div.xpath('.//text()') if re.sub(r'\s+', '', i)]
            )
            all_content = re.sub(r'-{5,}', '', all_content)

            if self.api or self.selenium_gpt:
                # 替代引号
                replace_quotes_in_text(target_div)
                content_with_label = tostring(target_div, encoding='utf-8').decode('utf-8')
                # 去除base64编码的图片标签
                soup = BeautifulSoup(content_with_label, 'html.parser')
                base64_imgs = soup.find_all('img', src=re.compile(r'base64'))
                for img in base64_imgs:
                    img.decompose()
                # 去除除img标签外的所有标签属性
                for tag in soup.find_all(True):
                    if tag.name != 'img':
                        tag.attrs = {}
                content_with_label = str(soup)
                # 去掉标签之间空白字符
                content_with_label = re.sub(r'>\s*<', r'><', content_with_label)


            # 姓名
            name = info_s[0][1]

            # 学校id
            institute_id = self.institute_id

            # 学院id
            laboratory_id = self.laboratory_id

            # 图片
            full_src = None
            if self.img_xpath:
                try:
                    img_list = target_div.xpath(self.img_xpath)
                    src = ','.join(img_list) if ','.join(img_list) else None
                    if src:
                        if self.img_url_head:
                            full_src = parse.urljoin(self.img_url_head, src)
                        else:
                            full_src = parse.urljoin(origin_url, src)
                except:
                    print('img_xpath错误')
            else:
                try:
                    src = target_div.xpath('.//img[@src!=""]/@src')[0]
                    src = re.sub(r'^.*?base64.*$', '', src)
                    if src:
                        if self.img_url_head:
                            full_src = parse.urljoin(self.img_url_head, src)
                        else:
                            full_src = parse.urljoin(origin_url, src)
                except:
                    pass
            # full_src = parse.urljoin(url, src)

        # if not self.api:  # 不使用第三方api解析
            # 电话
            phone = None
            if self.phone_xpath:
                try:
                    phone_list = target_div.xpath(self.phone_xpath)
                    phone = ','.join(phone_list) if ','.join(phone_list) else None
                except:
                    print('phone_xpath错误')
            else:
                try:
                    phone_list = list(set(self.phone_pattern.findall(all_content)))
                    if len(phone_list) > 1:
                        phone = ','.join(phone_list)
                    else:
                        phone = phone_list[0]
                    phone = re.sub(r'——', '-', phone)
                except:
                    pass
            # phone = re.sub(r'——', '-', phone)

            # 邮箱
            email = None
            if self.email_xpath:
                try:
                    email_list = target_div.xpath(self.email_xpath)
                    email = ','.join(email_list) if ','.join(email_list) else None
                except:
                    print('email_xpath错误')
            else:
                try:
                    email_list = list(set(self.email_pattern.findall(all_content)))
                    if len(email_list) > 1:
                        email = ','.join(email_list)
                    else:
                        email = email_list[0]
                    # email = re.sub(r'(?:\(at\)|\(AT\)|\[at]|\[AT])', '@', email)
                    email = re.sub(r'\(?\s?at\s?\)?|\(?\s?AT\s?\)?|\[?\s?at\s?]?|\[?\s?AT\s?]?', '@', email)

                except:
                    pass
            # email = re.sub(r'\(?\s?at\s?\)?|\(?\s?AT\s?\)?|\[?\s?at\s?]?|\[?\s?AT\s?]?', '@', email)

            # 职称
            job_title = None
            if self.job_title_xpath:
                try:
                    job_title_list = target_div.xpath(self.job_title_xpath)
                    job_title = ','.join(job_title_list) if ','.join(job_title_list) else None
                except:
                    print('job_title_xpath错误')
            else:
                try:
                    job_title = self.job_title_pattern.findall(all_content)[0]
                except:
                    pass

            # 个人简介
            abstracts = None
            if self.abstracts_xpath:
                try:
                    abstracts_list = target_div.xpath(self.abstracts_xpath)
                    abstracts = ','.join(abstracts_list) if ','.join(abstracts_list) else None
                except:
                    print('abstracts_xpath错误')
            else:
                if self.abstracts_pattern_list:
                    for abstracts_pattern in self.abstracts_pattern_list:
                        try:
                            abstracts = abstracts_pattern.findall(all_content)[0]
                            # abstracts = abstracts.replace('-', '')
                            if abstracts:
                                if isinstance(abstracts, tuple):
                                    abstracts = ';'.join(abstracts)
                                break
                            else:
                                continue
                        except:
                            continue

            # 研究方向
            directions = None
            if self.directions_xpath:
                try:
                    directions_list = target_div.xpath(self.directions_xpath)
                    directions = ','.join(directions_list) if ','.join(directions_list) else None
                except:
                    print('directions_xpath错误')
            else:
                if self.directions_pattern_list:
                    for directions_pattern in self.directions_pattern_list:
                        try:
                            directions = directions_pattern.findall(all_content)[0]
                            if directions:
                                if isinstance(directions, tuple):
                                    directions = ';'.join(directions)
                                break
                            else:
                                continue
                        except:
                            continue

            # 教育经历
            education_experience = None
            if self.education_experience_xpath:
                try:
                    education_experience_list = target_div.xpath(self.education_experience_xpath)
                    education_experience = ','.join(education_experience_list) if ','.join(education_experience_list) else None
                except:
                    print('education_experience_xpath错误')
            else:
                if self.education_experience_pattern_list:
                    for education_experience_pattern in self.education_experience_pattern_list:
                        try:
                            education_experience = education_experience_pattern.findall(all_content)[0]
                            if education_experience:
                                if isinstance(education_experience, tuple):
                                    education_experience = ';'.join(education_experience)
                                break
                            else:
                                continue
                        except:
                            continue

            # 工作经历
            work_experience = None
            if self.work_experience_xpath:
                try:
                    work_experience_list = target_div.xpath(self.work_experience_xpath)
                    work_experience = ','.join(work_experience_list) if ','.join(work_experience_list) else None
                except:
                    print('work_experience_xpath错误')
            else:
                if self.work_experience_pattern_list:
                    for work_experience_pattern in self.work_experience_pattern_list:
                        try:
                            work_experience = work_experience_pattern.findall(all_content)[0]
                            if work_experience:
                                if isinstance(work_experience, tuple):
                                    work_experience = ';'.join(work_experience)
                                break
                            else:
                                continue
                        except:
                            continue

            # 专利
            patent = None
            if self.patent_xpath:
                try:
                    patent_list = target_div.xpath(self.patent_xpath)
                    patent = ','.join(patent_list) if ','.join(patent_list) else None
                except:
                    print('patent_xpath错误')
            else:
                if self.patent_pattern_list:
                    for patent_pattern in self.patent_pattern_list:
                        try:
                            patent = patent_pattern.findall(all_content)[0]
                            if patent:
                                if isinstance(patent, tuple):
                                    patent = ';'.join(patent)
                                break
                            else:
                                continue
                        except:
                            continue

            # 科研项目
            project = None
            if self.project_xpath:
                try:
                    project_list = target_div.xpath(self.project_xpath)
                    project = ','.join(project_list) if ','.join(project_list) else None
                except:
                    print('project_xpath错误')
            else:
                if self.project_pattern_list:
                    for project_pattern in self.project_pattern_list:
                        try:
                            project = project_pattern.findall(all_content)[0]
                            if project:
                                if isinstance(project, tuple):
                                    project = ';'.join(project)
                                break
                            else:
                                continue
                        except:
                            continue

            # 荣誉/获奖信息
            award = None
            if self.award_xpath:
                try:
                    award_list = target_div.xpath(self.award_xpath)
                    award = ','.join(award_list) if ','.join(award_list) else None
                except:
                    print('award_xpath错误')
            else:
                if self.award_pattern_list:
                    for award_pattern in self.award_pattern_list:
                        try:
                            award = award_pattern.findall(all_content)[0]
                            if award:
                                if isinstance(award, tuple):
                                    award = ';'.join(award)
                                break
                            else:
                                continue
                        except:
                            continue

            # 社会兼职
            social_job = None
            if self.social_job_xpath:
                try:
                    social_job_list = target_div.xpath(self.social_job_xpath)
                    social_job = ','.join(social_job_list) if ','.join(social_job_list) else None
                except:
                    print('social_job_xpath错误')
            else:
                if self.social_job_pattern_list:
                    for social_job_pattern in self.social_job_pattern_list:
                        try:
                            social_job = social_job_pattern.findall(all_content)[0]
                            if social_job:
                                if isinstance(social_job, tuple):
                                    social_job = ';'.join(social_job)
                                break
                            else:
                                continue
                        except:
                            continue


            # 论文
            paper = None
            if self.paper_xpath:
                try:
                    paper_list = target_div.xpath(self.paper_xpath)
                    paper = ','.join(paper_list) if ','.join(paper_list) else None
                except:
                    print('paper_xpath错误')
            else:
                if self.paper_pattern_list:
                    for paper_pattern in self.paper_pattern_list:
                        try:
                            paper = paper_pattern.findall(all_content)[0]
                            if paper:
                                if isinstance(paper, tuple):
                                    paper = ';'.join(paper)
                                break
                            else:
                                continue
                        except:
                            continue

            # 学位、学历
            qualification = None
            education = None
            edu_dict = {'学士': 1, '硕士': 2, '博士': 3}
            edu_code = 0
            if self.qualification_xpath:
                try:
                    qualification_list = target_div.xpath(self.qualification_xpath)
                    qualification = ','.join(qualification_list) if ','.join(qualification_list) else None
                    if qualification == '学士':
                        education = '本科'
                    elif qualification == '硕士':
                        education = '研究生'
                    elif qualification == '博士':
                        education = '研究生'
                except:
                    print('qualification_xpath错误')
            else:
                try:
                    qualification_list = self.qualification_pattern.findall(all_content)
                    for qualification in qualification_list:
                        edu_code_temp = edu_dict[qualification]
                        if edu_code_temp > edu_code:
                            edu_code = edu_code_temp
                    if edu_code == 1:
                        qualification = '学士'
                        education = '本科'
                    elif edu_code == 2:
                        qualification = '硕士'
                        education = '研究生'
                    elif edu_code == 3:
                        qualification = '博士'
                        education = '研究生'
                except:
                    pass
            # try:
            #     qualification = self.qualification_pattern.findall(all_content)[0]
            #     if qualification == '学士':
            #         education = '本科'
            #     else:
            #         education = '研究生'
            # except:
            #     pass

            # 在职信息
            job_information = 1
            if self.job_information_pattern_list:
                for job_information_pattern in self.job_information_pattern_list:
                    try:
                        job_information = job_information_pattern.findall(all_content)[0]
                        if job_information:
                            if isinstance(job_information, tuple):
                                job_information = ';'.join(job_information)
                            break
                        else:
                            continue
                    except:
                        continue

            # 主要任职
            responsibilities = None
            if self.responsibilities_xpath:
                try:
                    responsibilities_list = target_div.xpath(self.responsibilities_xpath)
                    responsibilities = ','.join(responsibilities_list) if ','.join(responsibilities_list) else None
                except:
                    print('responsibilities_xpath错误')
            else:
                if self.responsibilities_pattern_list:
                    for responsibilities_pattern in self.responsibilities_pattern_list:
                        try:
                            responsibilities = responsibilities_pattern.findall(all_content)[0]
                            if responsibilities:
                                if isinstance(responsibilities, tuple):
                                    responsibilities = ';'.join(responsibilities)
                                break
                            else:
                                continue
                        except:
                            continue

            # 办公室地点
            office_address = None
            if self.office_address_xpath:
                try:
                    office_address_list = target_div.xpath(self.office_address_xpath)
                    office_address = ','.join(office_address_list) if ','.join(office_address_list) else None
                except:
                    print('office_address_xpath错误')
            else:
                if self.office_address_pattern_list:
                    for office_address_pattern in self.office_address_pattern_list:
                        try:
                            office_address = office_address_pattern.findall(all_content)[0]
                            if office_address:
                                if isinstance(office_address, tuple):
                                    office_address = ';'.join(office_address)
                                break
                            else:
                                continue
                        except:
                            continue

            result = {
                'name': name,
                'institute_id': institute_id,
                'laboratory_id': laboratory_id,
                'phone': phone,
                'email': email,
                'job_title': job_title,
                'abstracts': abstracts,
                'directions': directions,
                'talent_title': '',
                'administrative_title': '',
                'education_experience': education_experience,
                'work_experience': work_experience,
                'patent': patent,
                'project': project,
                'award': award,
                'paper': paper,
                'social_job': social_job,
                'picture': full_src,
                'education': education,
                'qualification': qualification,
                'job_information': job_information,
                'responsibilities': responsibilities,
                'office_address': office_address,
                'origin': origin_url,
                'results': '',
                'publication': '',
                'personal_website': '',
                'parent_id': ''
            }
            # return result
            if self.api or self.selenium_gpt:  # 使用第三方api解析
                return content_with_label, result
            else:
                return result
        else:
            print('对应页面的请求失败,详情页为None')

    def parse_by_api(self, mid_result):
        loop = asyncio.get_event_loop()
        session = aiohttp.ClientSession()
        tasks = [api_parse(result_gen, session, self.partition_num, self.img_url_head, self.cn_com) for result_gen in mid_result if result_gen]
        results = loop.run_until_complete(asyncio.gather(*tasks))
        session.connector.close()
        temp = []
        for result in results:
            if isinstance(result, tuple):
                self.api_cant.append(result[1])
                # results.remove(result)  # 若两个连续的值需要remove，因为是原地操作会出现问题，修正了bug
            else:
                temp.append(result)
                self.writter.writerow(result.values())

        print('*** api failed ***', self.api_cant)
        return pd.DataFrame(temp)


    # @retry(exceptions=Exception, tries=1, delay=1)
    def parse_detail_gpt(self, text, result_direct):
        # 清空对话记录
        try:
            logger.info('尝试清除记录...')
            if self.cn_com == 'cn':
                self.driver.find_element(by=By.XPATH, value=selector.get('cn-clear-xpath')).click()
            else:
                self.driver.find_element(By.CSS_SELECTOR, selector.get('com-clear-css')).click()
            logger.info('记录清除成功')
        except:
            logger.info('无记录')

        # 输入框
        # text = re.sub(r'\r|\n', '', text)
        # 刷新，避免retry时报错

        # 等待出现对话框，超时刷新页面
        while True:
            try:
                if self.cn_com == 'cn':
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector.get('cn-textarea-xpath'))))
                else:
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector.get('com-textarea-xpath'))))
                break
            except:
                self.driver.refresh()
                # time.sleep(2)
                continue
        # 找到对话框的元素element
        if self.cn_com == 'cn':
            element = self.driver.find_element(by=By.XPATH, value=selector.get('cn-textarea-xpath'))  # .send_keys(text)
        else:
            element = self.driver.find_element(By.CSS_SELECTOR, selector.get('com-textarea-css'))
        # send输入较慢换为使用pyperclip粘贴
        pyperclip.copy(text)
        element.send_keys(Keys.CONTROL, 'v')

        # 等待发送按钮  todo:若仍报错，尝试使用xpath，附带可点击的属性
        while True:
            try:
                if self.cn_com == 'cn':
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector.get('cn-sendtext-xpath'))))
                else:
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector.get('com-sendtext-xpath'))))
                break
            except:
                element.clear()
                logger.info('等待发送按钮的出现-p1')
                element.send_keys(Keys.CONTROL, 'v')
                continue
        time.sleep(0.5)

        # 点击发送按钮
        if self.cn_com == 'cn':
            self.driver.find_element(by=By.XPATH, value=selector.get('cn-sendtext-xpath')).click()
        else:
            self.driver.find_element(By.XPATH, selector.get('com-sendtext-xpath')).click()
        # 等待内容 --出现消耗的token数
        try:
            if self.cn_com == 'cn':
                WebDriverWait(self.driver, 180).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('cn-tokens-css'))))
            else:
                WebDriverWait(self.driver, 180).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('com-tokens-css'))))
        # 超时刷新页面，再试一次
        except:
            logger.warning('首次等待内容完成超时，重新刷新页面')
            self.driver.refresh()
            while True:
                try:
                    if self.cn_com == 'cn':
                        WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector.get('cn-textarea-xpath'))))
                    else:
                        WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(
                            (By.XPATH, selector.get('com-textarea-xpath'))))
                    break
                except:
                    self.driver.refresh()
                    # time.sleep(2)
                    continue

            if self.cn_com == 'cn':
                element = self.driver.find_element(by=By.XPATH,
                                                   value=selector.get('cn-textarea-xpath'))  # .send_keys(text)
            else:
                element = self.driver.find_element(By.CSS_SELECTOR, selector.get('com-textarea-css'))
            # send输入较慢换为使用pyperclip粘贴
            pyperclip.copy(text)
            element.send_keys(Keys.CONTROL, 'v')

            # 等待发送按钮 todo:若仍报错，尝试使用xpath，附带可点击的属性
            while True:
                try:
                    if self.cn_com == 'cn':
                        WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, selector.get('cn-sendtext-xpath'))))
                    else:
                        WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, selector.get('com-sendtext-xpath'))))
                    break
                except:
                    element.clear()
                    logger.info('等待发送按钮的出现-p2')
                    element.send_keys(Keys.CONTROL, 'v')
                    continue
            time.sleep(0.5)

            # 发送
            if self.cn_com == 'cn':
                self.driver.find_element(by=By.XPATH,
                                         value=selector.get('cn-sendtext-xpath')).click()
            else:
                self.driver.find_element(By.XPATH, selector.get('com-sendtext-xpath')).click()
            # 等待内容
            try:
                if self.cn_com == 'cn':
                    WebDriverWait(self.driver, 180).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('cn-tokens-css'))))
                else:
                    WebDriverWait(self.driver, 180).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('com-tokens-css'))))
            except:
                logger.warning('刷新页面后，仍未等到目标回复文本出现。将返回空值，并记录姓名')
                self.gpt_cant.append(result_direct['name'])

                # 清空对话记录
                try:
                    logger.info('尝试清除记录...')
                    if self.cn_com == 'cn':
                        self.driver.find_element(by=By.XPATH,
                                                 value=selector.get('cn-clear-xpath')).click()
                    else:
                        self.driver.find_element(By.CSS_SELECTOR, selector.get('com-clear-css'))
                    logger.info('记录清除成功')
                except:
                    logger.info('无记录')

                self.driver.refresh()
                time.sleep(2)

                return None

        # 解析过程
        try:
            if self.cn_com == 'cn':
                content = self.driver.find_element(by=By.XPATH, value=selector.get('cn-content-xpath')).get_attribute('innerText')
            else:
                try:
                    # gemini系列
                    content = self.driver.find_element(By.XPATH, selector.get('com-content-gemini-xpath')).get_attribute('innerText')
                except:
                    # gpt系列
                    content = self.driver.find_element(By.XPATH, selector.get('com-content-gpt-xpath')).get_attribute('innerText')
        # 第一次就出现了json/元素的定位方式不一样导致报错
        except:
            # gemini系列
            logger.warning('找不到回复文本,即将重新生成')
            # gpt系列
            # logger.warning('找不到回复文本，或者返回的内容是json框。即将重新生成')
            while True:
                # 点击重新生成按钮
                # todo:cn的json格式重新生成不知道在哪，暂未完善,遇到再处理
                logger.info('重新生成--位置1')
                if self.cn_com == 'cn':
                    self.driver.find_element(By.CSS_SELECTOR, selector.get('cn-regenerate-css')).click()
                    print('请完善cn的css选择器')
                else:
                    self.driver.find_element(By.CSS_SELECTOR, selector.get('com-regenerate-css')).click()
                # 等待重新生成
                try:
                    if self.cn_com == 'cn':
                        WebDriverWait(self.driver, 180).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('cn-tokens-css'))))
                        content = self.driver.find_element(By.XPATH, selector.get('cn-content-xpath')).get_attribute('innerText')
                    else:
                        WebDriverWait(self.driver, 180).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('com-tokens-css'))))
                        try:
                            # gemini系列
                            content = self.driver.find_element(By.XPATH, selector.get('com-content-gemini-xpath')).get_attribute('innerText')
                        except:
                            # gpt系列
                            content = self.driver.find_element(By.XPATH, selector.get('com-content-gpt-xpath')).get_attribute('innerText')
                    # if isinstance(content, list):
                    #     content = ''.join(content)
                    # content = json.loads(content, strict=False)
                    break
                except:
                    continue

        if isinstance(content, list):
            # for li in content:
            #     re.sub(r'')
            content = ''.join(content)
        # content = re.sub(r'\r|\n', '', content)
        # print(content)
        try:
            # content = json.loads(content, strict=False)
            content = demjson3.decode(content)
            if re.search('<.*?>', str(content)):
                logger.warning('发现html标签，即将重新生成')
                raise
        # 重新解析
        except:
            logger.warning('内容格式无法解析，即将重新生成')
            print(content)
            count = 0
            while True:
                if count > 3:
                    logger.warning(f'重新生成了3次均无法解析，可能{result_direct["name"]}内容过长或者存在html标签')
                    self.gpt_cant.append(result_direct['name'])
                    # 还原模型
                    restore_model(self.driver)
                    time.sleep(5)

                    # 刷新页面，防止下一个出问题
                    while True:
                        self.driver.refresh()
                        try:
                            if self.cn_com == 'cn':
                                WebDriverWait(self.driver, 5).until(
                                    EC.presence_of_element_located((By.XPATH, selector.get('cn-textarea-xpath'))))
                            else:
                                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(
                                    (By.XPATH, selector.get('com-textarea-xpath'))))
                            break
                        except:
                            # time.sleep(2)
                            continue
                    return None
                elif count > 0:
                    change_model(count, self.driver)
                    time.sleep(5)
                else:
                    logger.info('第一次重新生成，使用Gemini-flash')
                # 等待重新生成按钮出现,一般情况下不需要等待
                while True:
                    try:
                        if self.cn_com == 'cn':
                            WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('cn-regenerate-css'))))
                        else:
                            WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('com-regenerate-css'))))
                        break
                    except:
                        logger.info('等待重新生成按钮的出现')
                        continue
                # 点击重新生成按钮
                # logger.info('重新生成--2')
                if self.cn_com == 'cn':
                    self.driver.find_element(By.CSS_SELECTOR, selector.get('cn-regenerate-css')).click()
                else:
                    self.driver.find_element(By.CSS_SELECTOR, selector.get('com-regenerate-css')).click()
                # 等待重新生成
                try:
                    # 等待tokens出现
                    if self.cn_com == 'cn':
                        WebDriverWait(self.driver, 180).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('cn-tokens-css'))))
                        content = self.driver.find_element(By.XPATH, selector.get('cn-content-xpath')).get_attribute('innerText')
                    else:
                        WebDriverWait(self.driver, 180).until(EC.presence_of_element_located((By.CSS_SELECTOR, selector.get('com-tokens-css'))))
                        # gemini系列
                        try:
                            content = self.driver.find_element(By.XPATH, selector.get('com-content-gemini-xpath')).get_attribute('innerText')
                        # gpt系列
                        except:
                            content = self.driver.find_element(By.XPATH, selector.get('com-content-gpt-xpath')).get_attribute('innerText')
                    if isinstance(content, list):
                        content = ''.join(content)
                    # content = json.loads(content, strict=False)
                    content = demjson3.decode(content)
                    if re.search(r'<.*?>', str(content)):
                        logger.warning('发现html标签，即将重新生成')
                        raise
                    # 还原模型
                    if count > 0:
                        restore_model(self.driver)
                        time.sleep(5)
                    # 刷新页面，防止下一个出问题
                    while True:
                        self.driver.refresh()
                        try:
                            if self.cn_com == 'cn':
                                WebDriverWait(self.driver, 5).until(
                                    EC.presence_of_element_located((By.XPATH, selector.get('cn-textarea-xpath'))))
                            else:
                                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located(
                                    (By.XPATH, selector.get('com-textarea-xpath'))))
                            break
                        except:
                            # time.sleep(2)
                            continue
                    break
                except:
                    count += 1
                    continue

        # 清空对话记录
        try:
            logger.info('尝试清除记录...')
            if self.cn_com == 'cn':
                self.driver.find_element(by=By.XPATH,
                                         value=selector.get('cn-clear-xpath')).click()
            else:
                self.driver.find_element(By.CSS_SELECTOR, selector.get('com-clear-css')).click()
            logger.info('记录清除成功')
        except:
            logger.info('无记录')

        # 等待刷新成功  不刷新可能导致下一伦找不到元素
        while True:
            self.driver.refresh()
            try:
                if self.cn_com == 'cn':
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector.get('cn-textarea-xpath'))))
                else:
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, selector.get('com-textarea-xpath'))))
                break
            except:
                continue

        phone = clean_phone(self.partition_num, content.get('电话'))

        result = {
            'name': result_direct['name'],
            'institute_id': result_direct['institute_id'],
            'laboratory_id': result_direct['laboratory_id'],
            'phone': phone if phone else result_direct['phone'],
            'email': content.get('邮箱') if content.get('邮箱') else result_direct['email'],
            'job_title': content.get('职称') if content.get('职称') else result_direct['job_title'],
            'abstracts': content.get('个人简介') if content.get('个人简介') else result_direct['abstracts'],
            'directions': content.get('研究方向') if content.get('研究方向') else result_direct['directions'],
            'talent_title': result_direct['talent_title'],
            'administrative_title': result_direct['administrative_title'],
            'education_experience': content.get('教育经历') if content.get('教育经历') else result_direct['education_experience'],
            'work_experience': content.get('工作经历') if content.get('工作经历') else result_direct['work_experience'],
            'patent': content.get('专利') if content.get('专利') else result_direct['patent'],
            'project': content.get('科研项目') if content.get('科研项目') else result_direct['project'],
            'award': content.get('荣誉/获奖') if content.get('荣誉/获奖') else result_direct['award'],
            'paper': content.get('科研论文') if content.get('科研论文') else result_direct['paper'],
            'social_job': content.get('社会兼职') if content.get('社会兼职') else result_direct['social_job'],
            'picture': None if not content.get('照片地址') else parse.urljoin(result_direct['picture'], content.get('照片地址')) if not self.img_url_head else parse.urljoin(self.img_url_head, content.get('照片地址')),
            'education': result_direct['education'],
            'qualification': result_direct['qualification'],
            'job_information': result_direct['job_information'],
            'responsibilities': content.get('职位') if content.get('职位') else result_direct['responsibilities'],
            'office_address': content.get('办公地点') if content.get('办公地点') else result_direct['office_address'],
            'origin': result_direct['origin'],
            'results': result_direct['results'],
            'publication': result_direct['publication'],
            'personal_website': result_direct['personal_website'],
            'parent_id': result_direct['parent_id']
        }
        return result

    def parse_by_gpt(self, mid_results):
        count = 0
        parser = GptParser(self.cn_com)
        self.driver = parser.init_driver()
        for mid_result in mid_results:
            if mid_result:
                text, result_direct = mid_result
                result_gpt = self.parse_detail_gpt(text, result_direct)
                print(result_gpt)
                # 防止网页本身为空
                if result_gpt:
                    self.result_df = result_dict_2_df(self.result_df, result_gpt)
                    self.writter.writerow(result_gpt.values())
                else:
                    continue

                count += 1
                if count > 4:
                    self.driver.close()
                    self.driver = parser.init_driver()
                    count = 0
        self.driver.close()
        logger.info('处理完毕')

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
            index_page = get_response(url, self.cn_com)  # cn_com转化为是否代理
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
        if not self.result_df.empty:
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
