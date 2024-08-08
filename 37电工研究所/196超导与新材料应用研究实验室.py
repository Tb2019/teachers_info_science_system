# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import csv
import os
import re
import time
import asyncio
import aiohttp
import pandas as pd
from lxml import etree
from urllib import parse
from loguru import logger
from crawler import ReCrawler
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from utils import csv_header, result_dict_2_df, csv_2_df, drop_duplicate_collage, truncate_table, df2mysql, \
    local_engine, sf_engine, save_as_json, get_response_async, api_parse

service = Service('D:/Software/anaconda/envs/spider/chromedriver.exe')
options = webdriver.ChromeOptions()
options.add_experimental_option('detach', True)
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)

institute_name = '电工研究所'
laboratory_name = '超导与新材料应用研究实验室'
institute_id = 37
laboratory_id = 196
img_url_head = None
partition_num = '010'
start_urls = [
                'https://iee.cas.cn/rcdw/zgjzj/'
              ]

a_s_xpath_str = '//div[@class="row" and position()>1][5]//li/a'
target_div_xpath_str = '//div[@class="intro-bd intro-article"]|//div[@class="intro-bd intro-cv"]'


# 重写方法
class SpecialSpider(ReCrawler):
    # todo：方法一
    # 姓名和超链接需要单独获取时，重写姓名和链接的获取方式
    # 首页需要增加信息时（在首页获取照片信息），增加额外信息的获取方式，并且重写 方法二
    '''
    def parse_index(self, index_page, url):
        page = etree.HTML(index_page)
        a_s = page.xpath(self.a_s_xpath_str)
        for a in a_s:
            name = a.xpath('.//text()')
            if name:
                name = ''.join(name)
                if not re.match(r'[A-Za-z\s]*$', name, re.S):  # 中文名替换空格
                    name = re.sub(r'\s*', '', name)
                name = re.sub(self.name_filter_re, '', name)
                link = a.xpath('./@href')[0]
                link = parse.urljoin(url, link)
            else:
                print('未解析到name，请检查：a_s_xpath_str')
                continue
            yield name, link
    '''

    # todo：方法二
    # 方法一增加首页获取的信息时，需要重写
    '''
    def get_detail_page(self, index_result):
        loop = asyncio.get_event_loop()
        session = aiohttp.ClientSession()
        tasks = [get_response_async(url, session, name=name) for name, url in index_result if len(name) >= 2]
        detail_pages = loop.run_until_complete(asyncio.gather(*tasks))
        session.connector.close()
        return detail_pages
    '''

    # todo：方法三
    # 方法一增加首页信息时，需要重写
    '''
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
    '''

    # todo:方法四
    # 自动化工具获取 详情页 时重写
    '''
    def get_detail_page(self, index_result):
        detail_pages = []

        driver = webdriver.Chrome(options=options, service=service)
        driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
                                Object.defineProperty(navigator, 'webdriver', {
                                  get: () => undefined
                                })
                              """
        })
        driver.execute_cdp_cmd("Network.enable", {})
        driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {"User-Agent": "browser1"}})

        for name, url in index_result:
            if len(name) >= 2:
                driver.get(url)
                time.sleep(2)
                page = driver.page_source
                detail_pages.append((page, url, [('name', name)]))
        driver.close()
        return detail_pages
    '''

    # todo:方法五
    # 自动化工具获取 首页 时重写

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

            driver = webdriver.Chrome(options=options, service=service)
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                                            Object.defineProperty(navigator, 'webdriver', {
                                              get: () => undefined
                                            })
                                          """
            })
            driver.execute_cdp_cmd("Network.enable", {})
            driver.execute_cdp_cmd("Network.setExtraHTTPHeaders", {"headers": {"User-Agent": "browser1"}})
            driver.get(url)
            time.sleep(2)
            index_page = driver.page_source
            driver.close()
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

                   save2target='target',
                   selenium_gpt=False,
                   cn_com='',
                   api=True,
                   )

spider.run()

