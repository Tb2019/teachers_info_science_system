# -*- coding: utf-8 -*-
import re

from bs4 import BeautifulSoup

from crawler import ReCrawler
from utils import replace_quotes_in_text

institute_name = '物理研究所'
laboratory_name = '表面物理国家重点实验室'
institute_id = 1
laboratory_id = 2
img_url_head = None
partition_num = '010'
start_urls = [
                'https://surface.iphy.ac.cn/list_people.php?id=1840'
              ]

a_s_xpath_str = '/html/body/table[4]/tr/td[2]/table/tr/td/table/tr/td'
target_div_xpath_str = '/html/body/table[4]'

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
    def parse_index(self, index_page, url):
        # 方法重写时引入包
        global etree, parse
        if not globals().get('etree'):
            from lxml import etree
        if not globals().get('parse'):
            from urllib import parse

        page = etree.HTML(index_page)
        a_s = page.xpath(self.a_s_xpath_str)
        for a in a_s:
            name = a.xpath('./table/tr[2]/td/a/text()')
            # print(name)
            if name:
                name = ''.join(name)
                if not re.match(r'[A-Za-z\s]*$', name, re.S):  # 中文名替换空格
                    name = re.sub(r'\s*', '', name)
                name = re.sub(self.name_filter_re, '', name)
                link = a.xpath('./table/tr[2]/td/a/@href')[0]
                link = parse.urljoin(url, link)
                img = a.xpath('./table/tr[1]/td/a/img/@src')[0]
            else:
                print('未解析到name，请检查：a_s_xpath_str')
                continue
            yield name, link, img

    def get_detail_page(self, index_result):
        # 方法重写时引入包
        global asyncio, aiohttp, get_response_async
        if not globals().get('asyncio'):
            import asyncio
        if not globals().get('aiohttp'):
            import aiohttp
        if not globals().get('get_response_async'):
            from utils import get_response_async

        loop = asyncio.get_event_loop()
        session = aiohttp.ClientSession()
        tasks = [get_response_async(url, session, name=name, img=img) for name, url, img in index_result if len(name) >= 2]
        detail_pages = loop.run_until_complete(asyncio.gather(*tasks))
        session.connector.close()
        return detail_pages

    def parse_detail(self, detail_page, url):
        # 方法重写时引入包
        global etree, tostring, parse
        if not globals().get('etree'):
            from lxml import etree
        if not globals().get('tostring'):
            from lxml.html import tostring
        if not globals().get('parse'):
            from urllib import parse

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
                    education_experience = ','.join(education_experience_list) if ','.join(
                        education_experience_list) else None
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

            # 图片
            full_src = info_s[1][1]

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
            print('对应页面的请求失败,返回None')

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


