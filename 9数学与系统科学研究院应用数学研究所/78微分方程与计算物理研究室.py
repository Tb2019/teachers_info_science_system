# -*- coding: utf-8 -*-
# -*- coding: utf-8 -*-
import re
from fake_headers import Headers
import requests
from crawler import ReCrawler

headers = Headers(headers=True).generate()

institute_name = '数学与系统科学研究院应用数学研究所'
laboratory_name = '微分方程与计算物理研究室'
institute_id = 9
laboratory_id = 78
img_url_head = None
partition_num = '010'
start_urls = [
                'http://amt.amss.cas.cn/kyry/'
              ]

a_s_xpath_str = '//div[@class="namelist mt15"]/table[2]//td/a'
target_div_xpath_str = '/html//body'

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
            name = a.xpath('.//text()')
            # print(name)
            if name:
                name = ''.join(name)
                if not re.match(r'[A-Za-z\s]*$', name, re.S):  # 中文名替换空格
                    name = re.sub(r'\s*', '', name)
                name = re.sub(self.name_filter_re, '', name)
                link = a.xpath('./@href')[0]
                link = parse.urljoin(url, link)
                if re.match(r'http://amt.amss.cas.cn/[a-zA-Z0-9]*$', link, re.S):
                    resp = requests.get(link, headers=headers).text
                    link = re.search(r'<meta.*?content.*?url=(.*?)\s*".*?>', resp, re.S).group(1)
            else:
                print('未解析到name，请检查：a_s_xpath_str')
                continue
            yield name, link

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


