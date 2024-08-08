# -*- coding: utf-8 -*-
import re
from crawler import ReCrawler

institute_name = '古脊椎动物与古人类研究所'
laboratory_name = '古脊椎动物与古人类研究所'
institute_id = 22
laboratory_id = 146
img_url_head = None
partition_num = '010'
start_urls = [
                'https://ivpp.cas.cn/kyzc/yjdw/yszj/',
                'https://ivpp.cas.cn/kyzc/yjdw/yjy/',
                'https://ivpp.cas.cn/kyzc/yjdw/yjy/index_1.html',
                'https://ivpp.cas.cn/kyzc/yjdw/yjy/index_2.html',
                'https://ivpp.cas.cn/kyzc/yjdw/yjy/index_3.html',
                'https://ivpp.cas.cn/kyzc/yjdw/fyjy/',
                'https://ivpp.cas.cn/kyzc/yjdw/fyjy/index_1.html',
                'https://ivpp.cas.cn/kyzc/yjdw/fyjy/index_2.html',
              ]

a_s_xpath_str = '//ul[@class="temp02-wrap-list-people pb20 clearfix"]//a'
target_div_xpath_str = '//div[@class="row temp02-wrap-content"]'

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

spider = ReCrawler(
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


