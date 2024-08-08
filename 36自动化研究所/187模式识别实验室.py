# -*- coding: utf-8 -*-
import csv
import os
import re
import time

import pandas as pd
from loguru import logger

from crawler import ReCrawler
from selenium import webdriver
from utils import csv_header, result_dict_2_df, csv_2_df, drop_duplicate_collage, truncate_table, df2mysql, \
    local_engine, sf_engine, save_as_json

options = webdriver.ChromeOptions()
options.add_experimental_option('detach', False)

institute_name = '自动化研究所'
laboratory_name = '模式识别实验室'
institute_id = 36
laboratory_id = 187
img_url_head = None
partition_num = '010'
start_urls = [
                'http://www.cripac.ia.ac.cn/CN/column/column149.shtml',
                'http://www.cripac.ia.ac.cn/CN/column/column150.shtml',
                'http://www.cripac.ia.ac.cn/CN/column/column151.shtml',
              ]

a_s_xpath_str = ''
target_div_xpath_str = '//body'


class SpecialSpider(ReCrawler):
    def parse_index(self, index_page, url):
        # 方法重写时引入包
        global etree, parse
        if not globals().get('etree'):
            from lxml import etree
        if not globals().get('parse'):
            from urllib import parse

        page = etree.HTML(index_page)
        names = page.xpath('//strong/text()')
        names = list(map(lambda x: re.sub(r'\s*', '', x), names))
        a_s = page.xpath('/html/body//table//table//table//table//a[@target]/@href')
        return [*zip(names, a_s)]

        # for a in a_s:
        #     name = a.xpath('.//text()')
        #     if name:
        #         name = ''.join(name)
        #         if not re.match(r'[A-Za-z\s]*$', name, re.S):  # 中文名替换空格
        #             name = re.sub(r'\s*', '', name)
        #         name = re.sub(self.name_filter_re, '', name)
        #         link = a.xpath('./@href')[0]
        #         link = parse.urljoin(url, link)
        #     else:
        #         print('未解析到name，请检查：a_s_xpath_str')
        #         continue
        #     yield name, link

spider = SpecialSpider(
                   institute_name=institute_name,
                   laboratory_name=laboratory_name,
                   partition_num=partition_num,
                   institute_id=institute_id,
                   laboratory_id=laboratory_id,
                   name_filter_re=r'\s*',
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


