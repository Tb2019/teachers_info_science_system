# -*- coding: utf-8 -*-
# 数据提取完毕 入库失败时使用。或者
import csv
import os
from asyncio.log import logger
from utils import csv_2_df, truncate_table, df2mysql, drop_duplicate_collage, local_engine, sf_engine, save_as_json
from urllib import parse

institute_id = 115
laboratory_id = 380
institute_name = '新疆生态与地理研究所'
laboratory_name = '新疆生态与地理研究所'

img_url_head = ''

data_real = {
"姓名": "苏永中",
"电话": "0931-4967070",
"邮箱": "suyzh@lzb.ac.cn",
"职称": "研究员",
"个人简介": "",
"研究方向": "土壤学与绿洲生态学",
"人才称号": "",
"行政称号": "",
"专利": "",
"科研项目": "1、国家自然科学基金项目“黑河流域边缘新垦绿洲农田灌溉需水评估及水土环境效应观测研究”（2012-2014），55万元，项目负责人； \n2、中国科学院重点布署项目“宁夏旱区生态高值农业”课题“河西新垦绿洲农田土壤与灌溉水生产力调控试验示范”（2013-2015），120万元，课题和项目负责人。",
"荣誉/获奖": "1、获甘肃省科技进步二等奖三项； \n2、获中国科学院首届优秀博士学位论文奖。",
"照片地址": "",
"最高学历": "研究生",
"最高学位": "硕士",
"职位": "",
"办公地点": "兰州东岗西路320号",
"科研论文": "1. Su, Yong Zhong, Yang, Rong, Liu, Wen Jie, Xue Fen. Evolution of soil structure and fertility after conversion of native sandy desert soil to irrigated cropland in arid region, China. Soil Science, 2010, 175(5): 246-254. \n2. Su, Yong Zhong, Wang, Xue Fen, Yang, Rong, Jaehoon Lee. Effects of sandy desertified land rehabilitation on soil carbon sequestration and aggregation in an arid region in China. Journal of Environmental Management, 2010, 91: 2109-2116. \n3. Su, Yong Zhong, Liu, Wen Jie, Yang, Rong, Chang Xue Xiang. Changes in soil aggregate, carbon, and nitrogen storages following the conversion of cropland to alfalfa forage land in the marginal oasis of northwest China. Environmental Management, 2009, 43:1061-1070. \n4. Su Yong Zhong. 2007. Soil carbon and nitrogen sequestration following the conversion of cropland to alfalfa forage land in northwest China. Soil and Tillage Research, 92: 181-189. \n5. Su, Yong Zhong, Yang, Rong, 2008.Background concentrations of elements in surface soils and their changes as affected by agriculture use in the desert-oasis ecotone in the middle of Heihe River Basin, North-west China Journal of Geochemical Exploration, 98(3): 57-64. ",
"源地址": "http://cw.casnw.net//wwwroot/nlh/rcpy/dsjs/249952.shtml",
"科技成果": "",
"著作": "",
"个人网址": "",
"导师id": ""
}

data = {
    "姓名": data_real["姓名"],
    "研究所": institute_id,
    "实验室": laboratory_id,
    "电话": data_real["电话"],
    "邮箱": data_real["邮箱"],
    "职称": data_real["职称"],
    "个人简介": data_real["个人简介"],
    "研究方向": data_real["研究方向"],
    "人才称号": data_real["人才称号"],
    "行政称号": data_real["行政称号"],
    "教育经历": "",
    "工作经历": "",
    "专利": data_real["专利"],
    "科研项目": data_real["科研项目"],
    "荣誉/获奖": data_real["荣誉/获奖"],
    "科研论文": data_real["科研论文"],
    "社会兼职": "",
    "照片地址": parse.urljoin(img_url_head, data_real["照片地址"]),
    "最高学历": data_real["最高学历"],
    "最高学位": data_real["最高学位"],
    "在职信息": 1,
    "职位": data_real["职位"],
    "办公地点": data_real["办公地点"],
    "源地址": data_real["源地址"],
    "科技成果": data_real["科技成果"],
    "著作": data_real["著作"],
    "个人网址": data_real["个人网址"],
    "导师id": data_real["导师id"]
}

add = input('是否需要手动增加数据？是请输入1：')
if add == '1':
    file = open(f'./{institute_id}{institute_name}/{laboratory_id}.csv', mode='a', encoding='utf-8', newline='')
    writter = csv.writer(file)
    writter.writerow(data.values())
    file.close()

result_df = csv_2_df(f'./{institute_id}{institute_name}/{laboratory_id}.csv')
result_df = drop_duplicate_collage(result_df)
print(result_df)
ensure = input('确定保存吗？确定输入1：')
if ensure == '1':
    logger.info('再次保存中...')
    # if save2target == 'no':
    #     pass

    # elif self.save2target == 'test':
    # truncate_table(host='localhost', user='root', password='123456', database='alpha_search', port=3306, table_name='search_teacher_test')
    # df2mysql(engine=local_engine, df=result_df, table_name='search_teacher_test')

    # elif self.save2target == 'local':
    #     df2mysql(engine=local_engine, df=result_df, table_name='search_teacher')
    # elif self.save2target == 'target':
    df2mysql(engine=sf_engine, df=result_df, table_name='search_institute_user')
    save_as_json(result_df, institute_name, laboratory_name, path=1)
