import os
import json
import re
import time

import demjson3
import pymysql
import requests
import pandas as pd
import numpy as np
from urllib import parse
from loguru import logger
from itertools import chain
from asyncio import Semaphore
from fake_headers import Headers
from urllib.parse import quote_plus
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from sqlalchemy import create_engine
from gptparser import GptParser

proxy = {
    'http': '127.0.0.1:7890',
    'https': '127.0.0.1:7890',
}

selector = {
    # 清除记录
    'com-clear-css': '#root > div:nth-child(2) > div > div > div > div > div.aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.UMf9npeM8cVkDi0CDqZ0 > div.TH9DlQU1qwg_KGXdDYzk > div > div.nIP4BqLGD8csFme4CavI > div.WfXRc6x8M2gbaaX2HSxJ > div > div.k7y7pgLJN2EYTHcUikQA > div.AXzy5aeT38Mdxk6pvvuE > div.NyvVfPwFXFYvQFyXUtTl > button',

    'cn-clear-xpath': '//div[@class="left-actions-container--NyvVfPwFXFYvQFyXUtTl"]',

    # 文本框
    'com-textarea-xpath': '//textarea[@class="rc-textarea oTXB57QK8bQN2BKYJ2Bi oTXB57QK8bQN2BKYJ2Bi"]',
    'com-textarea-css': '#root > div:nth-child(2) > div > div > div > div > div.aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.UMf9npeM8cVkDi0CDqZ0 > div.TH9DlQU1qwg_KGXdDYzk > div > div.nIP4BqLGD8csFme4CavI > div.WfXRc6x8M2gbaaX2HSxJ > div > div.k7y7pgLJN2EYTHcUikQA > div.AXzy5aeT38Mdxk6pvvuE > div.k5ePpJvczIMzaNIaOwKS > div > textarea',

    'cn-textarea-xpath': '//textarea[@class="rc-textarea textarea--oTXB57QK8bQN2BKYJ2Bi textarea--oTXB57QK8bQN2BKYJ2Bi"]',

    # 发送
    'com-sendtext-css': '#root > div:nth-child(2) > div > div > div > div > div.aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.UMf9npeM8cVkDi0CDqZ0 > div.TH9DlQU1qwg_KGXdDYzk > div > div.nIP4BqLGD8csFme4CavI > div.WfXRc6x8M2gbaaX2HSxJ > div > div.k7y7pgLJN2EYTHcUikQA > div.AXzy5aeT38Mdxk6pvvuE > div.k5ePpJvczIMzaNIaOwKS > div > div > div.vr4WgM3FUuUicP3kJDOU > button',
    'com-sendtext-xpath': '//button[@class="semi-button semi-button-primary semi-button-size-small semi-button-borderless tqxhKsM_tI4KvPKo9dkI semi-button-with-icon semi-button-with-icon-only" and @aria-disabled="false"]',

    'cn-sendtext-xpath': '//div[@class="textarea-actions-right--vr4WgM3FUuUicP3kJDOU"]',
    # tokens
    # 'com-tokens-css': '#root > div:nth-child(2) > div > div > div > div > div.aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.UMf9npeM8cVkDi0CDqZ0 > div.TH9DlQU1qwg_KGXdDYzk > div > div.R_WS6aCLs2gN7PUhpDB0.JlYYJX7uOFwGV6INj0ng > div > div > div:nth-child(2) > div > div > div:nth-child(1) > div > div > div > div > div.chat-uikit-message-box-container__message > div.chat-uikit-message-box-container__message__message-box__footer > div > div.tTSrEd1mQwEgF4_szmBb > div:nth-child(3) > div > div',
    'com-tokens-css': '#root > div:nth-child(2) > div > div > div > div > div.aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.UMf9npeM8cVkDi0CDqZ0 > div.TH9DlQU1qwg_KGXdDYzk > div > div.R_WS6aCLs2gN7PUhpDB0.JlYYJX7uOFwGV6INj0ng > div > div > div.nIVxVV6ZU7gCM5i4VQIL.message-group-wrapper > div > div > div:nth-child(1) > div > div > div > div > div.chat-uikit-message-box-container__message > div > div.chat-uikit-message-box-container__message__message-box__footer > div > div.tTSrEd1mQwEgF4_szmBb > div:nth-child(3) > div',

    'cn-tokens-css': '#root > div:nth-child(2) > div > div > div > div > div.container--aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.wrapper-single--UMf9npeM8cVkDi0CDqZ0 > div.message-area--TH9DlQU1qwg_KGXdDYzk > div > div.scroll-view--R_WS6aCLs2gN7PUhpDB0.scroll-view--JlYYJX7uOFwGV6INj0ng > div > div > div.wrapper--nIVxVV6ZU7gCM5i4VQIL.message-group-wrapper > div > div > div:nth-child(1) > div > div > div > div > div.chat-uikit-message-box-container__message > div > div.chat-uikit-message-box-container__message__message-box__footer > div > div.message-info-text--tTSrEd1mQwEgF4_szmBb > div:nth-child(3) > div > div',
    # 内容
    'com-content-gemini-xpath': '//pre[@class="language-json light-scrollbar_c982f"]',
    'com-content-gpt-xpath': '//div[@class="auto-hide-last-sibling-br paragraph_9dc5f paragraph-element"]',

    'cn-content-xpath': '//div[@class="auto-hide-last-sibling-br paragraph_4183d"]',
    # 重新生成
    'com-regenerate-css': '#root > div:nth-child(2) > div > div > div > div > div.aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.UMf9npeM8cVkDi0CDqZ0 > div.TH9DlQU1qwg_KGXdDYzk > div > div.R_WS6aCLs2gN7PUhpDB0.JlYYJX7uOFwGV6INj0ng > div > div > div.nIVxVV6ZU7gCM5i4VQIL.message-group-wrapper > div > div > div:nth-child(1) > div > div > div > div > div.chat-uikit-message-box-container__message > div > div.chat-uikit-message-box-container__message__message-box__footer > div > div.semi-space.semi-space-align-center.semi-space-horizontal > div:nth-child(3) > button',
                        # '#root > div:nth-child(2) > div > div > div > div > div.aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.UMf9npeM8cVkDi0CDqZ0 > div.TH9DlQU1qwg_KGXdDYzk > div > div.R_WS6aCLs2gN7PUhpDB0.JlYYJX7uOFwGV6INj0ng > div > div > div.nIVxVV6ZU7gCM5i4VQIL.message-group-wrapper > div > div > div:nth-child(1) > div > div > div > div > div.chat-uikit-message-box-container__message > div > div.chat-uikit-message-box-container__message__message-box__footer > div > div.semi-space.semi-space-align-center.semi-space-horizontal > div:nth-child(2) > button',

    'cn-regenerate-css': '#root > div:nth-child(2) > div > div > div > div > div.container--aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.wrapper-single--UMf9npeM8cVkDi0CDqZ0 > div.message-area--TH9DlQU1qwg_KGXdDYzk > div > div.scroll-view--R_WS6aCLs2gN7PUhpDB0.scroll-view--JlYYJX7uOFwGV6INj0ng > div > div > div.wrapper--nIVxVV6ZU7gCM5i4VQIL.message-group-wrapper > div > div > div:nth-child(1) > div > div > div > div > div.chat-uikit-message-box-container__message > div > div.chat-uikit-message-box-container__message__message-box__footer > div > div.semi-space.semi-space-align-center.semi-space-horizontal > div:nth-child(2)',

    # 切换模型
    'com-change-css': '#root > div:nth-child(2) > div > div > div > div > div.aSIvzUFX9dAs4AK6bTj0 > div.sidesheet-container.UMf9npeM8cVkDi0CDqZ0 > div.IoQhh3vVUhwDTJi9EIDK > div.LxUy6g0wgIgIWCGkQHkC.coz-bg-plus.coz-fg-secondary.Zo84sv5CjcC2ObBGEGDy.SKIazToEhtUg8ZweE2b6 > div.IJW2oexGFSA7_n0W_IUb > div > div.PLf9q929mqs4ZlidOgkc > button',

    # 下拉列表
    'com-model-list-xpath': '//div[@class="qNKhq1IFxcAMuhECsZz9"]',

    # 选择模型
    'com-gemini-1.5-flash-xpath': '//div[@_scrollindex="0"]',  # '//div[contains(@id, "-option-0")]',
    'com-gemini-1.5-pro-xpath': '//div[@_scrollindex="1"]',  # '//div[contains(@id, "-option-1")]',
    'com-gpt-4o-xpath': '//div[@_scrollindex="2"]',  # '//div[contains(@id, "-option-2")]',
    'com-gpt-4-turbo-xpath': '//div[@_scrollindex="3"]',  # '//div[contains(@id, "-option-3")]',

    # 模型输出文本长度
    'com-text-length-xpath': '//input[@class="semi-input semi-input-default" and @aria-valuemin="1"]',
    'com-text-length-ensure-1-xpath': '//input[@class="semi-input semi-input-default" and @aria-valuemin="1" and @aria-valuenow="8192"]',
    'com-text-length-ensure-2-xpath': '//input[@class="semi-input semi-input-default" and @aria-valuemin="1" and @aria-valuenow="4096"]'
}

headers = Headers(headers=True).generate()

# api_headers = {
#     'Authorization': 'Bearer pat_BeF0pCJRtoqSyqesxUUtaQgajL9EAcHzRjlI1ZqFyhGhFV3mTRfIuyKvU5nRHhIB',
#     'Content-Type': 'application/json',
#     'Accept': '*/*',
#     'Host': 'api.coze.com',
#     'Connection': 'keep-alive',
# }

prompt = {
    'prompt_0': """
                # 角色
                你是一位卓越非凡的AI简历信息提取专家，擅长从HTML格式的简历中精准抽取关键信息
                
                ## 技能
                ### 技能1: 筛选姓名
                - 从提供的简历中找出姓名。
                
                ### 技能2: 筛选联系电话
                - 从简历中找出联系电话，如果没有则返回空白。
                
                ### 技能3: 筛选邮箱
                - 从简历中找出邮箱，如果没有则返回空白。并且将格式不规范的@转为@。
                
                ### 技能4: 筛选职称
                - 从简历中找出职称，如果没有则返回空白。
                
                ### 技能5: 抽取个人简介
                - 从简历中识别并提取出个人简介，如果没有则返回空白。
                
                ### 技能6: 确认研究方向
                - 根据简历中的信息确定申请者的研究方向，如果没有则返回空白。
                
                ### 技能7: 筛选专利
                - 从简历中找出详细的专利信息，如果没有则返回空白。
                
                ### 技能8: 筛选科研项目
                - 从简历中找出科研项目信息，如果没有则返回空白。
                
                ### 技能9: 筛选荣誉/获奖信息
                - 从简历中找出荣誉或获奖信息，如果没有则返回空白。
                
                ### 技能10: 筛选简历图片地址
                - 从提供的简历中找出图片地址，如果没有则返回空白。
                
                ### 技能11: 筛选最高学历
                - 从简历中找出最高学历，如果没有则返回空白。
                
                ### 技能12: 筛选最高学位
                - 从简历中找出最高学位，如果没有则返回空白。
                
                ### 技能13: 筛选职位
                - 从简历中找出职位信息，如果没有则返回空白。
                
                ### 技能14: 筛选办公地点
                - 从简历中找出办公地点信息，如果没有则返回空白。
                
                ### 技能15: 筛选个人网址
                - 从简历中找出个人网址链接，如果没有则返回空白。
                
                ### 技能16: 筛选论文
                - 从简历中找出论文信息，如果没有则返回空白。
                
                ### 技能17: 筛选教育经历、工作经历
                - 从简历中找出工作经历和教育经历，如果没有则返回空白。
                
                ## 约束条件
                - 返回结果不要携带任何html标签。
                - 对于无法找到的信息，返回空白值。
                - 不修改原始简历的内容，遵循规定规则执行。
                - 只负责处理与简历关键信息提取有关的任务，无需处理其他信息。
                - 如果论文数量过多，只返回前5篇。
                - 如果有多个电话号码或邮箱地址，以逗号隔开。
                - 如果有多篇论文，每篇论文占一行，并按序号标记。
                - 如果有多项研究方向信息、专利信息、科研项目信息或获奖信息，每项信息占一行，并按序号标记。
                - 最高学位范围包括博士、硕士、学士。
                - 根据最高学位推断最高学历，最高学历范围包括大专、本科、研究生。
                - 只返回一个包含所需信息的json格式字符串，不添加任何多余文字。
                - 所有信息均以中文形式返回。
                - 不包含要求格式外的任何文字。
                - 不包含任何注释信息。
                
                ## 输出格式
                - 严格按照以下格式返回数据
                    {
                        "姓名":"",
                        "电话":"",
                        "邮箱":"",
                        "职称":"",
                        "个人简介":"",
                        "研究方向":[],
                        "专利":[],
                        "科研项目":[],
                        "荣誉/获奖":[],
                        "照片地址":"",
                        "最高学历":"",
                        "最高学位":"",
                        "职位":"",
                        "办公地点":"",
                        "个人网址":"",
                        "科研论文":[],
                        "教育经历":[],
                        "工作经历":[]
                    }
                """,
    'prompt_1': """
                # 角色
                你是一位卓越非凡的AI简历信息提取专家，擅长从HTML格式的简历中精准抽取关键信息
                
                ## 技能
                ### 技能1: 筛选姓名
                - 从提供的简历中找出姓名。
                
                ### 技能2: 筛选联系电话
                - 从简历中找出联系电话，如果没有则返回空白。
                
                ### 技能3: 筛选邮箱
                - 从简历中找出邮箱，如果没有则返回空白。并且将格式不规范的@转为@。
                
                ### 技能4: 筛选职称
                - 从简历中找出职称，如果没有则返回空白。
                
                ### 技能5: 确认研究方向
                - 根据简历中的信息确定申请者的研究方向，如果没有则返回空白。

                ### 技能6: 筛选专利
                - 从简历中找出详细的专利信息，如果没有则返回空白。
                
                ### 技能7: 筛选简历图片地址
                - 从提供的简历中找出图片地址，如果没有则返回空白。
                
                ### 技能8: 筛选最高学历
                - 从简历中找出最高学历，如果没有则返回空白。
                
                ### 技能9: 筛选最高学位
                - 从简历中找出最高学位，如果没有则返回空白。
                
                ### 技能10: 筛选职位
                - 从简历中找出职位信息，如果没有则返回空白。
                
                ### 技能11: 筛选办公地点
                - 从简历中找出办公地点信息，如果没有则返回空白。
                
                ### 技能12: 筛选个人网址
                - 从简历中找出个人网址链接，如果没有则返回空白。
                
                ## 约束条件
                - 返回结果不要携带任何html标签。
                - 对于无法找到的信息，返回空白值。
                - 不修改原始简历的内容，遵循规定规则执行。
                - 只负责处理与简历关键信息提取有关的任务，无需处理其他信息。
                - 如果有多个电话号码或邮箱地址，以逗号隔开。
                - 如果有多项研究方向信息、专利信息，每项信息占一行，并按序号标记。
                - 最高学位范围包括博士、硕士、学士。
                - 根据最高学位推断最高学历，最高学历范围包括大专、本科、研究生。
                - 只返回一个包含所需信息的json格式字符串，不添加任何多余文字。
                - 所有信息均以中文形式返回。
                - 不包含要求格式外的任何文字。
                - 不包含任何注释信息。
                
                ## 输出格式
                - 严格按照以下格式返回数据
                    {
                        "姓名":"",
                        "电话":,
                        "邮箱":"",
                        "职称":"",
                        "研究方向":[],
                        "专利":[],
                        "照片地址":"",
                        "最高学历":"",
                        "最高学位":"",
                        "职位":"",
                        "办公地点":"",
                        "个人网址":""
                    }
                """,
    'prompt_2': """
                # 角色
                你是一位卓越非凡的AI简历信息提取专家，擅长从HTML格式的简历中精准抽取关键信息
                
                ### 技能1: 筛选科研项目
                - 从简历中找出科研项目信息，如果没有则返回空白。
                
                ### 技能2: 筛选荣誉/获奖信息
                - 从简历中找出荣誉或获奖信息，如果没有则返回空白。
                
                ## 约束条件
                - 返回结果不要携带任何html标签。
                - 对于无法找到的信息，返回空白值。
                - 不修改原始简历的内容，遵循规定规则执行。
                - 只负责处理与简历关键信息提取有关的任务，无需处理其他信息。
                - 如果有多项科研项目信息或获奖信息，每项信息占一行，并按序号标记。
                - 只返回一个包含所需信息的json格式字符串，不添加任何多余文字。
                - 所有信息均以中文形式返回。
                - 不包含要求格式外的任何文字。
                - 不包含任何注释信息。
                
                ## 输出格式
                - 严格按照以下格式返回数据
                    {
                        "姓名":"",
                        "科研项目":[],
                        "荣誉/获奖":[]
                    }
                """,
    'prompt_3': """
                # 角色
                你是一位卓越非凡的AI简历信息提取专家，擅长从HTML格式的简历中精准抽取关键信息
                
                ### 技能1: 抽取个人简介
                - 从简历中识别并提取出个人简介，如果没有则返回空白。
                
                ### 技能2: 筛选论文
                - 从简历中找出论文信息，如果没有则返回空白。
                
                ### 技能3: 筛选教育经历、工作经历
                - 从简历中找出工作经历和教育经历，如果没有则返回空白。
                
                ## 约束条件
                - 返回结果不要携带任何html标签。
                - 对于无法找到的信息，返回空白值。
                - 不修改原始简历的内容，遵循规定规则执行。
                - 只负责处理与简历关键信息提取有关的任务，无需处理其他信息。
                - 如果论文数量过多，只返回前10篇。
                - 如果有多篇论文，每篇论文占一行，并按序号标记。
                - 只返回一个包含所需信息的json格式字符串，不添加任何多余文字。
                - 所有信息均以中文形式返回。
                - 不包含要求格式外的任何文字。
                - 不包含任何注释信息。
                
                ## 输出格式
                - 严格按照以下格式返回数据
                    {
                        "姓名":"",
                        "个人简介":"",
                        "科研论文":[],
                        "教育经历":[],
                        "工作经历":[]
                    }
                """
}

api_info = {
    'deepseek': {
        'headers': {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': 'Bearer sk-ea1b90265dc2408ab7bf61e0dee759aa'
        },
        'payload': {
            "messages": [{
                  "content": None,
                  "role": "system"
                },
                {
                  "content": None,
                  "role": "user"
                }],
            "model": "deepseek-chat",
            "frequency_penalty": 0,
            "max_tokens": 8192,
            "presence_penalty": 0,
            "stop": None,
            "stream": False,
            "temperature": 1,
            "top_p": 1,
            "logprobs": False,
            "top_logprobs": None
        }
    }
}

semaphore = Semaphore(5)
semaphore_api = Semaphore(20)

api_base_url = 'https://api.deepseek.com/beta/chat/completions'

sf_password = 'Shufang_@919'

# engine = create_engine('mysql+pymysql://root:123456@127.0.0.1:3306/alpha_search?charset=utf8')
local_engine = create_engine('mysql+pymysql://root:123456@127.0.0.1:3306/alpha_search?charset=utf8mb4')
sf_engine = create_engine(f'mysql+pymysql://root:{quote_plus(sf_password)}@192.168.2.12:3306/alpha_search_update?charset=utf8mb4')

csv_header = ['name', 'institute_id', 'laboratory_id', 'phone', 'email', 'job_title', 'abstracts', 'directions', 'talent_title', 'administrative_title', 'education_experience', 'work_experience', 'patent', 'project', 'award', 'paper', 'social_job', 'picture', 'education', 'qualification', 'job_information', 'responsibilities', 'office_address', 'origin', 'results', 'publication', 'personal_website', 'parent_id']


def get_response(url, cn_com):
    logger.info(f'crawling {url}...')
    if cn_com == 'com':
        resp = requests.get(url, headers=headers, proxies=proxy)
    else:
        resp = requests.get(url, headers=headers)
    if resp.status_code == 200:
        resp.encoding = 'utf-8'
        logger.info(f'successfully get {url}')
        return resp.text
    logger.warning(f'can not get {url}')


async def get_response_async(url, session, **kwargs):
    async with semaphore:
        try:
            logger.info(f'async crawling {url}...')
            async with session.get(url, headers=headers) as resp:
                if resp.ok:
                    try:
                        return await resp.text(encoding='utf-8'), url, [*zip(kwargs.keys(), kwargs.values())]
                    except:
                        logger.warning('utf-8编码有问题，手动解码')
                        resp = await resp.read()
                        resp = resp.decode('utf-8', errors='ignore')
                        return resp, url, [*zip(kwargs.keys(), kwargs.values())]
                else:
                    print('请求失败', url)
        except:
            print(f'{url}请求出错，可能是超时')

async def get_api_resp(session, data, api_headers):
    # async with semaphore_api:
    logger.info('request api to parse')
    # async with session.post(api_base_url, data=json.dumps(data), headers=api_headers,proxy='http://127.0.0.1:7890') as resp:
    try:
        async with session.post(api_base_url, data=json.dumps(data), headers=api_headers) as resp:
            if resp.ok:
                return await resp.json()
    except Exception as e:
        logger.error('错误，即将重试或记录...')


def api_payload_info(api, turn, content):
    if api == 'deepseek':
        api_info['deepseek']['payload']['messages'][0]['content'] = prompt[f'prompt_{turn}']
        api_info['deepseek']['payload']['messages'][1]['content'] = content
        payload = api_info['deepseek']['payload']
        return payload

def list2str(result: dict):
    for key in result.keys():
        if isinstance(result[key], list):
            # 添加标号
            # for i in range(len(result[key])):
            #     result[key][i] = f'[{i + 1}] ' + result[key][i]

            # if key in ('邮箱', '职称'):
            #     result[key] = ','.join(result[key])
            # else:
            temp_res = []
            for item in result[key]:
                if isinstance(item, dict):
                    for child_key, child_value in item.items():
                        temp_res.append(child_key + ':' + child_value)
            if temp_res:
                result[key] = '\n'.join(temp_res)
            else:
                result[key] = '\n'.join(result[key])
    return result

def get_format_result(turn, content: dict, result_direct: dict, partition_num, img_url_head):
    if turn == 1:
        phone = content.get('电话')
        phone = re.sub(r'，|\\|/|;', ',', phone)
        phone = re.sub(r'\s', '', phone)
        phone = clean_phone(partition_num, phone)

        email = content.get('邮箱')
        email = re.sub(r'，|\\|/|;', ',', email)
        email = re.sub(r'\s', '', email)
        result = {
            'phone': phone if phone else result_direct['phone'],
            # 'phone': result_direct['phone'],
            'email': email if email else result_direct['email'],
            # 'email': result_direct['email'],
            'job_title': content.get('职称') if content.get('职称') else result_direct['job_title'],
            'directions': content.get('研究方向') if content.get('研究方向') else result_direct['directions'],
            'talent_title': result_direct['talent_title'],
            'administrative_title': result_direct['administrative_title'],
            'education_experience': content.get('教育经历') if content.get('教育经历') else result_direct['education_experience'],
            'work_experience': content.get('工作经历') if content.get('工作经历') else result_direct['work_experience'],
            'patent': content.get('专利') if content.get('专利') else result_direct['patent'],
            'social_job': content.get('社会兼职') if content.get('社会兼职') else result_direct['social_job'],
            'picture': None if not content.get('照片地址') else parse.urljoin(result_direct['origin'], content.get('照片地址')) if not img_url_head else parse.urljoin(img_url_head, content.get('照片地址')),
            # 'picture': result_direct['picture'],
            'education': result_direct['education'],
            'qualification': result_direct['qualification'],
            'job_information': result_direct['job_information'],
            'responsibilities': content.get('职位') if content.get('职位') else result_direct['responsibilities'],
            'office_address': content.get('办公地点') if content.get('办公地点') else result_direct['office_address'],
            'origin': result_direct['origin'],
            'results': result_direct['talent_title'],
            'publication': result_direct['publication'],
            'personal_website': content.get('个人网址') if content.get('个人网址') else result_direct['personal_website'],
            'parent_id': result_direct['parent_id']
        }
    elif turn == 2:
        result = {
            'project': content.get('科研项目') if content.get('科研项目') else result_direct['project'],
            'award': content.get('荣誉/获奖') if content.get('荣誉/获奖') else result_direct['award']
        }
    elif turn == 3:
        result = {
            'abstracts': content.get('个人简介') if content.get('个人简介') else result_direct['abstracts'],
            'paper': content.get('科研论文') if content.get('科研论文') else result_direct['paper']
        }
    else:
        phone = content.get('电话')
        phone = re.sub(r'，|\\|/|;', ',', phone)
        phone = re.sub(r'\s', '', phone)
        phone = clean_phone(partition_num, phone)

        email = content.get('邮箱')
        email = re.sub(r'，|\\|/|;', ',', email)
        email = re.sub(r'\s', '', email)
        result = {
            'name': result_direct['name'],
            'institute_id': result_direct['institute_id'],
            'laboratory_id': result_direct['laboratory_id'],
            'phone': phone if phone else result_direct['phone'],
            # 'phone': result_direct['phone'],
            'email': email if email else result_direct['email'],
            # 'email': result_direct['email'],
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
            'picture': None if not content.get('照片地址') else parse.urljoin(result_direct['origin'], content.get('照片地址')) if not img_url_head else parse.urljoin(img_url_head, content.get('照片地址')),
            # 'picture': result_direct['picture'],
            'education': result_direct['education'],
            'qualification': result_direct['qualification'],
            'job_information': result_direct['job_information'],
            'responsibilities': content.get('职位') if content.get('职位') else result_direct['responsibilities'],
            'office_address': content.get('办公地点') if content.get('办公地点') else result_direct['office_address'],
            'origin': result_direct['origin'],
            'results': result_direct['results'],
            'publication': result_direct['publication'],
            'personal_website': content.get('个人网址') if content.get('个人网址') else result_direct['personal_website'],
            'parent_id': result_direct['parent_id']
        }
    return result

def gpt_api(result_gen, cn_com, partition_num):
    from crawler import ReCrawler
    crawler_ins = ReCrawler(partition_num=partition_num, institute_name='', laboratory_name='', institute_id=None, laboratory_id=None, cn_com='com', start_urls=[], a_s_xpath_str='', target_div_xpath_str='', name_filter_re='')
    parser = GptParser(cn_com)
    driver = parser.init_driver()
    crawler_ins.driver = driver
    crawler_ins.gpt_cant = []
    text, result_direct = result_gen
    result_gpt = crawler_ins.parse_detail_gpt(text, result_direct)
    # print(result_gpt)
    driver.close()
    if crawler_ins.gpt_cant:
        return 'failed', result_direct['name']
    else:
        return result_gpt

async def api_parse(result_gen, session, partition_num, img_url_head, cn_com):
    async with semaphore_api:
        # for unpack in result_gen:
        content_with_label, result_direct = result_gen
        # print(result_direct['name'])
        # todo: 切换api的逻辑
        # for count in range(3):  # 切换api
        #     api = list(api_info.keys())[count]
        api = 'deepseek'
        # todo:请求一次尝试，失败分段尝试
        try:
            logger.info(f'{result_direct["name"]}尝试只请求一次...')
            api_result = await get_api_resp(session, data=api_payload_info(api, 0, content_with_label), api_headers=api_info[api]['headers'])
            if api_result:
                # if result_direct["name"] == '汤方栋':
                #     print(api_result)
                # print(api_result)
                try:
                    api_result = api_result['choices'][0]['message']['content']
                    api_result = re.sub(r'^.*?(\{.*}).*?$', r'\1', api_result, flags=re.S)
                    api_result = re.sub(r'(,\s*)(?=}$)', '', api_result, flags=re.S)
                    api_result = re.sub(r'<.*?>', '', api_result)
                    # api_result = json.loads(api_result, strict=False)
                    api_result = demjson3.decode(api_result)
                    api_result = list2str(api_result)
                    result = get_format_result(0, api_result, result_direct, partition_num, img_url_head)
                    # print(api_result)
                    # return api_result
                except:
                    logger.warning(f'{result_direct["name"]}尝试只请求一次失败--解析层，可能是文本过长导致json返回不完整，即将分段请求...')
                    raise
            else:  # 400或者返回了None
                logger.warning(f'{result_direct["name"]}尝试只请求一次失败--请求层，即将重试...')
                logger.info(f'{result_direct["name"]}再次尝试一次性请求...')
                api_result = await get_api_resp(session, data=api_payload_info(api, 0, content_with_label),api_headers=api_info[api]['headers'])
                if api_result:
                    try:
                        api_result = api_result['choices'][0]['message']['content']
                        api_result = re.sub(r'^.*?(\{.*}).*?$', r'\1', api_result, flags=re.S)
                        api_result = re.sub(r'(,\s*)(?=}$)', '', api_result, flags=re.S)
                        api_result = re.sub(r'<.*?>', '', api_result)
                        # api_result = json.loads(api_result, strict=False)
                        api_result = demjson3.decode(api_result)
                        api_result = list2str(api_result)
                        result = get_format_result(0, api_result, result_direct, partition_num, img_url_head)
                        # print(api_result)
                        # return api_result
                    except:
                        logger.warning(f'{result_direct["name"]}第二次尝试只请求一次失败--解析层，可能是文本过长导致json返回不完整，即将分段请求...')
                        raise
                else:  # 400或者返回了None
                    logger.info(f'{result_direct["name"]}第二次尝试只请求一次失败--请求层,即将尝试分段请求...')
                    raise
                    # result = gpt_api(result_gen, cn_com, partition_num)

        except:
            result = {
                        'name': result_direct['name'],
                        'institute_id': result_direct['institute_id'],
                        'laboratory_id': result_direct['laboratory_id'],
                        'phone': '',
                        'email': '',
                        'job_title': '',
                        'abstracts': '',
                        'directions': '',
                        'talent_title': '',
                        'administrative_title': '',
                        'education_experience': '',
                        'work_experience': '',
                        'patent': '',
                        'project': '',
                        'award': '',
                        'paper': '',
                        'social_job': '',
                        'picture': '',
                        'education': '',
                        'qualification': '',
                        'job_information': '',
                        'responsibilities': '',
                        'office_address': '',
                        'origin': result_direct['origin'],
                        'results': '',
                        'publication': '',
                        'personal_website': '',
                        'parent_id': ''
                    }
            for turn in range(1, 4):
                logger.info(f'{result_direct["name"]}第{turn}段请求')
                try:
                    api_result = await get_api_resp(session, data=api_payload_info(api, turn, content_with_label), api_headers=api_info[api]['headers'])
                    if api_result:  # 返回了结果
                        # if result_direct["name"] == '汤方栋':
                        #     print(api_result)
                        try:
                            api_result = api_result['choices'][0]['message']['content']
                            api_result = re.sub(r'^.*?(\{.*}).*?$', r'\1', api_result, flags=re.S)
                            api_result = re.sub(r'(,\s*)(?=}$)', '', api_result, flags=re.S)
                            api_result = re.sub(r'<.*?>', '', api_result)
                            # api_result = json.loads(api_result, strict=False)
                            api_result = demjson3.decode(api_result)
                            api_result = list2str(api_result)
                            api_result = get_format_result(turn, api_result, result_direct, partition_num, img_url_head)
                            # print(api_result)
                            result.update(api_result)
                        except:
                            logger.warning(f'{result_direct["name"]}第{turn}段返回内容解析失败--解析层，可能返回内容过长，即将重试...')
                            raise
                    else:  # 400
                        logger.warning(f'{result_direct["name"]}第{turn}段请求失败--请求层，即将重试...')
                        raise
                        # api_result = await get_api_resp(session, data=api_payload_info(api, turn, content_with_label), api_headers=api_info[api]['headers'])
                        # if api_result:  # 返回了结果
                        #     pass
                        # else:  # 400
                        #     logger.warning(f'{result["name"]}请求失败')
                        #     return 'failed', result['name']
                except:
                    logger.warning(f'{result_direct["name"]}第{turn}段请求出错重试中...')
                    api_result = await get_api_resp(session, data=api_payload_info(api, turn, content_with_label),api_headers=api_info[api]['headers'])
                    if api_result:  # 返回了结果
                        try:
                            api_result = api_result['choices'][0]['message']['content']
                            api_result = re.sub(r'^.*?(\{.*}).*?$', r'\1', api_result, flags=re.S)
                            api_result = re.sub(r'(,\s*)(?=}$)', '', api_result, flags=re.S)
                            api_result = re.sub(r'<.*?>', '', api_result)
                            # api_result = json.loads(api_result, strict=False)
                            api_result = demjson3.decode(api_result)
                            api_result = list2str(api_result)
                            api_result = get_format_result(turn, api_result, result_direct, partition_num, img_url_head)
                            # print(api_result)
                            result.update(api_result)
                        except:
                            logger.warning(f'{result_direct["name"]}第{turn}段重试，返回内容依然解析失败--解析层，可能内容过长将记录，手动处理')
                            return 'failed', result_direct['name']
                    else:
                        logger.warning(f'{result_direct["name"]}第{turn}段第二次尝试请求失败--请求层，将记录...')
                        return 'failed', result_direct['name']
        if not isinstance(result, tuple):  # gpt可能失败，返回一个元组
            print(result)
        return result


def result_dict_2_df(empty_df, result: dict):
    """
    将返回的字典数据转化为df，方便去重操作
    :param empty_df:
    :param result:
    :return:
    """
    temp_df = pd.DataFrame([result])
    result_df = pd.concat([empty_df, temp_df], ignore_index=True)
    return result_df


def df2mysql(engine, df, table_name):
    # engine = create_engine(engine_string)
    with engine.begin() as conn:
        df.to_sql(name=table_name, con=conn, if_exists='append', index=False)


def save_as_json(df, school_name, college_name, path=None):
    if not path:
        path = f'../Results/{school_name}'
    # 手动保存csv时，路径要变化
    else:
        path = f'./Results/{school_name}'
    os.path.exists(path) or os.makedirs(path)

    # json_format = df.drop('id', axis=1).to_dict('records')
    json_format = df.to_dict('records')

    with open(path + f'/{college_name}.json', mode='w', encoding='utf-8') as f:
        f.write(json.dumps(json_format, ensure_ascii=False))



def get_split(element):
    """
    分割同一字段中的多个号码
    :param element:
    :return:
    """
    try:
        element = element.lower().split(',')
        return element
    except:
        return [element]

def get_most_info(df):
    """
    保留信息量最大的一条记录
    :param df:
    :return:
    """
    df['info_num'] = df.notnull().sum(axis=1)
    df.sort_values(by='info_num', inplace=True, ascending=False)
    df.drop('info_num', axis=1, inplace=True)
    return df.iloc[0]

def drop_duplicates(df):
    """
    使用集合去重。
    从第一个集合开始遍历，遇到存在交集的记录将其标记为重复
    下一轮遍历，跳过有标记的记录，直至结束
    按照标记分组，取信息量最大的一条记录
    :param df:
    :return:
    """
    if len(df) == 1:
        return df

    df['duplicate'] = None
    get_split_info = df[['phone', 'email']].applymap(get_split).values.tolist()  # 分割同一字段中的多个号码或邮箱地址
    info_set_list = [*map(lambda x: set(chain.from_iterable(x)), get_split_info)]  # 将多个号码整合为一个集合，每个号码为一个单独的元素
    # info_set_list = [*map(lambda x: set(x), df[['phone', 'email']].values.tolist())]
    for i in range(len(df)):
        if df.iloc[i].duplicate == None:
            for j in range(i+1, len(df)):
                if info_set_list[i] & info_set_list[j]:
                    df.duplicate.iloc[i] = i
                    df.duplicate.iloc[j] = i
                else:
                    df.duplicate.iloc[i] = i
        if i == len(df)-1:
            break

#         df.drop_duplicates('duplicate', inplace=True)  # 简单处理，保留第一条记录，不一定信息量最多
    df = df.groupby('duplicate', as_index=False, group_keys=False, dropna=False).apply(get_most_info)
    df.drop('duplicate', axis=1, inplace=True)

    return df

def drop_duplicate_collage(df):
    """
    按照姓名 groupby， 用 phone 和 email 构成的集合进行去重
    各记录集合若产生交集，则表明记录重复
    最后按照重复记录的缺失值情况，保留缺失值最小的一条记录
    :param df:
    :return:
    """
    df_temp = df.drop_duplicates()
    if len(df_temp) == df.name.nunique():
        return df_temp

    df_temp = df.groupby('name', as_index=False, group_keys=False, dropna=False).apply(drop_duplicates)
    return df_temp


def truncate_table(host, user, password: str, database, port, table_name):
    conn = pymysql.connect(host=host, user=user, password=password, database=database, port=port, charset='utf8')
    cursor = conn.cursor()
    sql = f'truncate table {table_name}'
    try:
        cursor.execute(sql)
        logger.info(f'truncate {table_name} successfully')
    except:
        logger.info('truncate {table_name} failed, rollback')
        conn.rollback()


def clean_phone(partition_num: str, dirty_phone: str):
    if not dirty_phone:
        return None

    if ',' not in dirty_phone:
        # 格式化
        phone = re.sub(r'—|~', '-', dirty_phone)
        phone = re.sub(r'\s', '', phone)
        phone = re.sub(r'(?<!^)(?:\(|（).*?(?:）|\))$', '', phone)
        phone = re.sub(r'(?:（|\()(.*?)(?:\)|）)', r'\1', phone)
        phone = re.sub(r'^\+?(?:（|\()?86(?:）|\))?\+?-?', '', phone)
        phone = re.sub(r'\+', '-', phone)

        try:
            int(re.sub('-|^0|,', '', phone))
        except:
            return None

        if len(phone) > 13 and re.search(r'-\d{3,4}$', phone):
            phone = re.sub(r'-\d{3,4}$', '', phone)

        # 识别错误区号
        if re.match('^' + partition_num, phone) or re.match('^' + partition_num[1:], phone):
            pass
        else:
            if not re.match('^' + partition_num, phone) and phone.startswith('0'):
                if '-' in phone:
                    return phone
                if '-' not in phone and (phone.replace('0', '').startswith('1') or phone.replace('0', '').startswith('2')):
                    return re.sub(r'(^\d{3})', r'\1-', phone)
                else:
                    return re.sub(r'(^\d{4})', r'\1-', phone)
            elif (not re.match('^' + partition_num[1:], phone) and (len(re.sub(r'-', '', phone)) != 11 or len(re.sub(r'-', '', phone)) == 11 and not phone.startswith('1')) and len(re.sub(r'-', '', phone)) > 8):
                if '-' in phone:
                    return '0' + phone
                if '-' not in phone and (phone.startswith('1') or phone.startswith('2')):
                    return '0' + re.sub(r'(^\d{2})', r'\1-', phone)
                else:
                    return '0' + re.sub(r'(^\d{3})', r'\1-', phone)


        phone = re.sub('-', '', phone)

        if re.match('^' + partition_num, phone):
            phone = re.sub('^' + partition_num, partition_num + '-', phone)
        elif re.match('^' + partition_num[1:], phone):
            phone = re.sub('^' + partition_num[1:], partition_num + '-', phone)
        elif len(phone) == 7 or len(phone) == 8:
            phone = partition_num + '-' + phone
        else:
            pass
        # elif len(phone) == 11 and phone.startswith('1'):
        #     pass
        # else:
        #     phone = None
        return phone
    else:
        res_phone = ''
        for num in dirty_phone.split(','):

            # 格式化
            num = re.sub(r'—|~', '-', num)
            num = re.sub(r'\s', '', num)
            num = re.sub(r'(?<!^)(?:\(|（).*?(?:）|\))$', '', num)
            num = re.sub(r'(?:（|\()(.*?)(?:\)|）)', r'\1', num)
            num = re.sub(r'^\+?(?:（|\()?86(?:）|\))?\+?-?', '', num)
            num = re.sub(r'\+', '-', num)

            try:
                int(re.sub('-|^0|,', '', num))
            except:
                return None

            # 识别错误区号
                # 识别错误区号
            if re.match('^' + partition_num, num) or re.match('^' + partition_num[1:], num):
                pass
            else:
                if not re.match('^' + partition_num, num) and num.startswith('0'):
                    if '-' in num:
                        pass
                    elif '-' not in num and (num.replace('0', '').startswith('1') or num.replace('0', '').startswith('2')):
                        num = re.sub(r'(^\d{3})', r'\1-', num)
                    else:
                        num = re.sub(r'(^\d{4})', r'\1-', num)

                    if not res_phone:  # 防止开头多一个逗号
                        res_phone += num
                    else:
                        res_phone = res_phone + ',' + num
                    continue
                elif (not re.match('^' + partition_num[1:], num) and (len(re.sub(r'-', '', num)) != 11 or len(re.sub(r'-', '', num)) == 11 and not num.startswith('1')) and len(re.sub(r'-', '', num)) > 8):
                    if '-' in num:
                        num = '0' + num
                    elif '-' not in num and (num.startswith('1') or num.startswith('2')):
                        num = '0' + re.sub(r'(^\d{2})', r'\1-', num)
                    else:
                        num = '0' + re.sub(r'(^\d{3})', r'\1-', num)

                    if not res_phone:  # 防止开头多一个逗号
                        res_phone += num
                    else:
                        res_phone = res_phone + ',' + num
                    continue

            # 区号正常
            if len(num) > 13 and re.search(r'-\d{3,4}$', num):
                num = re.sub(r'-\d{3,4}$', '', num)
            num = re.sub('-', '', num)

            if re.match('^' + partition_num, num):
                num = re.sub('^' + partition_num, partition_num + '-', num)
            elif re.match('^' + partition_num[1:], num):
                num = re.sub('^' + partition_num[1:], partition_num + '-', num)
            elif len(num) == 7 or len(num) == 8:
                num = partition_num + '-' + num
            else:
                pass
            if not res_phone:  # 防止开头多一个逗号
                res_phone += num
            else:
                res_phone = res_phone + ',' + num
        return res_phone


def replace_quotes_in_text(node):
    if node.text and '"' in node.text:
        node.text = node.text.replace('"', '“').replace('"', '”')
    if node.tail and '"' in node.tail:
        node.tail = node.tail.replace('"', '“').replace('"', '”')
    for child in node:
        replace_quotes_in_text(child)

def csv_2_df(path):
    df = pd.read_csv(path, encoding='utf-8', index_col=False).replace(np.nan, None)  # 不replace，则转换为json时空值为NaN，转换后为null
    return df


def change_model(count, driver):
    driver.find_element(By.CSS_SELECTOR, selector.get('com-change-css')).click()
    time.sleep(0.5)
    driver.find_element(By.XPATH, selector.get('com-model-list-xpath')).click()
    time.sleep(0.5)
    action = ActionChains(driver)
    if count == 1:
        driver.find_element(By.XPATH, selector.get('com-gemini-1.5-pro-xpath')).click()
        time.sleep(0.5)
        length_element = driver.find_element(By.XPATH, selector.get('com-text-length-xpath'))
        # 确认修改最大输出长度成功
        while True:
            try:
                length_element.click()
                length_element.send_keys(Keys.CONTROL, 'a')
                length_element.send_keys(Keys.DELETE)
                length_element.send_keys('8192')
                time.sleep(1)
                WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.XPATH, selector.get('com-text-length-ensure-1-xpath'))))
                break
            except:
                continue
        action.send_keys(Keys.ESCAPE).perform()
        time.sleep(0.5)
        logger.info('第二次重新生成，使用Gemini-pro')
    if count == 2:
        driver.find_element(By.XPATH, selector.get('com-gpt-4o-xpath')).click()
        time.sleep(0.5)
        action.send_keys(Keys.ESCAPE).perform()
        logger.info('第三次重新生成，使用gpt-4o')
    if count == 3:
        driver.find_element(By.XPATH, selector.get('com-gpt-4-turbo-xpath')).click()
        time.sleep(0.5)
        action.send_keys(Keys.ESCAPE).perform()
        logger.info('第四次重新生成，使用gpt-4-turbo')

def restore_model(driver):
    driver.find_element(By.CSS_SELECTOR, selector.get('com-change-css')).click()
    time.sleep(0.5)
    driver.find_element(By.XPATH, selector.get('com-model-list-xpath')).click()
    time.sleep(0.5)
    action = ActionChains(driver)

    driver.find_element(By.XPATH, selector.get('com-gemini-1.5-flash-xpath')).click()
    time.sleep(0.5)
    length_element = driver.find_element(By.XPATH, selector.get('com-text-length-xpath'))
    # 确认修改最大输出长度成功
    while True:
        try:
            length_element.click()
            length_element.send_keys(Keys.CONTROL, 'a')
            length_element.send_keys(Keys.DELETE)
            length_element.send_keys('8192')
            time.sleep(1)
            WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.XPATH, selector.get('com-text-length-ensure-1-xpath'))))
            break
        except:
            continue
    action.send_keys(Keys.ESCAPE).perform()
    time.sleep(0.5)
    logger.info('模型已切换回gemini-flash')
