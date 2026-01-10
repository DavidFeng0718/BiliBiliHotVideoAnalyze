# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup

url = "https://www.baidu.com"




# 1. 发请求
response = requests.get(url)
response.encoding = "utf-8"   # 或 gbk / gb2312
print(response.text)
# 2. 看返回内容
html = response.text

# 3. 解析 HTML
soup = BeautifulSoup(html, "lxml")

# 4. 抓标题
title = soup.title.text

print(title)