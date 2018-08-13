from crawlers.common.ThreadPool import ThreadPool
import time
import datetime
from bs4 import BeautifulSoup
import random
import requests
from crawlers.common.db_operation import batch_insert_update_delete, db_query
from concurrent.futures import ThreadPoolExecutor
import base64
import re

PRO = ['192.155.185.43', '192.155.185.153', '192.155.185.171', '192.155.185.197']

USER_AGENT = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24"
]

"""
    IP Agent Free Provider Website
"""
PROXY_LIST_URL = 'https://proxy-list.org/chinese/index.php?p={index}'
PROXY_LIST_COOKIE = 'PHPSESSID=bgcj6a4srcvej1he14saqp4ls1; __utmc=68685392; __utmz=68685392.1533002364.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _ym_uid=1533002371142314242; _ym_d=1533002371; _ym_isad=2; language=chinese; __utma=68685392.1794129556.1533002364.1533013612.1533017349.4; _ym_visorc_24577874=w; __utmt=1; __utmb=68685392.8.10.1533017349'

CODEBUSY_URL = 'https://proxy.coderbusy.com/'
CODEBUSY_COOKIE = 'UM_distinctid=164ef777d1fb10-002d82a1f57f38-47e1039-144000-164ef777d21bb9; CNZZDATA1267483031=1278491219-1533023446-%7C1533023446; Hm_lvt_b5b6cdc2e21a646a6938ae301388c5e5=1533025943; Hm_lpvt_b5b6cdc2e21a646a6938ae301388c5e5=1533025943'

"""
    Testing IP is enable or not
"""
TEST_URL = 'https://www.baidu.com'


class IPAgent(ThreadPool):

    def __init__(self, pool_size):
        super().__init__(pool_size)
        self.ip_agent_proxy_sqls = []
        self.ip_status_update_sqls = []

    def run(self):

        begin = datetime.datetime.now()
        self.rtl_proxy_list()
        end = datetime.datetime.now()
        print("{}: Successful collect data from proxy-list website during {} s.".format(self.__class__.__name__,
                                                                                        (end - begin).seconds))
        begin = datetime.datetime.now()
        self.rtl_codebusy_list()
        end = datetime.datetime.now()
        print("{}: Successful collect data from proxy-codebusy website during {} s.".format(self.__class__.__name__,
                                                                                        (end - begin).seconds))
        begin = datetime.datetime.now()
        self.check_ip_status()
        end = datetime.datetime.now()
        print("{}: Successful test ip-agent status in local DB during {} s.".format(self.__class__.__name__,
                                                                                        (end - begin).seconds))

    def rtl_proxy_list(self):
        try:
            url = PROXY_LIST_URL.format(index=1)
            time.sleep(random.random() * 3)
            response = requests.get(url, proxies={'http': random.choice(PRO)}, headers={
                'User-Agent': random.choice(USER_AGENT),
                'Cookie': PROXY_LIST_COOKIE
            })
            bs_obj = BeautifulSoup(response.text.encode("utf8"), "html.parser", from_encoding='utf-8')

            indexs = bs_obj.find_all(class_='item')

            max_index = 0

            for item in indexs:
                if max_index < int(item.get_text()):
                    max_index = int(item.get_text())

            for page in range(1, max_index + 1):
                with ThreadPoolExecutor(self.pool_size) as executor:
                    executor.submit(self.insert_update_ip_agent_from_proxy_list, page)
            batch_insert_update_delete(self.ip_agent_proxy_sqls)
            print("{}: Successful update ip agent from proxy-list website.".format(self.__class__.__name__))
        except Exception as e:
            print('{}: Failed collect the ip agent from proxy-list website. Error - {}'.format(self.__class__.__name__,
                                                                                               str(e)))
            raise

    def insert_update_ip_agent_from_proxy_list(self, page):
        try:
            url = PROXY_LIST_URL.format(index=page)
            time.sleep(random.random() * 3)

            response = requests.get(url, proxies={'http': random.choice(PRO)}, headers={
                'User-Agent': random.choice(USER_AGENT),
                'Cookie': PROXY_LIST_COOKIE
            })
            bs_obj = BeautifulSoup(response.text.encode("utf8"), "html.parser", from_encoding='utf-8')

            ip_lists = bs_obj.find(class_='table').find_all('ul')

            sql = """
                IF EXISTS (
                    SELECT
                        *
                    FROM
                        ip_agent
                    WHERE
                        ip = '{ip}'
                ) UPDATE ip_agent
                SET ip = '{ip}', port = '{port}', type = '{type}'
                WHERE ip = '{ip}'
                ELSE 
                    INSERT INTO ip_agent (ip, port, type)
                    VALUES('{ip}', '{port}', '{type}')
            """

            for item in ip_lists:
                ip_port = str(base64.b64decode(re.findall(r'[\'](.*?)[\']', item.find(class_='proxy').get_text())[0]),
                              encoding='utf-8').split(':')
                ip = ip_port[0]
                port = ip_port[1]
                type = item.find(class_='https').get_text()

                self.ip_agent_proxy_sqls.append(sql.format(ip=ip,
                                                           port=port,
                                                           type=type))
        except Exception as e:
            print(
                "{}: Failed to pull proxy-list ip in page {}. Error - {}".format(self.__class__.__name__, page, str(e)))

    def rtl_codebusy_list(self):
        try:
            url = CODEBUSY_URL
            time.sleep(random.random() * 3)
            response = requests.get(url, proxies={'http': random.choice(PRO)}, headers={
                'User-Agent': random.choice(USER_AGENT),
                'Cookie': CODEBUSY_COOKIE
            })
            if response.status_code != 200:
                print("{}: call ip agent from codebusy websit url - {} denied.".format(self.__class__.__name__, url))
            bs_obj = BeautifulSoup(response.text.encode("utf8"), "html.parser", from_encoding='utf-8')
            sqls = []
            ip_ports = bs_obj.find_all(class_='port-box')
            sql = """
                IF EXISTS (
                    SELECT
                        *
                    FROM
                        ip_agent
                    WHERE
                        ip = '{ip}'
                ) UPDATE ip_agent
                SET ip = '{ip}', port = '{port}'
                WHERE ip = '{ip}'
                ELSE 
                    INSERT INTO ip_agent (ip, port)
                    VALUES('{ip}', '{port}')
            """
            for item in ip_ports:
                sqls.append(sql.format(ip=item['data-ip'],
                                       port=item.get_text()))
            batch_insert_update_delete(sqls)
            print("{}: Successful pull ip agent information from codebusy website.".format(self.__class__.__name__))
        except Exception as e:
            print("{}: Failed call ip agent from codebusy website. Error - {}.".format(self.__class__.__name__, str(e)))

    def get_exist_ip(self):
        try:
            sql = """
                SELECT
                    ip
                FROM
                    ip_agent
            """
            return db_query(sql)
        except Exception as e:
            print("{}: Failed to call the existed ip. Error - {}.".format(self.__class__.__name__, str(e)))
            return []

    def check_ip_status(self):
        print("{}: Begin to check the ip agent's status.".format(self.__class__.__name__))
        try:
            for ip in self.get_exist_ip():
                with ThreadPoolExecutor(self.pool_size) as executor:
                    executor.submit(self.test_ip, ip)
            batch_insert_update_delete(self.ip_status_update_sqls)
            print("{}: Successful to update the ip agent's status.".format(self.__class__.__name__))
        except Exception as e:
            print("{}: Failed to check ips status. Error - {}.".format(self.__class__.__name__, str(e)))

    def test_ip(self, ip):
        sql = """
            UPDATE ip_agent
              SET enable = '{enable}'
            WHERE ip = '{ip}'
        """
        try:
            response = requests.get(TEST_URL, headers={
                'User-Agent': random.choice(USER_AGENT)
            }, proxies={'http': ip['ip']}, timeout=5)
            time.sleep(random.random() * 5)
            if response.status_code == 200:
                self.ip_status_update_sqls.append(sql.format(ip=ip['ip'],
                                                             enable=1))
            else:
                self.ip_status_update_sqls.append(sql.format(ip=ip['ip'],
                                                             enable=0))
        except Exception as e:
            print("{}: Failed to check status for ip {}. Error - {}.".format(self.__class__.__name__, ip['ip'], str(e)))
            self.ip_status_update_sqls.append(sql.format(ip=ip['ip'],
                                                         enable=0))
