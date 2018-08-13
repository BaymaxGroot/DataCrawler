import requests
import random
import time
import datetime
from bs4 import BeautifulSoup
from crawlers.common.db_operation import batch_insert_update_delete, db_query
from concurrent.futures import ThreadPoolExecutor
from crawlers.common.IPAgentPool import IPAgentPool
from crawlers.common.ThreadPool import ThreadPool

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
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",

    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:56.0) Gecko/20100101 Firefox/56.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",

    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.2)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)"
]

"""
    DOUBAN relation info url or cookie 
"""
DOUBAN_BASE_URL = 'https://www.douban.com/'
DOUBAN_MOVIE_TYPE_URL = "https://movie.douban.com/j/search_tags?type=movie&source="
DOUBAN_MOVIE_BASIC_INFO_URL = "https://movie.douban.com/j/search_subjects?type=movie&tag={type}&sort=recommend&page_limit={page_size}&page_start={start_num}"
DOUBAN_MOVIE_DETAIL_INFO_URL = "https://movie.douban.com/subject/{}/"
DOUBAN_COOKIE = 'bid=HypvQwpRptk; ps=y; ll="108099"; __utmz=30149280.1533094300.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); as="https://sec.douban.com/b?r=https%3A%2F%2Fmovie.douban.com%2F"; _pk_ses.100001.8cb4=*; __utma=30149280.630807168.1533094300.1533096313.1533100475.3; __utmc=30149280; _vwo_uuid_v2=D7F3BC9A37BC5F4F9AF4466DBF19E4890|c5846c6b9746d9b0800f6302be11722d; __utmt=1; _pk_id.100001.8cb4=4c30040137478aac.1533094264.3.1533101129.1533096401.; __utmb=30149280.5.10.1533100475'


class Douban(ThreadPool):

    def __init__(self, pool_size):
        super().__init__(pool_size)
        self.type_exist = []
        self.update_type_sqls = []
        self.movie_type_exist = []
        self.update_movie_basic_sqls = []
        self.update_cache_date()
        self.movie_exist = []
        self.update_movie_detail_sqls = []
        self.ip_agent = IPAgentPool.get_ip_agent()

    def crawler(self):

        begin = datetime.datetime.now()
        self.rtl_movie_type()
        end = datetime.datetime.now()
        print("{}: Successful collect movie type during {} s".format(self.__class__.__name__, (end - begin).seconds))

        begin = datetime.datetime.now()
        self.rtl_movie_basic_info()
        end = datetime.datetime.now()
        print(
            "{}: Successful collect movie basic info during {} s".format(self.__class__.__name__,
                                                                         (end - begin).seconds))

        begin = datetime.datetime.now()
        self.rtl_movie_detail_info()
        end = datetime.datetime.now()
        print(
            "{}: Successful collect movie detail info during {} s".format(self.__class__.__name__,
                                                                          (end - begin).seconds))

    def update_cache_date(self):
        """
        update cache data during program running
        :return:
        """
        self.type_exist = self.get_exist_movie_type()
        self.movie_type_exist = self.get_exist_movie_type_link()
        self.movie_exist = self.get_exist_movie()
        self.ip_agent = IPAgentPool.get_ip_agent()

    def get_exist_movie_type(self):
        """
        get existed movie type info from local database
        :return:
        """
        sql = """
            select 
              id,type 
            from 
              db_movie_type
        """
        type_list = []
        try:
            type_list = db_query(sql)
            # print("{}: Success query the database to get the existed movie type list.".format(self.__class__.__name__))
        except Exception as e:
            print("{}: Failed to query the existed movie type in personal database. Error - {}.".format(
                self.__class__.__name__, str(e)))
        finally:
            return type_list

    def rtl_movie_type(self):
        """
        call movie type from moviw.douban's database then update this infos to local DB
        :return:
        """
        try:
            self.update_cache_date()
            url = DOUBAN_MOVIE_TYPE_URL
            time.sleep(random.random() * 3)
            response = requests.get(url, proxies={'http': random.choice(self.ip_agent)['ip']}, headers={
                'User-Agent': random.choice(USER_AGENT),
                'Cookie': DOUBAN_COOKIE
            })
            movie_list_remote = response.json()['tags']
            for type_remote in movie_list_remote:
                with ThreadPoolExecutor(self.pool_size) as executor:
                    executor.submit(self.update_movie_type, type_remote)
            batch_insert_update_delete(self.update_type_sqls)
            print("{}: Success update the movie type in local database.".format(self.__class__.__name__))
        except Exception as e:
            print("{}: Failed update the movie type in local database. Error - {}.".format(self.__class__.__name__,
                                                                                           str(e)))
            raise

    def update_movie_type(self, type_remote):
        """
            generate the update movie type sql
        :param type_remote:
        :return:
        """
        isExisted = False
        for type_local in self.type_exist:
            if type_local['type'] == type_remote:
                isExisted = True
                break
        if not isExisted:
            sql = """
                insert into db_movie_type 
                  ( type ) 
                values 
                  ('{type}')
            """.format(type=type_remote)
            self.update_type_sqls.append(sql)

    def get_exist_movie_type_link(self):
        """
        get existed movie id - type link from local database
        :return:
        """
        sql = """
            SELECT 
                m_id,
                type_id,
                m_playable
            FROM
                db_movie, db_movie_type_link
            WHERE
                m_id = movie_id
            ORDER BY m_id
        """
        movie_list = []
        try:
            movie_list = db_query(sql)
            # print("{}: Success query the database to get the existed movie id list.".format(self.__class__.__name__))
        except Exception as e:
            print(
                "{}: Failed to query the existed movie id - type link relation in local database. Error - {}.".format(
                    self.__class__.__name__,
                    str(e)))
        finally:
            return movie_list

    def get_exist_movie(self):
        """
        get existed movie id from local database
        :return:
        """
        sql = """
            SELECT
                m_id,
                m_playable
            FROM
                db_movie
            WHERE 
                DATEDIFF(DAY, m_update_time, GETDATE()) > 7
            ORDER BY m_id
        """
        movie_list = []
        try:
            movie_list = db_query(sql)
            # print("{}: Success query the database to get the existed movie id list.".format(self.__class__.__name__))
        except Exception as e:
            print(
                "{}: Failed to query the existed movie id in local database. Error - {}.".format(
                    self.__class__.__name__,
                    str(e)))
        finally:
            return movie_list

    def rtl_movie_basic_info(self):
        """
        update movie basic info in local database from movie.douban's database
        :return:
        """
        try:
            self.update_cache_date()
            for type in self.type_exist:
                with ThreadPoolExecutor(self.pool_size) as executor:
                    executor.submit(self.call_data_through_movie_type, type)
            self.update_movie_basic_sqls.append(
                """
                    DELETE ad FROM db_movie_type_link ad
                    WHERE ad.id NOT IN (
                        SELECT 
                            MIN(id)
                        FROM
                            db_movie_type_link
                        GROUP BY movie_id,type_id
                    )
                """
            )
            self.update_movie_basic_sqls.append(
                """
                    DELETE ad FROM db_movie ad
                    WHERE ad.id NOT IN (
                        SELECT 
                            MIN(id)
                        FROM
                            db_movie
                        GROUP BY m_id
                    )
                """
            )
            batch_insert_update_delete(self.update_movie_basic_sqls)
            print("{}: Success update the basic info of the movies in local database.".format(self.__class__.__name__))
        except Exception as e:
            print("{}: Failed update the basic info of the movies in local database. Error - {}.".format(
                self.__class__.__name__, str(e)))
            raise

    def call_data_through_movie_type(self, type):
        """
        pull data through movie type from douban DB to local DB
        :param type:
        :return:
        """
        try:
            print("{}: start get the {} movies from douban DB.".format(self.__class__.__name__, type['type']))
            page_size = 100
            start_num = 0
            while 1:
                url = DOUBAN_MOVIE_BASIC_INFO_URL.format(
                    type=type['type'], page_size=page_size, start_num=start_num)
                time.sleep(random.random() * 3)
                response = requests.get(url, proxies={'http': random.choice(self.ip_agent)['ip']}, headers={
                    'User-Agent': random.choice(USER_AGENT),
                    'Cookie': DOUBAN_COOKIE
                })

                if response.status_code == 403:
                    print("{}: url - {} was forbided.".format(self.__class__.__name__, url))
                    return

                if response.status_code != 200:
                    print("{}: url status code is {}.".format(self.__class__.__name__, response.status_code))
                    return

                movie_basic_info_remote = response.json()['subjects']
                if len(movie_basic_info_remote) == 0:
                    break
                for info in movie_basic_info_remote:
                    with ThreadPoolExecutor(self.pool_size) as executor:
                        executor.submit(self.insert_update_movie_basic_info, type, info)
                start_num += page_size
            # print("{}: Success collect data for the {} movies.".format(self.__class__.__name__, type["type"]))
        except Exception as e:
            print("{}: Failed collect data for the {} movies. Error - {}.".format(self.__class__.__name__, type["type"],
                                                                                  str(e)))

    def insert_update_movie_basic_info(self, type, info):
        isIdExisted = False
        isIdTypeExisted = False
        for id in self.movie_type_exist:
            if int(id['m_id']) == int(info['id']) & int(id['type_id']) == int(type['id']):
                isIdExisted = True
                isIdTypeExisted = True
                break
            if int(id['m_id']) == int(info['id']):
                isIdExisted = True
        if not isIdExisted:
            sql = """
                insert into db_movie
                    (m_id, m_title, m_rate, m_url, m_cover_url, m_cover_x, m_cover_y, m_is_new, m_playable)
                values
                    ('{m_id}', '{m_title}', '{m_rate}', '{m_url}', '{m_cover_url}', '{m_cover_x}', '{m_cover_y}', '{m_is_new}', '{m_playable}')
            """.format(m_id=info['id'],
                       m_title=str(info['title']).replace("'", " "),
                       m_rate=info['rate'],
                       m_url=info['url'],
                       m_cover_url=info['cover'],
                       m_cover_x=info['cover_x'],
                       m_cover_y=info['cover_y'],
                       m_is_new=1 if info['is_new'] else 0,
                       m_playable=1 if info['playable'] else 0)
            self.update_movie_basic_sqls.append(sql)
            sql = """
                insert into db_movie_type_link
                    (movie_id, type_id)
                values
                    ('{movie_id}', '{m_type}')
            """.format(movie_id=info['id'], m_type=type['id'])
            self.update_movie_basic_sqls.append(sql)
        if isIdExisted & (not isIdTypeExisted):
            sql = """
                insert into db_movie_type_link
                    (movie_id, type_id)
                values
                    ('{movie_id}', '{m_type}')
            """.format(movie_id=info['id'], m_type=type['id'])
            self.update_movie_basic_sqls.append(sql)

    def rtl_movie_detail_info(self):
        try:
            print("{}: Start to pull movies's detail information. - Time {}.".format(self.__class__.__name__,
                                                                                     datetime.datetime.now()))
            self.update_cache_date()
            for movie in self.movie_exist:
                with ThreadPoolExecutor(self.pool_size) as executor:
                    executor.submit(self.insert_update_movie_detail_info, movie)
            print("{}: Successful update movies's detail information.".format(self.__class__.__name__))
        except Exception as e:
            print("{}: Failed update movies's detail information. Error - {}".format(self.__class__.__name__, str(e)))
            raise

    def insert_update_movie_detail_info(self, movie):
        try:
            url = DOUBAN_MOVIE_DETAIL_INFO_URL.format(movie['m_id'])
            time.sleep(random.random() * 4)
            ip = random.choice(self.ip_agent)['ip']
            user_agent = random.choice(USER_AGENT)
            response = requests.post(DOUBAN_BASE_URL, proxies={'http': ip}, headers={
                'User-Agent': user_agent
            })
            cookie = response.cookies.get_dict()
            time.sleep(random.random() * 2)
            response = requests.get(url, proxies={'http': ip}, headers={
                'User-Agent': user_agent,
                # 'Cookie': cookie
            })

            if response.status_code == 403:
                print("{}: url - {} was forbided.".format(self.__class__.__name__, url))
                batch_insert_update_delete(self.update_movie_detail_sqls)
                self.update_movie_detail_sqls = []
                return

            if response.status_code != 200:
                print("{}: url status code is {}.".format(self.__class__.__name__, response.status_code))
                return

            bs_obj = BeautifulSoup(response.text.encode("utf8"), "html.parser", from_encoding='utf-8')

            sqls = []

            if movie['m_playable']:
                play_links = bs_obj.find_all(class_='playBtn')
                for element in play_links:
                    sql = """
                        IF EXISTS (
                            SELECT
                                *
                            FROM
                                db_movie_play_link
                            WHERE
                                m_id = '{m_id}' AND m_play_app = '{app}'
                        ) UPDATE db_movie_play_link
                        SET m_play_url = '{m_play_url}'
                        WHERE m_id = '{m_id}' AND m_play_app = '{app}'
                        ELSE 
                            INSERT INTO db_movie_play_link (m_id, m_play_app, m_play_url)
                            VALUES('{m_id}', '{app}', '{m_play_url}')
                    """.format(m_id=movie['m_id'],
                               app=element['data-cn'],
                               m_play_url=element['href'])
                    sqls.append(sql)

            movieInfo = bs_obj.find(id='info')
            """得到导演信息 并在dom树中删除"""
            if movieInfo.find(rel='v:directedBy'):
                director = movieInfo.find(rel='v:directedBy').get_text().replace('\'', '-')
                movieInfo.find(rel='v:directedBy').extract()
            else:
                director = 'Not Sure'

            """得到演员信息 并在dom树中删除"""
            for item in movieInfo.find_all(rel='v:starring'):
                sql = """
                    INSERT INTO db_movie_actor_link 
                        (m_id, actor)
                    VALUES
                        ('{m_id}', '{actor}')
                """
                item.extract()
                sqls.append(sql.format(m_id=movie['m_id'],
                                       actor=item.get_text().replace('\'', '"')))

            """从dom树中删除电影类型信息"""
            for item in movieInfo.find_all(property='v:genre'):
                item.extract()

            """得到电影上映时间 并从dom树中删除"""
            onDates = []
            for item in movieInfo.find_all(property='v:initialReleaseDate'):
                onDates.append(item.get_text().replace('\'', '-'))
                item.extract()

            if movieInfo.find(class_='actor'):
                movieInfo.find(class_='actor').extract()

            sql = """
                UPDATE db_movie
                SET m_director = '{m_director}', m_ondates = '{m_ondates}' 
                WHERE m_id = '{m_id}'
            """.format(m_id=movie['m_id'],
                       m_director=director,
                       m_ondates='*'.join(onDates))
            sqls.append(sql)
            batch_insert_update_delete(sqls)

            print("Successful pull detail data for movie: {}".format(movie['m_id']))

        except Exception as e:
            print("{}: Failed to call detail information for movie {}. Error - {}.".format(self.__class__.__name__,
                                                                                           movie.m_id, str(e)))
            raise
