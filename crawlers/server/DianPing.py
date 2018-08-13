import os
import json
import requests
import random
import time

from crawlers.common.db_operation import batch_insert_update_delete,db_query

pro = ['192.155.185.43', '192.155.185.153', '192.155.185.171', '192.155.185.197']
head = {
    'user-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
    'Cookie': ''
}


def update_domestic_city_info():
    """
    update local database domestic city info from dinaping.com
    :return:
    """
    try:
        province_exist = get_exist_province_list()
        city_exist = get_exist_city_list()
        url = "http://www.dianping.com/ajax/citylist/getAllDomesticCity"
        time.sleep(random.random() * 3)
        print("DianPing: start crawler city info from {}".format(url))
        response = requests.get(url)
        province_list_remote = response.json()['provinceList']
        city_list_remote = response.json()['cityMap']
        sql_list = []
        for province_remote in province_list_remote:
            isExisted = False
            for province_local in province_exist:
                if str(province_local['provinceId']) == str(province_remote['provinceId']):
                    isExisted = True
                    break
            if not isExisted:
                sql = """
                    insert into dp_province
                        (areaId, provinceId, provinceName)
                    values
                        ('{areaId}','{provinceId}','{provinceName}')
                """.format(areaId=province_remote['areaId'],
                           provinceId=province_remote['provinceId'],
                           provinceName=province_remote['provinceName'])
                sql_list.append(sql)
        batch_insert_update_delete(sql_list)
        print("DianPing: success update the province info for local database")
        for k in city_list_remote:
            sql_list = []
            city_list = city_list_remote[k]
            for city_remote in city_list:
                isExisted = False
                for city_local in city_exist:
                    if str(city_local['cityId']) == str(city_remote['cityId']):
                        isExisted = True
                        break
                if not isExisted:
                    sql = """
                        insert into dp_city
                            (activeCity, appHotLevel, cityAbbrCode,
                             cityEnName, cityId, cityLevel, cityName, cityOrderId,
                             cityPyName, gLat, gLng, overseasCity,
                             parentCityId, provinceId, scenery, tuanGouFlag)
                        values
                            ('{activeCity}', '{appHotLevel}', '{cityAbbrCode}', 
                            '{cityEnName}','{cityId}','{cityLevel}', '{cityName}','{cityOrderId}',
                            '{cityPyName}','{gLat}', '{gLng}','{overseasCity}',
                            '{parentCityId}', '{provinceId}', '{scenery}','{tuanGouFlag}')
                    """.format(activeCity=1 if city_remote['activeCity'] else 0,
                               appHotLevel=city_remote['appHotLevel'],
                               cityAbbrCode=city_remote['cityAbbrCode'],
                               cityEnName=city_remote['cityEnName'],
                               cityId=city_remote['cityId'], cityLevel=city_remote['cityLevel'],
                               cityName=city_remote['cityName'], cityOrderId=city_remote['cityOrderId'],
                               cityPyName=city_remote['cityPyName'],
                               gLat=float(city_remote['gLat']), gLng=float(city_remote['gLng']),
                               overseasCity=1 if city_remote['overseasCity'] else 0,
                               parentCityId=city_remote['parentCityId'],
                               provinceId=city_remote['provinceId'],
                               scenery=1 if city_remote['scenery'] else 0,
                               tuanGouFlag=city_remote['tuanGouFlag'])
                    sql_list.append(sql)
            batch_insert_update_delete(sql_list)
            print("DianPing: success update the city for provinceId {}".format(k))
        print("DianPing: success update the city info for local database")
    except:
        print("DianPing: Failed to update the domestic city info in local databse")
        raise


def get_exist_province_list():
    """
    get existed province list information
    :return:
    """
    sql = """
        select provinceId from dp_province 
    """
    province_list = []
    try:
        province_list = db_query(sql)
        print("DianPing: success to get the province existed in local database")
        return province_list
    except:
        print("DianPing: failed to query the database to get the existed province info list")
        raise


def get_exist_city_list():
    """
    get existed city list from local database
    :return:
    """
    sql = """
        select cityId from dp_city
    """
    city_list = []
    try:
        city_list = db_query(sql)
        print("DianPing: success to get the city list from local database")
        return city_list
    except:
        print("DianPing: failed to query the database to get the city info ")
        raise
    pass
