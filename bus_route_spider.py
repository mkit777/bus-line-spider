import requests
import json
from urllib.parse import quote
import logging
from bus_line_name import BUS_LINES


logging.basicConfig(level = logging.INFO, format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def save_to_csv(file, data):
    """ 将数据保存至 csv 文件 """
    id = data['id']
    name = data['name']
    for e in data['lines']:
        flat = (id, name, str(e['lng']), str(e['lat']), str(e['is_station']), e['station_id'], e['station_name'], e['station_num'])
        file.write(','.join(flat)+'\n')

def parse_route_info(route_data):
    """ 整理一条路线的数据 """

    id = route_data['id']
    name = route_data['name']

    logger.info(f'[查找成功]  线路id:[ {id} ]  线路名:[ {name} ]')

    lng = route_data['xs'].split(',')
    lat = route_data['ys'].split(',')

    lines = [
        {
            'lng': float(x), 
            'lat':float(y), 
            'is_station': False,
            'station_name': '',
            'station_id': '',
            'station_num': ''
        } for x, y in zip(lng, lat)]


    stations = []
    for station in route_data['stations']:
        splited_xy = station['xy_coords'].split(';')
        lng = float(splited_xy[0])
        lat = float(splited_xy[1])

        stations.append(
            {
                'lng': lng,
                'lat': lat,
                'is_station': True,
                'station_name': station['name'],
                'station_id': station['station_id'],
                'station_num': station['station_num'],
                'status': station['status']
            }
        )

    for i, pos in enumerate(lines) :
        if pos['lng'] == stations[0]['lng'] and pos['lat'] == stations[0]['lat']:
            lines[i] = stations.pop(0)
 
    return {'id':id, 'name':name, 'lines':lines}

def parse_data(data):
    # return [parse_route_info(x) for x in data['busline_list']
    return [parse_route_info(busline) for busline in data['busline_list']]

def get_bus_route(citycode, route_name):
    """ 获得指定线路的公交路线 """
    request_url = BASE_URL.format(city=citycode, keywords=quote(route_name))
    data = requests.get(request_url, headers=HEADER).json()
    if( data['status'] != '1'):
        raise Exception(f'数据请求失败 {request_url}')
    return parse_data(data['data'])

CSV_HEADER = ['id', 'name', 'lng', 'lat', 'is_station','station_id','station_name', 'station_num']

BASE_URL = 'https://www.amap.com/service/poiInfo?query_type=TQUERY&city={city}&keywords={keywords}'

HEADER = {
    'Cookie': ''
}

FILE_NAME = "result.csv" 

def spider(cityCode, cookie):
    HEADER['Cookie'] = cookie
    with open(FILE_NAME, 'w') as f:
        f.write(','.join(CSV_HEADER) + '\n')
        error_route = []
        for i, line in enumerate(BUS_LINES) :
            logger.info(f'[正在查找]  关键词:[ {line} ] {i+1}/{len(BUS_LINES)}')
            try:
                result_list = get_bus_route(cityCode, line)
                for result in result_list:
                    save_to_csv(f, result)
            except Exception as e:
                logger.error(f'[爬取错误]  关键词:[ {line} ] {e}')
                error_route.append(line)
        logger.info(f'[完整任务]  成功:[ {len(BUS_LINES) - len(error_route)} ] 失败:[ {len(error_route)} ]')
        if len(error_route) > 0:
            logger.error(f'[失败任务] { ",".join(error_route) }')

if __name__ == "__main__":
    # city 在这里修改
    CITY = 370200
    # Cookie 在这里修改
    COOKIE = 'UM_distinctid=17a3d0b45c3ab6-0d489ab4acb0fc-34657400-1fa400-17a3d0b45c4dfe; cna=wn5YGWqxtxwCAdo6O0hffvWt; _uab_collina=162452178892489088983628; passport_login=NTUzNDYzMzgsYW1hcF8xODU2MDI2MTUxOTl2aGJSZDdsLHZ5NGF1ZmkzYnhhb2x5ZWh3ZW1kYTJmcTV1dG5ja3FxLDE2MjQ2MTgwMjAsTUdWa1pXSTNOMlF6WXpSa05qQTFNMlJrWVdaaU9EUTVZalkwTVdZNVlXTT0%3D; CNZZDATA1255626299=522210911-1624518968-https%253A%252F%252Fwww.baidu.com%252F%7C1625495040; guid=a1fc-2e45-3dad-bb41; x-csrf-token=becc059bb279bfe0f7c389e453fda876; x5sec=7b22617365727665723b32223a223631376564663164343466333631313637383130373231653637393762366438434948526a6f6347454e376378354441314b4c597177456f416a447675594159227d; tfstk=cy_GBRsVGpMWNK-hVPT1s_wi40SNCf32EZ7C8Z6p12EW_-kWQh5mxDoydVFHIObxc; l=eBMq6_6lj0rF1afFKO5aourza779rLAfG5VzaNbMiInca6L5VF4YGNCBm5cehdtjgtCjwExrRawxURFy-I4N95n8-lA1tTf7_xvO.; isg=BNvbxye4P4Fxd0PCgXMwYq-aajlFsO-yqUyMY80S81rxrP-OW4FGAtxiRgwis0eq'

    spider(CITY, COOKIE)