import logging
from http.client import HTTPConnection

import requests
from requests import Response
from zvt import zvt_config

logger = logging.getLogger(__name__)

def chrome_copy_header_to_dict(src):
    lines = src.split('\n')
    header = {}
    if lines:
        for line in lines:
            try:
                index = line.index(':')
                key = line[:index]
                value = line[index + 1:]
                if key and value:
                    header.setdefault(key.strip(), value.strip())
            except Exception:
                pass
    return header


HEADER = chrome_copy_header_to_dict(zvt_config['tradingview_header'])


def parse_resp(resp: Response):
    if resp.status_code != 200:
        raise Exception(f'code:{resp.status_code},message:{resp.content}')
    # {'success': False, 'message': '参数有误', 'data': False}
    ret = resp.json()
    logger.info(f'ret:{ret}')
    return ret

def add_to_group(code, entity_type='stock', group_id=None):
    url = f'https://cn.tradingview.com/api/v1/symbols_list/custom/{group_id}/append/'
    resp = requests.post(url, headers=HEADER,
                         json=[to_tradingview_code(code=code, entity_type=entity_type)])

    ret = parse_resp(resp)
    return ret

def add_list_to_group(codeList, entity_type='stock', group_id=None):
    url = f'https://cn.tradingview.com/api/v1/symbols_list/custom/{group_id}/append/'
    data = list(map(lambda x: to_tradingview_code(code=x, entity_type=entity_type), codeList))
    resp = requests.post(url, headers=HEADER,
                         json=data)

    ret = parse_resp(resp)
    return ret

def to_tradingview_code(code, entity_type='stock'):
    if entity_type == 'stock':
        # 上海
        if code >= '333333':
            return f'SSE:{code}'
        else:
            return f'SZSE:{code}'

__all__ = ['add_to_group', 'to_tradingview_code']

if __name__ == '__main__':
    # groups = get_groups()
    # if len(groups) >= 9:
    #     del_group(group_id=groups[-2]['id'])
    #
    # create_group('tmp')
    # add_to_group('002229', group_id='22081672')
    add_list_to_group(['002230','000231'], group_id='22081672')
    # del_group('tmp')