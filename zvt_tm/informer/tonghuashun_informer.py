import logging
from http.client import HTTPConnection

import requests
from requests import Response
from zvt import zvt_config
from zvt_tm import zvt_tm_config

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


HEADER = chrome_copy_header_to_dict(zvt_tm_config['tonghuashun_header'])


def parse_resp(resp: Response):
    if resp.status_code != 200:
        raise Exception(f'code:{resp.status_code},message:{resp.content}')
    # {'success': False, 'message': '参数有误', 'data': False}
    ret = resp.json()
    logger.info(f'ret:{ret}')
    return ret['success'], ret.get('data')

def add_to_group(code, entity_type='stock', group_id=None):
    url = 'http://www.iwencai.com/unifiedwap/self-stock/plate/add'
    resp = requests.post(url, headers=HEADER,
                         data={'sn': group_id,
                               'query_type': 'stock',
                               'code': to_tonghuashun_code(code=code, entity_type=entity_type)})

    ret, _ = parse_resp(resp)
    return ret


def to_tonghuashun_code(code, entity_type='stock'):
    if entity_type == 'stock':
        # 上海
        if code >= '333333':
            return f'{code}.SH'
        else:
            return f'{code}.SZ'

__all__ = ['add_to_group', 'to_tonghuashun_code']

if __name__ == '__main__':
    # groups = get_groups()
    # if len(groups) >= 9:
    #     del_group(group_id=groups[-2]['id'])
    #
    # create_group('tmp')
    add_to_group('002229', group_id='23')
    # del_group('tmp')