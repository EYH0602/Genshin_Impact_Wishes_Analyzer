import json
import time
import requests
import pandas as pd
from urllib import parse
from data import DataBase

ADDRESS = 'https://hk4e-api.mihoyo.com/event/gacha_info/api/getGachaLog'
TIMESLEEP = 0.2
PAGE_SIZE = 6


class WishesBase():
    def __init__(self, url, use_csv=True, use_db=True):
        resolved = dict(
            parse.parse_qsl(parse.urlsplit(url.strip()).query)
        )
        self.params = {
            'authkey_ver': resolved['authkey_ver'],
            'sign_type': resolved['sign_type'],
            'auth_appid': resolved['auth_appid'],
            'init_type': resolved['init_type'],
            'gacha_id': resolved['gacha_id'],
            'timestamp': resolved['timestamp'],
            'lang': resolved['lang'],
            'device_type': resolved['device_type'],
            'ext': resolved['ext'],
            'game_version': resolved['game_version'],
            'region': resolved['region'],
            'authkey': resolved['authkey'],
            'game_biz': resolved['game_biz'],
            'gacha_type': '',
            'page': '1',
            'size': str(PAGE_SIZE),
            'end_id': '0'
        }

        self.use_csv = use_csv
        self.use_db = use_db

        self.wishes = []
        self.df = None

        # file settings
        self.file_name = ''
        self.rst_file_name = ''

        # database settings
        self.table = ''
        self.db = DataBase()

    def run(self):
        self.init_params()
        self.check_params()
        self.fetch_request()
        self.process_data()
        if self.use_csv:
            self.to_local_file()
        if self.use_db:
            self.to_remote_storage()

    def init_params(self):
        raise NotImplementedError

    def check_params(self):
        if self.params['gacha_type'] == '':
            raise ValueError("gacha type should be set.")
        if self.file_name == '':
            raise ValueError("file name should be set.")
        if self.rst_file_name == '':
            raise ValueError('result file name should be set.')
        if self.table == '':
            raise ValueError('db table name should be set.')

    def fetch_request(self):
        while True:
            resp = requests.get(ADDRESS, params=self.params)
            if resp.status_code != 200:
                raise RuntimeError(
                    "Request Failed: {}.".format(resp.status_code)
                )
            response = json.loads(resp.text)
            if response['message'].upper() != 'OK':
                raise RuntimeError(
                    "Request Failed: {}.".format(response['message'])
                )
            self.wishes += response['data']['list']
            if len(response['data']['list']) < PAGE_SIZE:
                break
            self.params['page'] = str(int(self.params['page']) + 1)
            self.params['end_id'] = response['data']['list'][-1]['id']
            time.sleep(TIMESLEEP)

    def process_data(self):
        data = []
        for i in range(len(self.wishes) - 1, -1, -1):
            data.append({
                'item_type': self.wishes[i]['item_type'],
                'name': self.wishes[i]['name'],
                'rank_type': self.wishes[i]['rank_type'],
                'time': self.wishes[i]['time']
            })
        self.df = pd.DataFrame(
            data, columns=('item_type', 'name', 'rank_type', 'time')
        )

    def to_local_file(self):
        """
        write to local csv file for view.
        """
        self.df.to_csv(self.file_name, index=False)

    def to_remote_storage(self):
        """
        write to local/remote db for record.
        """
        self.db.append(self.table, source="df")(self.df)

    def backup_local_record(self):
        """
        sync local csv to db
        """
        self.db.append(self.table, source="csv")(self.file_name)

    def analyze(self):
        self.init_params()
        self.check_params()
        # if dataframe does not exists, read from db table
        if self.df is None:
            self.df = self.db.get_table(self.table)

        result = self.calculate()
        self.write_result_file(result)

    def calculate(self):
        """
        返回总抽卡数, 5星总数与占比, 4星总数与占比, 3星总数与占比,
        平均y抽出5星, 平均y抽出4星,
        已x抽未出5星, 已x抽未出4星,
        5星列表（抽卡数）, 4星列表（抽卡数）, 日期范围
        """

        # query data
        fromdate, todate = self.db.get_time_range(self.table)
        total = self.db.get_total_count(self.table)
        five_list = self.db.get_wishes(self.table, 5)  # [(id, name)]
        four_list = self.db.get_wishes(self.table, 4)  # [(id, name)]

        # calculation data
        # NOTE: all count can be queried,
        # such functions are provided in data/database_connection.py
        five_count = len(five_list)
        four_count = len(four_list)
        three_count = total - five_count - four_count  # there are only three rank_types in the game
        five_wait = total
        four_wait = total

        five_percent = 0.0
        four_percent = 0.0
        three_percent = 0.0
        five_avg = 0.0
        four_avg = 0.0

        if five_count > 0:
            five_wait -= five_list[-1][0]
            five_avg = round((total - five_wait) / five_count, 1)
        if four_count > 0:
            four_avg = round((total - four_wait) / four_count, 1)
            four_wait -= four_list[-1][0]
        if total > 0:
            five_percent = round(100 * five_count / total, 2)
            four_percent = round(100 * four_count / total, 2)
            three_percent = round(100 * three_count / total, 2)

        return (total, five_count, four_count, three_count, five_percent, four_percent, three_percent,
                five_avg, four_avg, five_wait, four_wait, five_list, four_list, fromdate, todate)

    def write_result_file(self, results):
        with open(self.rst_file_name, 'w', encoding='UTF-8') as fo:
            fo.write('原神祈愿历史记录分析 ({}) '.format(self.rst_file_name))
            fo.write('{} ~ {}\n\n'.format(results[13], results[14]))
            fo.write('共{}抽\n'.format(results[0]))
            fo.write('|五星\t|四星\t|三星\t|\n')
            fo.write('|{}  \t|{}  \t|{}  \t|\n'.format(
                results[1], results[2], results[3]))
            fo.write('|{}%\t|{}%\t|{}%\t|\n\n'.format(
                results[4], results[5], results[6]))
            fo.write('平均{}抽出五星, 平均{}抽出四星\n'.format(results[7], results[8]))
            fo.write('已{}抽未出五星, 已{}抽未出四星'.format(results[9], results[10]))
            if results[1] > 0:
                fo.write('\n\n五星列表：\n')
                for item in results[11]:
                    fo.write('{} ({}), '.format(item[1], item[0]))
            if results[2] > 0:
                fo.write('\n\n四星列表：\n')
                for item in results[12]:
                    fo.write('{} ({}), '.format(item[1], item[0]))
