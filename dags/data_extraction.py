import datetime

import gspread
import os
import time
import requests
import itertools
import pandas as pd
from dotenv import load_dotenv
from airflow.decorators import dag, task
from utils.google_connect import CloudConnection

load_dotenv()

KEY_FILE = os.getenv("KEY_FILE")
VK_TOKEN = os.getenv("VK_TOKEN")
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")


@dag(
    schedule="0 3 * * *", # Запуск каждый день в 3:00
    start_date=datetime.datetime(2024, 9, 13),
    catchup=False,
    tags=["VK", "data_extraction"],
)
def get_vk_posts() -> None:
    """
    DAG which extracts data from VK and upload it to Google Sheets.
    """


    @task
    def extract_data() -> list[dict]:
        """
        Extracts data from VK.
        """
        posts = []
        for i in range(11):
            offset = i
            response = requests.get('https://api.vk.com/method/wall.get',
                                    params={'access_token': VK_TOKEN,
                                            'v': '5.199',
                                            'domain': 'itmoru',
                                            'count': 100,
                                            'offset': offset})
            
            data = response.json()['response']['items']
            posts.append(data)
            time.sleep(5)

        posts_total = list(itertools.chain(*posts))
        return posts_total
    

    @task
    def transform_data(posts_total: list[dict]) -> pd.DataFrame:
        """
        Transforms data from VK to pandas DataFrame.
        """
        posts_total_unpacked = [(post['id'],
                         post['text'],
                         post['date'],
                         post['likes']['count'],
                         post['comments']['count'],
                         post['reactions']['count'],
                         post['views']['count']) for post in posts_total]
        
        df = pd.DataFrame(posts_total_unpacked, columns=['post_id',
                                                 'text',
                                                 'date',
                                                 'likes_count',
                                                 'comments_count',
                                                 'reactions_count',
                                                 'views_count'])
        
        df['date'] = pd.to_datetime(df['date'], unit='s')
        return df
    

    @task
    def load_data(df: pd.DataFrame) -> None:
        """
        Loads data to Google Sheets.
        """
        conn = CloudConnection(key_file=KEY_FILE)
        spr_sheet = conn._client.open_by_key(SPREADSHEET_ID)

        worksheets = map(lambda sheet: sheet.title, spr_sheet.worksheets())
        worksheet_name = "Posts Data"

        if worksheet_name in worksheets:
            active_sheet = spr_sheet.worksheet(worksheet_name)
        else:
            active_sheet = spr_sheet.add_worksheet(
                title=worksheet_name, rows=2000, cols=20)

        active_sheet.update([df.columns.values.tolist()] + df.values.tolist())

    data = extract_data()
    df = transform_data(data)
    load_data(df)

get_vk_posts()
