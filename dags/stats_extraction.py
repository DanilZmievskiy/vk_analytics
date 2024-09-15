import datetime

import gspread
import os
import time
import requests
import itertools
import pandas as pd
from dotenv import load_dotenv
from typing import List, Dict, Any, Tuple
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
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
def get_vk_posts_stats() -> None:
    """
    DAG which extracts data from VK and upload it to Google Sheets.
    """

    create_posts_table = PostgresOperator(
        task_id="create_posts_table",
        postgres_conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS posts (
                "post_id" INTEGER,
                "text" TEXT,
                "date" DATE,
                "time" TIME,
                "likes_count" INTEGER,
                "comments_count" INTEGER,
                "reactions_count" INTEGER,
                "views_count" INTEGER
            );""",
    )


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
            
            response.raise_for_status()
            data = response.json()['response']['items']
            posts.append(data)
            time.sleep(5)

        posts_total = list(itertools.chain(*posts))
        return posts_total
    

    @task
    def check_existing_records() -> List[int]:
        """
        Checks if there are existing records in the database.
        """
        conn = PostgresHook(postgres_conn_id="postgres").get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT post_id FROM posts")
        records = cursor.fetchall()
        if records:
            records = list(itertools.chain(*records))
        return records
    

    @task
    def transform_data(posts_total: list[dict], records: List[int]) -> pd.DataFrame:
        """
        Transforms data from VK to pandas DataFrame.
        :param posts_total: extracted data
        :param records: existing records in the database
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
                                                 'raw_date',
                                                 'likes_count',
                                                 'comments_count',
                                                 'reactions_count',
                                                 'views_count'])
        
        df = df[~df['post_id'].isin(records)]
        if df.empty:
            return df

        df['raw_date'] = pd.to_datetime(df['raw_date'], unit='s')
        df['date'] = df['raw_date'].dt.strftime('%Y-%m-%d')
        df['time'] = df['raw_date'].dt.strftime('%H:%M:%S')
        df = df.drop(['raw_date'], axis=1)

        df['user_activity'] = df['likes_count'] + df['comments_count'] + df['reactions_count']

        return df
    

    @task
    def load_data_to_postgres(df: pd.DataFrame) -> None:
        """
        Loads data to the database.
        """

        if df.empty:
            return
        
        target_fields = ['post_id', 'text', 'date', 'time', 'likes_count', 'comments_count', 'reactions_count', 'views_count']
        df = df[['post_id', 'text', 'date', 'time', 'likes_count', 'comments_count', 'reactions_count', 'views_count']]

        PostgresHook(postgres_conn_id="postgres").insert_rows("posts", df.values, target_fields=target_fields)


    @task
    def load_data_to_google_sheets(df: pd.DataFrame) -> None:
        """
        Loads data to Google Sheets.
        """
        df = df[['post_id', 'text', 'date', 'time', 'likes_count', 'comments_count', 'reactions_count', 'views_count', 'user_activity']]

        conn = CloudConnection(key_file=KEY_FILE)
        spr_sheet = conn._client.open_by_key(SPREADSHEET_ID)

        worksheets = map(lambda sheet: sheet.title, spr_sheet.worksheets())
        worksheet_name = "Posts Data"

        if worksheet_name in worksheets:
            active_sheet = spr_sheet.worksheet(worksheet_name)
            active_sheet.insert_rows(df.values.tolist(), row=2)
        else:
            active_sheet = spr_sheet.add_worksheet(
                title=worksheet_name, rows=2000, cols=20)
            
            active_sheet.update([df.columns.values.tolist()] + df.values.tolist())

    posts = extract_data()
    records = check_existing_records()
    data = transform_data(posts, records)

    create_posts_table >> [posts, records] >> data >> [load_data_to_postgres(data), load_data_to_google_sheets(data)]

get_vk_posts_stats()