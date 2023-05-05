
import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import os
import matplotlib.pyplot as plt
from datetime import datetime, timedelta

import io
import telegram
from dotenv import load_dotenv


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

connection = {
    'host': 'https://clickhouse.lab.karpov.courses',
    'password': 'dpo_python_2020',
    'user': 'student',
    'database': 'simulator_20230320'
}

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'u.plusnina',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 4, 10),
}

schedule_interval = '0 11 * * *' # отчет приходит каждый день в 11 утра


@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def u_plusnina_feed_report():

    @task()
    # Загружаем данные за прошлый день
    def extract_yesterday():
        q_1 = """
        SELECT max(toDate(time)) as day, 
        count(DISTINCT user_id) as DAU, 
        sum(action = 'like') as likes,
        sum(action = 'view') as views, 
        likes/views as CTR
        FROM simulator_20230320.feed_actions
        WHERE toDate(time) = yesterday()
        """

        yesterdays_information = ph.read_clickhouse(q_1, connection=connection)
        return yesterdays_information

    @task()
    # Загружаем данные за прошлую неделею
    def extract_last_week():
        q_2 = """
        SELECT toDate(time) as day,
        count(DISTINCT user_id) as DAU,
        sum(action = 'like') as likes,
        sum(action = 'view') as views,
        likes/views as CTR
        FROM simulator_20230320.feed_actions
        WHERE toDate(time) > today()-8 AND toDate(time) < today()
        GROUP BY day
        """

        last_weeks_information = ph.read_clickhouse(q_2, connection=connection)
        return last_weeks_information

    @task()
    def report_yesterday(yesterdays_information):
        dau = yesterdays_information['DAU'].sum()
        views = yesterdays_information['views'].sum()
        likes = yesterdays_information['likes'].sum()
        ctr = yesterdays_information['CTR'].sum()

        text = f'Статистика ленты новостей за вчера:\n\nDAU: {dau}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {ctr:.2f}'

        return text

    @task()
    def report_last_week(last_weeks_information):
        fig, axes = plt.subplots(2, 2, figsize=(20, 14))

        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=30)

        sns.lineplot(ax = axes[0, 0], data = last_weeks_information, x = 'day', y = 'DAU')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()

        sns.lineplot(ax = axes[0, 1], data = last_weeks_information, x = 'day', y = 'CTR')
        axes[0, 1].set_title('CTR')
        axes[0, 1].grid()

        sns.lineplot(ax = axes[1, 0], data = last_weeks_information, x = 'day', y = 'views')
        axes[1, 0].set_title('Просмотры')
        axes[1, 0].grid()

        sns.lineplot(ax = axes[1, 1], data = last_weeks_information, x = 'day', y = 'likes')
        axes[1, 1].set_title('Лайки')
        axes[1, 1].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Stats.png'
        plt.close()

        return plot_object

    @task()
    def load(text, plot_object):
        # инициализируем бота
        load_dotenv()
        bot = telegram.Bot(token=os.getenv('TOKEN'))
        bot.sendMessage(chat_id=os.getenv('CHAT_ID'), text=text)
        bot.sendPhoto(chat_id=os.getenv('CHAT_ID'), photo=plot_object)

    yesterdays_information = extract_yesterday()
    last_weeks_information = extract_last_week()
    text = report_yesterday(yesterdays_information)
    plot_object = report_last_week(last_weeks_information)
    load(text, plot_object)

u_plusnina_feed_report = u_plusnina_feed_report()
