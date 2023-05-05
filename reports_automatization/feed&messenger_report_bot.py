import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import os
import matplotlib.pyplot as plt
from datetime import date, timedelta, datetime

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
def u_plusnina_full_report():

    @task()
    # Загружаем данные
    def extract_posts():
        # Запросы для выгрузки данных из базы данных
        query_posts = """
        SELECT post_id,
            count(DISTINCT user_id) AS coverage,
            countIf(action='view') AS views,
            countIf(action='like') AS likes
        FROM simulator_20230320.feed_actions
        WHERE toDate(time) = yesterday()
        GROUP BY post_id
        """
        posts_information = ph.read_clickhouse(query_posts, connection=connection)
        return posts_information

    @task()
    def extract_feed():
        query_feed = """
        SELECT
            toDate(time) AS event_date,
            user_id AS user,
            gender, age, os,
            countIf(action='view') AS views,
            countIf(action='like') AS likes
        FROM simulator_20230320.feed_actions
        WHERE event_date > (yesterday() - 8) AND event_date <= yesterday()
        GROUP BY event_date, user, gender, age, os
        """
        feed_information = ph.read_clickhouse(query_feed, connection=connection)
        return feed_information

    @task()
    def extract_messenger():
        query_messenger = """
        SELECT
            event_date, user, gender, age, os,
            messages_received, messages_sent,
            users_received, users_sent
        FROM
        (SELECT
            toDate(time) AS event_date,
            user_id AS user,
            gender, age, os,
            count() AS messages_sent,
            uniq(reciever_id) AS users_sent
        FROM simulator_20230320.message_actions
        WHERE event_date > (yesterday() - 8) AND event_date <= yesterday()
        GROUP BY event_date, user, gender, age, os) q1
        FULL OUTER JOIN
        (SELECT
            toDate(time) AS event_date,
            reciever_id AS user,
            gender, age, os,
            count() AS messages_received,
            uniq(user_id) AS users_received
        FROM simulator_20230320.message_actions
        WHERE event_date > (yesterday() - 8) AND event_date <= yesterday()
        GROUP BY event_date, user, gender, age, os) q2
        USING user
        """
        messenger_information = ph.read_clickhouse(query_messenger, connection=connection)
        return messenger_information

    @task()
    # Функция для объединения двух таблиц в одну
    def merge(feed_information: pd.DataFrame, messenger_information: pd.DataFrame):
        data_last_week = feed_information.merge(
            messenger_information,
            how='outer',
            on=['event_date', 'user', 'gender', 'age', 'os']
            ).dropna().sort_values(by='event_date').reset_index(drop=True)
        data_last_week['CTR'] = data_last_week['likes']/data_last_week['views']
        return data_last_week



    @task()
    def report_yesterday(data_last_week):
        yestarday = np.datetime64(date.today()-timedelta(days=1))
        data_yesterdays = data_last_week[data_last_week['event_date'] == (yestarday)]
        dau = data_yesterdays['user'].nunique()
        views = data_yesterdays['views'].sum()
        likes = data_yesterdays['likes'].sum()
        ctr = likes/views
        messages = data_yesterdays['messages_sent'].sum()

        text = f'Статистика ленты новостей за вчера:\n\nDAU: {dau}\nПросмотры: {views}\nЛайки: {likes}\nCTR: {ctr*100:.2f} %\nСообщения: {messages:.2f}'

        return text

    @task()
    def report_last_week(data_last_week):
        fig, axes = plt.subplots(2, 2, figsize=(20, 14))
        fig.suptitle('Динамика показателей за последние 7 дней', fontsize=30)

        data_grouped = pd.pivot_table(data_last_week, index='event_date', aggfunc={
                                                                                'user':'nunique',
                                                                                'views':'sum',
                                                                                'likes':'sum',
                                                                                'messages_sent':'sum',
                                                                                'users_received':'sum',
                                                                                'users_sent':'sum'
                                                                            })
        data_grouped['day'] = data_grouped.index
        data_grouped['CTR'] = data_grouped['likes']/data_grouped['views']
        data_grouped['views_per_user'] = data_grouped['views']/data_grouped['user']
        data_grouped['likes_per_user'] = data_grouped['likes']/data_grouped['user']
        data_grouped['messages_per_user'] = data_grouped['messages_sent']/data_grouped['user']


        fig, axes = plt.subplots(2, 2, figsize=(20, 14))

        sns.lineplot(ax = axes[0, 0], data = data_grouped, x = 'day', y = 'user')
        axes[0, 0].set_title('DAU')
        axes[0, 0].grid()

        sns.lineplot(ax = axes[1, 0], data = data_grouped, x = 'day', y = 'CTR')
        axes[1, 0].set_title('CTR')
        axes[1, 0].grid()

        sns.lineplot(ax = axes[0, 1], data = data_grouped, x = 'day', y = 'views', label='views')
        sns.lineplot(ax = axes[0, 1], data = data_grouped, x = 'day', y = 'likes', label='likes')
        sns.lineplot(ax = axes[0, 1], data = data_grouped, x = 'day', y = 'messages_sent', label='messages')
        axes[0, 1].set_title('Просмотры, лайки и сообщения')
        axes[0, 1].grid()

        sns.lineplot(ax = axes[1, 1], data = data_grouped, x = 'day', y = 'views_per_user', label='views_per_user')
        sns.lineplot(ax = axes[1, 1], data = data_grouped, x = 'day', y = 'likes_per_user', label='likes_per_user')
        sns.lineplot(ax = axes[1, 1], data = data_grouped, x = 'day', y = 'messages_per_user', label='messages_per_user')
        axes[1, 1].set_title('Средние просмотры, лайки и сообщения на пользователя')
        axes[1, 1].grid()

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'Stats.png'
        plt.close()

        return plot_object


    @task()
    def report_top_users(data_last_week):
        top_users = pd.pivot_table(data_last_week, index='user', aggfunc={
                                                                        'views':'sum',
                                                                        'likes':'sum',
                                                                        'messages_sent':'sum',
                                                                        'users_received':'sum',
                                                                        'users_sent':'sum',
                                                                        'CTR':'mean'
                                                                        }).sort_values(by='CTR', ascending=False).reset_index(drop=True).head(30)

        top_users['user'] = top_users.index

        file_object1 = io.StringIO()
        top_users.to_csv(file_object1)
        file_object1.name = 'top_active_users.csv'
        file_object1.seek(0)

        return file_object1

    @task()
    def report_top_posts(posts_information):
        top_posts = pd.pivot_table(posts_information, index='post_id', aggfunc={
                                                                        'coverage':'sum',
                                                                        'views':'sum',
                                                                        'likes':'sum'
                                                                        }).sort_values(by='likes', ascending=False).reset_index(drop=True).head(30)
        top_posts['CTR'] = top_posts['likes']/top_posts['views']
        top_posts['post_id'] = top_posts.index

        file_object2 = io.StringIO()
        top_posts.to_csv(file_object2)
        file_object2.name = 'top_posts.csv'
        file_object2.seek(0)

        return file_object2


    @task()
    def load(text, plot_object, file_object1, file_object2):
        # инициализируем бота
        load_dotenv()
        bot = telegram.Bot(token=os.getenv('TOKEN'))
        bot.sendMessage(chat_id=os.getenv('CHAT_ID'), text=text)
        bot.sendPhoto(chat_id=os.getenv('CHAT_ID'), photo=plot_object)
        bot.sendDocument(chat_id=os.getenv('CHAT_ID'), document=file_object1)
        bot.sendDocument(chat_id=os.getenv('CHAT_ID'), document=file_object2)


    # Последовательный вызов задач DAG'a
    feed_information = extract_feed()
    messenger_information = extract_messenger()
    data_last_week = merge(feed_information, messenger_information)
    posts_information = extract_posts()

    text = report_yesterday(data_last_week)
    plot_object = report_last_week(data_last_week)
    file_objects1 = report_top_users(data_last_week)
    file_objects2 = report_top_posts(posts_information)
    load(text, plot_object, file_objects1, file_objects2)

u_plusnina_full_report = u_plusnina_full_report()
