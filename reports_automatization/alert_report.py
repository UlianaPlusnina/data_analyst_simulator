import pandahouse as ph
import pandas as pd
import numpy as np
import seaborn as sns
import os
import matplotlib.pyplot as plt
from datetime import date, timedelta, datetime
from sklearn.ensemble import IsolationForest
from airflow.decorators import dag, task


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
def u_plusnina_alert_report():

    @task()
    def extract_feed():
        query_feed = """
        SELECT
            toStartOfFifteenMinutes(time) AS ts,
            formatDateTime(ts, '%R') AS hm,
                    toDate(time) AS date,
            COUNT(DISTINCT user_id) AS user,
            countIf(action='view') AS views,
            countIf(action='like') AS likes
        FROM simulator_20230320.feed_actions
        WHERE ts >= today()-1 and time < toStartOfFifteenMinutes(now())

        GROUP BY ts, date, hm
        ORDER BY ts
        """
        feed_information = ph.read_clickhouse(query_feed, connection=connection)
        feed_information['CTR'] = feed_information['likes']/feed_information['views']

        return feed_information

    @task()
    def extract_messenger():
        query_messenger = """
        SELECT
                ts,
                formatDateTime(ts, '%R') AS hm,
                date,
                sum(user) AS users,
                sum(messages_received) AS messages_received, sum(messages_sent) AS messages_sent
        FROM
            (SELECT
                toStartOfFifteenMinutes(time) AS ts,
                toDate(time) AS date,
                user_id AS user,
                count() AS messages_sent
            FROM simulator_20230320.message_actions
            WHERE time >= today()-1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, user) q1
        FULL OUTER JOIN
            (SELECT
                toStartOfFifteenMinutes(time) AS ts,
                toDate(time) AS date,
                reciever_id AS user,
                count() AS messages_received
            FROM simulator_20230320.message_actions
            WHERE time >= today()-1 and time < toStartOfFifteenMinutes(now())
            GROUP BY ts, date, user) q2
        USING user
        GROUP BY ts, date, hm
        ORDER BY ts
        """
        messenger_information = ph.read_clickhouse(query_messenger, connection=connection)
        return messenger_information

    @task()
    def merge(feed_information: pd.DataFrame, messenger_information: pd.DataFrame):
        data = feed_information.merge(
            messenger_information,
            how='outer',
            on=['ts', 'hm', 'date']
            ).dropna().sort_values(by='ts').reset_index(drop=True)
        return data

    @task()
    def check_anomaly(data, metric:str, n_estimators=10, n=4):

        '''
        Функция осуществляет поиск аномалий методом изоляционного леса

        Параметры:
        data - датафрейм;
        metric - метрика для проверки на аномалии;
        n - количество временных промежутков;


        Функция возвращает:
        is_alert - оповещение, есть отклонение (1) или нет (0)
        '''

        isolation_forest = IsolationForest(n_estimators=n_estimators)
        model = isolation_forest.fit(data.loc[len(data)-n:len(data)-2,  metric].values.reshape(-1, 1))
        is_anomaly = model.predict([[data.loc[len(data)-1,  metric]]])
        # we are predicting one value, so picking first
        if is_anomaly[0] == -1:
            is_alert = 1
        else:
            is_alert = 0
        return is_alert

    @task()
    def msg_report_alerts(is_alert, data, metric:str):
        '''
        Функция визуализирует данные для аномалий и формирует отчет

        Параметры:
        data - датафрейм;
        metric -  метрика для проверки на аномалии;

        Функция возвращает:
        msg - текстовое оповещение;
        '''
        msg = ''
        if is_alert == 1:

            current_val = data.loc[len(data) - 1,  metric]
            prev_val = data.loc[len(data) - 2,  metric]
            last_val_diff = abs(current_val - prev_val) / prev_val * 100 if prev_val != 0 else 0
            time_alert = data.loc[len(data) - 1, 'hm']

            msg = f"""Метрика {metric}:
            \n текущее значение {current_val:.2f}
            \n отклонение от предыдущего значения {last_val_diff:.2%}
            \n время {time_alert}"""

        return msg

    @task()
    def plot_report_alerts(is_alert, data, metric:str):
        '''
        Функция визуализирует данные для аномалий и формирует отчет

        Параметры:
        data - датафрейм;
        metric -  метрика для проверки на аномалии;

        Функция возвращает:
        plot_object - график изменения метрики;
        '''
        plot_object = None

        if is_alert == 1:

            sns.set(rc={'figure.figsize': (16,10)})
            ax = sns.lineplot(x=data.loc[len(data) - 6:len(data) - 1, 'ts'], y=data.loc[len(data) - 20:len(data) - 1,metric], label=metric)
            ax.grid(True)

            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = f'{metric}.png'
            plt.close()

        return plot_object

    @task()
    def load(msg, plot_object):
        # инициализируем бота
        load_dotenv()
        if msg != '':
            bot = telegram.Bot(token=os.getenv('TOKEN'))
            bot.sendMessage(chat_id=os.getenv('CHAT_ID'), text=msg)
            bot.sendPhoto(chat_id=os.getenv('CHAT_ID'), photo=plot_object)

    # Последовательный вызов задач DAG'a
    feed_information = extract_feed()
    messenger_information = extract_messenger()
    data = merge(feed_information, messenger_information)

    metrics = ['user', 'views', 'likes', 'CTR', 'messages_sent', 'messages_received']
    for metric in metrics:

        is_alert = check_anomaly(data, metric, n_estimators=10, n=20)
        msg = msg_report_alerts(is_alert, data, metric)
        plot_object = plot_report_alerts(is_alert, data, metric)
        load(msg, plot_object)

u_plusnina_alert_report = u_plusnina_alert_report()
