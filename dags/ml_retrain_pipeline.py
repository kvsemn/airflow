from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import requests


# ====== Переменные окружения ======
MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0.0")
TELEGRAM_BOT_TOKEN = os.getenv("8510506501:AAHuTNqDJFrgDn4pdHls5TYIi3K-RaJx38o")
TELEGRAM_CHAT_ID = os.getenv("5025462610")


# ====== Задачи пайплайна ======
def train_model():
    print(f"Модель версии {MODEL_VERSION} обучена")


def evaluate_model():
    # Здесь может быть логика проверки метрик
    print("Модель оценена, метрики в допустимых пределах")


def deploy_model():
    # Здесь может быть регистрация модели в MLflow / деплой
    print(f"Модель версии {MODEL_VERSION} выведена в продакшен")


def send_telegram_message():
    message = f"🚀 Новая модель в продакшене!\nВерсия: {MODEL_VERSION}"
    url = (
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
        f"/sendMessage?chat_id={TELEGRAM_CHAT_ID}&text={message}"
    )
    response = requests.get(url)
    response.raise_for_status()
    print("Уведомление в Telegram отправлено")


# ====== Описание DAG ======

with DAG(
    dag_id="ml_retrain_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",   # <-- вот замена schedule_interval
    catchup=False,
    tags=["ml", "retraining", "mlops"],
) as dag:


    train = PythonOperator(
        task_id="train_model",
        python_callable=train_model,
    )

    evaluate = PythonOperator(
        task_id="evaluate_model",
        python_callable=evaluate_model,
    )

    deploy = PythonOperator(
        task_id="deploy_model",
        python_callable=deploy_model,
    )

    notify = PythonOperator(
        task_id="notify_success",
        python_callable=send_telegram_message,
    )

    # ====== Зависимости ======
    train >> evaluate >> deploy >> notify
