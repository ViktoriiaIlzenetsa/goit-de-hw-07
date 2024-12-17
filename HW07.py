from airflow import DAG
from datetime import datetime
from airflow.sensors.sql import SqlSensor
from airflow.operators.mysql_operator import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule as tr
from airflow.utils.state import State
import random
import time

# Функція для генерації випадкової медалі
def pick_medal_f(ti):
    medals_list = ['Bronze', 'Silver', 'Gold']
    medal = random.choice(medals_list)
    print(f"Generated medal: {medal}")

    return medal


def pick_medal_task_f(ti):
    medal = ti.xcom_pull(task_ids='pick_medal')

    if medal == "Bronze":
        return "calc_Bronze"
    elif medal == "Silver":
        return "calc_Silver"
    else:
        return "calc_Gold"
    
def generate_delay(ti):
    time.sleep(4)

# Аргументи за замовчуванням для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

# Назва з'єднання з базою даних MySQL
connection_name = "goit_mysql_db_vika"

# Визначення DAG
with DAG(
        'working_with_mysql',
        default_args=default_args,
        schedule_interval=None,  # DAG не має запланованого інтервалу виконання
        catchup=False,  # Вимкнути запуск пропущених задач
        tags=["viktoriia"]  # Теги для класифікації DAG
) as dag:

    # Завдання для створення таблиці (якщо не існує)
    create_table = MySqlOperator(
        task_id='create_table',
        mysql_conn_id=connection_name,
        sql="""
        CREATE DATABASE IF NOT EXISTS viktoriia;

        CREATE TABLE IF NOT EXISTS viktoriia.medals (
        id INT AUTO_INCREMENT PRIMARY KEY, 
        medal_type TEXT, 
        count INT, 
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )

    pick_medal = PythonOperator(
        task_id='pick_medal',
        python_callable=pick_medal_f,
    )

    pick_medal_task = BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_medal_task_f,
    )

    calc_Bronze = MySqlOperator(
        task_id='calc_Bronze',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO viktoriia.medals (medal_type, count)
            SELECT 'Bronze', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """,
    )

    calc_Silver = MySqlOperator(
        task_id='calc_Silver',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO viktoriia.medals (medal_type, count)
            SELECT 'Silver', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver'; 
        """,
    )

    calc_Gold = MySqlOperator(
        task_id='calc_Gold',
        mysql_conn_id=connection_name,
        sql="""
            INSERT INTO viktoriia.medals (medal_type, count)
            SELECT 'Gold', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';  
        """,
    )

    generate_delay_task = PythonOperator(
        task_id='generate_delay',
        python_callable=generate_delay,
        trigger_rule=tr.ONE_SUCCESS
    )


    # Сенсор 
    check_for_correctness = SqlSensor(
        task_id='check_for_correctness',
        conn_id=connection_name,
        sql="""SELECT 
            CASE 
                WHEN TIMESTAMPDIFF(SECOND, (SELECT MAX(created_at) FROM viktoriia.medals), CURRENT_TIMESTAMP) <= 30 
                THEN 1 
                ELSE 0 
            END AS is_recent;""",
        mode='poke',  # Режим перевірки: періодична перевірка умови
        poke_interval=5,  # Перевірка кожні 5 секунд
        timeout=6,  # Тайм-аут після 6 секунд (1 повторна перевірка)
    )

  

    # Встановлення залежностей між завданнями
    create_table >> pick_medal >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay_task
    generate_delay_task >> check_for_correctness
