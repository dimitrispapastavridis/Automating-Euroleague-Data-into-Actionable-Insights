from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.discord.operators.discord_webhook import DiscordWebhookOperator
from datetime import datetime
import requests
from bs4 import BeautifulSoup

default_args = {"start_date": datetime(2022, 12, 11)}

def check_site():
    url = "https://basketball.realgm.com/international/league/1/Euroleague/transactions"
    
    response = requests.get(url)
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return "site_is_up"
        else:
            return "site_is_down"
    except:
        return "site_is_down"
    
def etl_job():
    import pandas as pd
    import requests
    from datetime import datetime

    rows = []

    for year in range(2016,2027):

        url = f"https://basketball.realgm.com/international/league/1/Euroleague/transactions/{year}"
        response = requests.get(url)
        soup = BeautifulSoup(response.content, 'html.parser')

        for i in soup.find('div',class_='main-container').find('div',class_='interior-page').find_all('div',class_='portal widget fullpage'):
            for j in i.find_all('li'):
                rows.append({
                    'date':i.find('h3').text,
                    'description':j.text
                    })

    df = pd.DataFrame(rows)

    df['player'] = df['description'].apply(lambda x : x.split('has signed')[0].strip() if 'has signed' in x else x.split('has left')[0].strip() if 'has left' in x else None)

    df['player'] = df['player'].apply(lambda x: x.split('previously with')[0].strip()[:-1] if isinstance(x, str) and 'previously with' in x else x)

    df['signing_team'] = df['description'].apply(lambda x : x.split('has signed with')[1].strip()[:-1] if 'has signed with' in x else None)

    df['departing_team'] = df['description'].apply(lambda x : x.split('has left')[1].strip()[:-1] if 'has left' in x else None)

    df['date'] = pd.to_datetime(df['date'])

    df['previous_team'] = df['description'].apply(lambda x : x.split('previously with')[1][:x.split('previously with')[1].find(',')].strip() if len(x.split('previously with')) > 1 else None)

    df['synced_at'] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")

    df['synced_at'] = pd.to_datetime(df['synced_at'])

    df.columns = [col.lower() for col in df.columns]

    return df.to_json(orient="records", date_format="iso")

from sqlalchemy import create_engine, text
import pandas as pd

def load_to_postgres(ti):

    json_data = ti.xcom_pull(task_ids='etl')
    df = pd.read_json(json_data)

    engine = create_engine(
    'postgresql+psycopg2:'
    '//postgres:'    
    'docker'            
    '@postgresdb:5432/'      
    'postgres')

    con = engine.connect()

    sql = """
        create table if not exists euroleague_signings (
        date TIMESTAMP,
        description TEXT,
        player TEXT,
        signing_team TEXT,
        departing_team TEXT,
        previous_team TEXT,
        synced_at TIMESTAMP
    );
    """

    with engine.connect().execution_options(autocommit=True) as conn:
        query = conn.execute(text(sql))

    existing = pd.read_sql("SELECT description FROM euroleague_signings", con)

    df_new = df[~df['description'].isin(existing['description'])]

    if len(df_new) != 0:
        df_new.to_sql("euroleague_signings", con=con, if_exists="append", index=False)
        return f"✅ Added {len(df_new)} new records in database."
    else:
        return "ℹ️ The database is updated. No new records"


with DAG("euroleague_signings", schedule='@daily', default_args=default_args, catchup=False) as dag:

    check_site_1 = BranchPythonOperator(task_id='check_site',python_callable=check_site)

    site_is_up = DiscordWebhookOperator(task_id='site_is_up',http_conn_id='http_conn_id',message='✅ Site is up.')

    site_is_down = DiscordWebhookOperator(task_id='site_is_down',http_conn_id='http_conn_id',message='❌ Site is down.')

    etl_task = PythonOperator(task_id='etl',python_callable=etl_job)

    load_to_postgre = PythonOperator(task_id='load_to_postgres',python_callable=load_to_postgres)

    discord_log = DiscordWebhookOperator(task_id='log_to_discord',http_conn_id='http_conn_id',message="{{ ti.xcom_pull(task_ids='load_to_postgres') }}")

    check_site_1 >> [site_is_up, site_is_down]
    site_is_up >> etl_task >> load_to_postgre >> discord_log