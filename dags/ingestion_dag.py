import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta


def insert_briefing_data():
    pg_hook = PostgresHook(postgres_conn_id="postgres_adludio_data_science_challenge")
    conn = pg_hook.get_sqlalchemy_engine()

    briefing_data = pd.read_csv('/opt/airflow/data/briefing.csv')
    briefing_data.rename(columns={
        'Submission Date': 'submission_date',
        'Description': 'description',
        'Campaign Objectives': 'campaign_objectives',
        'KPIs': 'kpis',
        'Placement(s)': 'placements',
        'StartDate': 'start_date',
        'EndDate': 'end_date',
        'Serving Location(s)': 'serving_locations',
        'Black/white/audience list included?': 'black_white_audience_list_included',
        'Delivery Requirements (Black/Audience/White List)': 'delivery_requirements',
        'Cost Centre': 'cost_centre',
        'Currency': 'currency',
        'Buy Rate (CPE)': 'buy_rate',
        'Volume Agreed': 'volume_agreed',
        'Gross Cost/Budget': 'gross_cost',
        'Agency Fee': 'agency_fee',
        'Percentage': 'percentage',
        'Flat Fee': 'flat_fee',
        'Net Cost': 'net_cost'
    }, inplace=True)

    briefing_data.to_sql(
        "briefing",
        con=conn,
        if_exists="replace",
        index=False,
    )


def insert_campaigns_inventory_data():
    pg_hook = PostgresHook(postgres_conn_id="postgres_adludio_data_science_challenge")
    conn = pg_hook.get_sqlalchemy_engine()

    campaigns_inventory_data = pd.read_csv('/opt/airflow/data/campaigns_inventory_updated.csv')
    campaigns_inventory_data.to_sql(
        "campaigns_inventory",
        con=conn,
        if_exists="replace",
        index=False,
    )


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['diyye101@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    "start_date": datetime(2022, 8, 8, 2, 30, 00),
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        "ingestion_dag",
        default_args=default_args,
        schedule_interval="0 * * * *",
        catchup=False,
) as dag:
    create_briefing_table_op = PostgresOperator(
        task_id="create_briefing_table",
        postgres_conn_id="postgres_adludio_data_science_challenge",
        sql="""
            create table if not exists briefing (
                campaign_id text,
                campaign_name text,
                submission_date text,
                description text,
                campaign_objectives text,
                kpis text,
                placements text,
                start_date text,
                end_date text,
                serving_locations text,
                black_white_audience_list_included text,
                delivery_requirements text,
                cost_centre text,
                currency text,
                buy_rate double,
                volume_agreed double,
                gross_cost double,
                agency_fee text,
                percentage double,
                flat_fee double,
                net_cost double
            )
            """,
    )
    create_campaigns_inventory_table_op = PostgresOperator(
        task_id="create_campaigns_inventory_table",
        postgres_conn_id="postgres_adludio_data_science_challenge",
        sql="""
                create table if not exists campaigns_inventory (
                    type text,
                    width text,
                    height text,
                    campaign_id text,
                    creative_id text,
                    auction_id text,
                    browser_ts text,
                    game_key text,
                    geo_country text,
                    site_name text,
                    platform_os text,
                    device_type text,
                    browser text
                )
                """,
    )
    create_global_design_table_op = PostgresOperator(
        task_id="create_global_design_table",
        postgres_conn_id="postgres_adludio_data_science_challenge",
        sql="""
                    create table if not exists global_design (
                        game_key text,
                        labels text,
                        text text,
                        colors text,
                        video_data text,
                        eng_type text,
                        direction text,
                        adunit_size text
                    )
                    """,
    )
    ingest_briefing_data_op = PythonOperator(
        task_id="ingest_briefing_data",
        python_callable=insert_briefing_data
    )
    ingest_campaigns_inventory_data_op = PythonOperator(
        task_id="ingest_campaigns_inventory_data",
        python_callable=insert_campaigns_inventory_data
    )

create_briefing_table_op >> create_campaigns_inventory_table_op >> create_global_design_table_op >> ingest_briefing_data_op >> ingest_campaigns_inventory_data_op
