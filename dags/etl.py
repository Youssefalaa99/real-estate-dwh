from datetime import datetime, timedelta
import time,json,re
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


default_args = {
    'owner': 'Youssef',
    'depends_on_past': False,
    'email': ['youssef@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Helpers
def test_conn():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    sql = f"SELECT COUNT(*) FROM data_source.sales"
    result = hook.get_first(sql)
    print(result)
    return result[0] if result else 0

def date_to_id(date_val):
        if pd.isna(date_val):
            return None
        try:
            return int(date_val.strftime('%Y%m%d'))
        except:
            return None

def clean_best_time(val):
    if pd.isna(val):
        return "NA"

    v = str(val).lower().strip()

    if any(x in v for x in ["أي وقت", "اي وقت", "anytime", "any time", "any", "all day", "all time"]):
        return "ANYTIME"

    # Working hours
    if re.search(r'(8|9|10)\s*(am)?.*(4|5|6)\s*(pm)', v):
        return "WORKING_HOURS"

    # Morning
    if "morning" in v or "am" in v or "before noon" in v or "صباح" in v:
        return "MORNING"

    # Evening
    if (
        "evening" in v
        or "pm" in v
        or "after noon" in v
        or "after" in v
        or "مساء" in v
    ):
        return "EVENING"

    # Junk / numbers only / irrelevant
    if re.fullmatch(r'[\d\W]+', v) or "mail" in v:
        return f"NA - {val}"

    return "NA"

def extract_data():
    source_hook = PostgresHook(postgres_conn_id='postgres_default')
    source_schema = "data_source"
    trgt_schema = "dwh"


    # Extract leads
    leads_query = f"""
    SELECT 
        id as lead_id,
        user_id,
        customer_id,
        compound_id,
        developer_id,
        area_id,
        lead_type_id,
        lead_type,
        budget,
        buyer as is_buyer,
        seller as is_seller,
        commercial as is_commercial,
        merged as is_merged,
        do_not_call,
        meeting_flag,
        lead_source,
        campaign,
        method_of_contact,
        status_name,
        best_time_to_call as time_to_call,
        location,
        created_at,
        updated_at,
        date_of_last_request,
        date_of_last_contact
    FROM {source_schema}.leads
    WHERE updated_at > (SELECT COALESCE(MAX(updated_at),'1900-01-01') FROM {trgt_schema}.fct_leads WHERE updated_at IS NOT NULL) 
    OR created_at > (SELECT COALESCE(MAX(created_at),'1900-01-01') FROM {trgt_schema}.fct_leads WHERE created_at IS NOT NULL)
    """
    
    # Extract sales
    sales_query = f"""
    SELECT 
        id as sale_id,
        lead_id,
        date_of_reservation,
        date_of_contraction,
        property_type_id,
        unit_location,
        unit_value,
        expected_value,
        actual_value,
        years_of_payment,
        sale_category,
        property_type,
        area_id,
        compound_id
    FROM {source_schema}.sales
    WHERE date_of_reservation > (SELECT COALESCE(TO_DATE(MAX(date_of_reservation_id)::TEXT, 'YYYYMMDD'),'1900-01-01') FROM {trgt_schema}.fct_sales WHERE date_of_reservation_id IS NOT NULL)
    """
    
    leads_df = source_hook.get_pandas_df(leads_query)
    sales_df = source_hook.get_pandas_df(sales_query)

    # print(len(leads_df))
    # print(len(sales_df))

    print(sales_df.info())
    print(leads_df.info())

    return {'leads': leads_df, 'sales': sales_df}

def transform_data(ti):
    data = ti.xcom_pull(task_ids='extract_task')
    
    leads_df = data['leads']
    sales_df = data['sales']

    # Clean leads data
    if not leads_df.empty:
        # Handle missing values
        leads_df.fillna({
            'user_id': 0,
            'compound_id': 0,
            'developer_id': 0,
            'area_id': 0,
            'budget': 0,
            'is_buyer': False,
            'is_seller': False,
            'is_commercial': False,
            'is_merged': False,
            'do_not_call': False,
            'meeting_flag': 0,
            'lead_type_id': 1
        }, inplace=True)

        # Drop duplicate leads
        leads_df = leads_df.drop_duplicates(subset=["lead_id"])

        # Clean best_time_to_buy
        leads_df["time_to_call"] = leads_df["time_to_call"].apply(clean_best_time)

        # Define col types
        col_types = {"user_id":"int64","compound_id":"int64","developer_id":"int64","area_id":"int64","lead_type_id":"int64","lead_type":"str","budget":"int64","meeting_flag":"int64","lead_source": "str","campaign": "str","method_of_contact": "str","status_name": "str","time_to_call": "str","location": "str","date_of_last_contact":"datetime64[ns]"}
        leads_df = leads_df.astype(col_types)



    # Clean sales data
    if not sales_df.empty:
        # Fill missing values
        sales_df.fillna({
            'unit_value': 0,
            'expected_value': 0,
            'actual_value': 0,
            'years_of_payment': 0,
            'compound_id': 0,
            'area_id': 0
        }, inplace=True)

        # Define col types
        col_types = {"date_of_reservation":"datetime64[ns]","date_of_contraction":"datetime64[ns]","property_type_id":"int64","unit_location":"str","unit_value":"int64","expected_value":"int64","actual_value":"int64","years_of_payment":"float64","sale_category":"str","property_type":"str","area_id":"int64","compound_id":"int64"}
        sales_df = sales_df.astype(col_types)    
    
    # print(leads_df.head(10))
    # print(sales_df.head(10))
    
    return {'leads': leads_df, 'sales': sales_df}

def load_dims(ti):
    data = ti.xcom_pull(task_ids='transform_task')

    leads_df = data['leads']
    sales_df = data['sales']

    trgt_hook = PostgresHook(postgres_conn_id='postgres_default')
    trgt_schema = "dwh"

    engine = create_engine(trgt_hook.get_uri())

    # Load dim_property_type
    if not sales_df.empty:
        dim_property_type_df = sales_df[['property_type_id','property_type']].dropna().drop_duplicates(subset=["property_type_id"])

        # Write to staging
        dim_property_type_df.to_sql(
            "stg_property_type",
            engine,
            schema="staging",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000
        )
        
        # Upsert dim_property_type
        upsert_query = f"""
            INSERT INTO {trgt_schema}.dim_property_type (property_type_id, property_type)
            SELECT
                property_type_id, 
                property_type
            FROM staging.stg_property_type
            ON CONFLICT (property_type_id) DO UPDATE SET
                property_type = EXCLUDED.property_type
            """
        
        trgt_hook.run(upsert_query)
        
        print(f"Loaded {len(dim_property_type_df)} rows into dim_property_type")

    # Load dim_lead_type
    if not leads_df.empty:
        dim_lead_type_df = leads_df[['lead_type_id','lead_type']].dropna().drop_duplicates(subset=["lead_type_id"])

        # Write to staging
        dim_lead_type_df.to_sql(
            "stg_lead_type",
            engine,
            schema="staging",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000
        )

        # Upsert dim_lead_type
        upsert_query = f"""
            INSERT INTO {trgt_schema}.dim_lead_type (lead_type_id, lead_type)
            SELECT
                lead_type_id, 
                lead_type
            FROM staging.stg_lead_type
            ON CONFLICT (lead_type_id) DO UPDATE SET
                lead_type = EXCLUDED.lead_type
            """
        
        trgt_hook.run(upsert_query)
        
        print(f"Loaded {len(dim_lead_type_df)} rows into dim_lead_type")


    # TODO: Rest of the dimensions




def load_facts(ti):
    data = ti.xcom_pull(task_ids='transform_task')

    leads_df = data['leads']
    sales_df = data['sales']

    trgt_hook = PostgresHook(postgres_conn_id='postgres_default')
    trgt_schema = "dwh"

    engine = create_engine(trgt_hook.get_uri())

    # Load fct_leads
    if not leads_df.empty:
        leads_df = leads_df[['lead_id','customer_id','user_id','compound_id','lead_type_id','budget','is_buyer','is_seller','is_commercial','is_merged','do_not_call','meeting_flag','lead_source','campaign','method_of_contact','status_name','time_to_call','created_at','updated_at','date_of_last_request','date_of_last_contact']]

        # Write to staging
        print("Inserting into staging leads..")

        leads_df.to_sql(
            "stg_leads",
            engine,
            schema="staging",
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000
        )
        
        # Upsert fct_leads
        upsert_query = f"""
            INSERT INTO {trgt_schema}.fct_leads
            SELECT
                *
            FROM staging.stg_leads
            ON CONFLICT (lead_id) DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                user_id = EXCLUDED.user_id,
                compound_id = EXCLUDED.compound_id,
                lead_type_id = EXCLUDED.lead_type_id,
                budget = EXCLUDED.budget,
                is_buyer = EXCLUDED.is_buyer,
                is_seller = EXCLUDED.is_seller,
                is_commercial = EXCLUDED.is_commercial,
                is_merged = EXCLUDED.is_merged,
                do_not_call = EXCLUDED.do_not_call,
                meeting_flag = EXCLUDED.meeting_flag,
                lead_source = EXCLUDED.lead_source,
                campaign = EXCLUDED.campaign,
                method_of_contact = EXCLUDED.method_of_contact,
                status_name = EXCLUDED.status_name,
                time_to_call = EXCLUDED.time_to_call,
                created_at = EXCLUDED.created_at,
                updated_at = EXCLUDED.updated_at,
                date_of_last_request = EXCLUDED.date_of_last_request,
                date_of_last_contact = EXCLUDED.date_of_last_contact
            """
        
        trgt_hook.run(upsert_query)
        
        print(f"Loaded {len(leads_df)} rows into fct_leads")



    # Load fct_sales
    if not sales_df.empty:
        # Convert dates to date_id
        sales_df['date_of_reservation_id'] = sales_df['date_of_reservation'].apply(date_to_id)
        sales_df['date_of_contraction_id'] = sales_df['date_of_contraction'].apply(date_to_id)

        sales_df = sales_df[['sale_id','lead_id','date_of_reservation_id','date_of_contraction_id','property_type_id','compound_id','unit_value','expected_value','actual_value','years_of_payment','sale_category']]

        # Write to staging
        print("Inserting into fct_sales..")

        sales_df.to_sql(
            "fct_sales",
            engine,
            schema=trgt_schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )
        
        print(f"Loaded {len(sales_df)} rows into fct_sales")


with DAG(
    dag_id='leads_sales_daily_etl',
    default_args=default_args,
    description='ETL that extracts leads and sales table and load them into DWH',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    # tags=['']  
) as dag:
    
    start_task = EmptyOperator(task_id="start_task")

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_data,
    )

    load_dims_task = PythonOperator(
        task_id='load_dims_task',
        python_callable=load_dims,
    )

    load_fcts_task = PythonOperator(
        task_id='load_fcts_task',
        python_callable=load_facts,
    )

    end_task = EmptyOperator(task_id="end_task")


    # Task dependecies
    start_task >> extract_task >> transform_task >> [load_dims_task, load_fcts_task] >> end_task