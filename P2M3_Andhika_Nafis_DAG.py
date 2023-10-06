'''
=================================================
Milestone 3

Nama  : Andhika Abdurachim Nafis
Batch : HCK-007

Program ini dirancang untuk mengotomatisasi transformasi dan pengisian data dari database PostgreSQL ke Elasticsearch Melalui Airflow.
Data yang di digunakan Graded Challenge ini berfokus untuk mengenali faktor-faktor yang memengaruhi tingkat attrition (keluar) karyawan di perusahaan.
=================================================
'''

from datetime import datetime
import psycopg2 as db
from elasticsearch import Elasticsearch
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pandas as pd

def load_data():
    conn_string = "dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn = db.connect(conn_string)
    df = pd.read_sql("select * from table_m3", conn)
    df.to_csv('P2M3_Andhika-Nafis_Data_Raw.csv',index=False)

def cleaning_data():
    df = pd.read_csv('P2M3_Andhika-Nafis_Data_Raw.csv')
    # Transformasi tipe data yang tidak cocok, kolom-kolom 'Age', 'EmployeeNumber', dan 'MonthlyIncome' akan diubah dari tipe data objek menjadi tipe data integer, yang lebih sesuai untuk tipe data ini.
    data_type = ['Age', 'EmployeeNumber', 'MonthlyIncome'] # Age, EmployeeNumber, dan MonthlyIncome seharusnya tipe int, bukan object
    df[data_type] = df[data_type].astype(int)
    
    # Bagian ini menggantikan nilai-nilai dalam kolom-kolom tertentu dalam dataframe df dengan nilai-nilai yang sesuai berdasarkan mapping yang diberikan.
    mapping = {
        'EnvironmentSatisfaction': {1: 'Low', 2: 'Medium', 3: 'High', 4: 'Very High'},
        'Education': {1: 'Below College', 2: 'College', 3: 'Bachelor', 4: 'Master', 5: 'Doctor'},
        'JobInvolvement': {1: 'Low', 2: 'Medium', 3: 'High', 4: 'Very High'},
        'PerformanceRating': {1: 'Bad', 2: 'Good'},
        'WorkLifeBalance': {1: 'Bad', 2: 'Good', 3: 'Better', 4: 'Best'},
        'RelationshipSatisfaction': {1: 'Low', 2: 'Medium', 3: 'High', 4: 'Very High'}
    }

    df.replace(mapping, inplace=True)

    # mengubah nama semua kolom dalam dataframe df menjadi huruf kecil (lowercase)
    df.columns = df.columns.str.lower()

    return df.values.tolist()

def push_es ():
    es = Elasticsearch("http://elasticsearch:9200") # define elasticsearch ke variable
    df_cleaned=pd.read_csv('/opt/airflow/data/P2M3_Andhika-Nafis_data_clean.csv') # import csv clean
    for i,r in df_cleaned.iterrows(): # looping untuk masuk ke elastic search
        doc=r.to_json()
        res=es.index(index="data_clean", body=doc)
        print(res)  

default_args= {
    'owner': 'NAFIS MILIK ALLAH',
    'start_date': datetime(2023, 9, 29) }

with DAG(
    "Milestone3",
    description='Milestone3',
    schedule_interval='@yearly',
    default_args=default_args, 
    catchup=False) as dag:

    # Task 1
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data

    )

    # Task 2
    cleaning_data = PythonOperator(
        task_id='cleaning_data',
        python_callable=cleaning_data

    )

    #Task 3
    push_es = PythonOperator(
        task_id='push_es',
        python_callable=push_es

    )

    load_data >> cleaning_data >> push_es