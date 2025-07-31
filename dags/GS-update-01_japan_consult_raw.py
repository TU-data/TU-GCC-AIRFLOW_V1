from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pendulum
import os
import json
import pandas as pd
import gspread
from google.auth import default
from google.cloud import bigquery

# Slack 알림을 위한 더미 함수 (실제 구현 필요)
# def send_slack_notification(config_key, table_name, success, num_rows=None, error_message=None):
#     print(f"Slack Notification: config_key={config_key}, table_name={table_name}, success={success}")
#     if success:
#         print(f"Successfully loaded {num_rows} rows.")
#     else:
#         print(f"Error: {error_message}")

def get_google_credentials():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/bigquery'
    ]
    credentials, project = default(scopes=scopes)
    return credentials

def load_sheet_to_bigquery(config_key: str):
    try:
        # Airflow 환경에서는 DAG 파일이 dags 폴더에 있으므로 경로를 조정
        config_path = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'configs', 'main_configs.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            all_configs = json.load(f)
        
        config = all_configs.get(config_key)
        if not config:
            raise ValueError(f"'{config_key}'에 해당하는 설정을 'main_configs.json'에서 찾을 수 없습니다.")

        schema_path = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'schemas', '01_japan_consult_raw.csv')
        schema_df = pd.read_csv(schema_path)
    except FileNotFoundError as e:
        raise ValueError(f"설정 또는 스키마 파일({e.filename})이 없습니다.")
    except Exception as e:
        raise e

    credentials = get_google_credentials()
    gc = gspread.authorize(credentials)
    
    spreadsheet = gc.open_by_key(config['google_sheet_id'])
    worksheet = spreadsheet.worksheet(config['sheet_name'])
    
    print(f"[{config_key}] Google Sheet '{config['sheet_name']}'에서 데이터 읽기 시작 (범위: '{config['column_range']}')...", flush=True)
    data = worksheet.get(config['column_range'])
     
    if len(data) < 2:
        print(f"[{config_key}] 시트에 데이터가 없거나 헤더만 존재합니다. 작업을 종료합니다.", flush=True)
        return

    print(f"[{config_key}] Google Sheet에서 {len(data) - 1}개의 행 데이터 읽기 완료.", flush=True)

    sheet_header = [h.strip() for h in data[0]]
    schema_header = schema_df['기존 컬럼명'].tolist()

    if sheet_header != schema_header:
        error_message = (
            f"컬럼명/순서 불일치 오류!\n"
            f"> Google Sheet 헤더: {sheet_header}\n"
            f"> 스키마 파일 헤더: {schema_header}"
        )
        print(f"[{config_key}] {error_message}", flush=True)
        raise ValueError(error_message)

    print(f"[{config_key}] 컬럼명 및 순서 검증 완료: Google Sheet와 스키마 파일의 헤더가 일치합니다.", flush=True)

    expected_col_count = len(schema_header)
    normalized_data = []
    for i, row in enumerate(data[1:]):
        row_len = len(row)
        if row_len != expected_col_count:
            print(f"[{config_key}] 경고: 시트의 {i + 4}행 길이가 헤더({expected_col_count}개)와 다릅니다({row_len}개). 길이를 조정합니다.", flush=True)
            if row_len < expected_col_count:
                normalized_data.append(row + [''] * (expected_col_count - row_len))
            else:
                normalized_data.append(row[:expected_col_count])
        else:
            normalized_data.append(row)

    if not normalized_data:
        print(f"[{config_key}] 처리할 데이터가 없습니다. 작업을 종료합니다.", flush=True)
        return

    df = pd.DataFrame(normalized_data, columns=schema_df['영어 컬럼명'].tolist())
    
    print(f"[{config_key}] 데이터프레임 생성 및 영문 컬럼명 할당 완료. 총 {len(df)} 행.", flush=True)

    final_columns = schema_df['영어 컬럼명'].tolist()
    df = df[final_columns]
    
    print(f"[{config_key}] 컬럼명 매핑 및 순서 고정 완료. 최종 컬럼: {df.columns.tolist()}", flush=True)

    print(f"[{config_key}] 스키마에 따라 데이터 타입을 변환합니다...", flush=True)
    for index, row in schema_df.iterrows():
        col_name = row['영어 컬럼명']
        col_type = row['데이터 타입'].upper()
        
        if col_name in df.columns:
            try:
                if col_type == 'STRING':
                    df[col_name] = df[col_name].astype(str).fillna('')
                elif col_type in ['INTEGER', 'INT64']:
                    df[col_name] = df[col_name].astype(str).str.replace(',', '', regex=False)
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0).astype(int)
                elif col_type in ['FLOAT', 'FLOAT64']:
                    df[col_name] = df[col_name].astype(str).str.replace(',', '', regex=False)
                    df[col_name] = pd.to_numeric(df[col_name], errors='coerce').fillna(0.0).astype(float)
                elif col_type in ['BOOLEAN', 'BOOL']:
                    df[col_name] = df[col_name].str.lower().isin(['true', 't', '1']).astype(bool)
                elif col_type == 'DATE':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date
                elif col_type == 'TIME':
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.time
                elif col_type in ['DATETIME', 'TIMESTAMP']:
                    df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
            except Exception as e:
                print(f"[{config_key}] 경고: 컬럼 '{col_name}'({col_type}) 변환 중 오류 발생. 데이터를 건너뜁니다. 오류: {e}", flush=True)
                df[col_name] = pd.NA

    print(f"[{config_key}] 데이터 타입 변환 완료.", flush=True)

    table_id = config['bigquery_table_id']
    project_id = table_id.split('.')[0]
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    
    bq_schema = [
        bigquery.SchemaField(row['영어 컬럼명'], row['데이터 타입'])
        for index, row in schema_df.iterrows()
    ]
    
    job_config = bigquery.LoadJobConfig(
        schema=bq_schema,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED"
    )

    print(f"[{config_key}] BigQuery 테이블 '{table_id}'에 데이터 로드 시작 (스키마: {bq_schema})...", flush=True)
    print(f"[{config_key}] 데이터프레임 미리보기 (상위 5행):\n{df.head().to_string()}", flush=True)
    print(f"[{config_key}] 데이터프레임 컬럼 타입:\n{df.dtypes.to_string()}", flush=True)

    job = bigquery_client.load_table_from_dataframe(
        df, table_id, job_config=job_config
    )
    job.result()

    print(f"[{config_key}] 성공: {job.output_rows}개의 행을 BigQuery 테이블 {table_id}에 로드했습니다.", flush=True)
    return job.output_rows

def run_data_load_task(config_key):
    table_name = ""
    try:
        # Airflow 환경에서는 DAG 파일이 dags 폴더에 있으므로 경로를 조정
        config_path = os.path.join(os.environ.get('AIRFLOW_HOME', '/opt/airflow'), 'dags', 'configs', 'main_configs.json')
        with open(config_path, 'r', encoding='utf-8') as f:
            all_configs = json.load(f)
        config = all_configs.get(config_key)
        if config:
            table_name = config.get('bigquery_table_id', 'Unknown Table')

        print(f"[{config_key}] 작업 시작...", flush=True)
        num_rows_loaded = load_sheet_to_bigquery(config_key)
        # send_slack_notification(config_key, table_name, True, num_rows=num_rows_loaded)
        print(f"[{config_key}] 작업 성공적으로 완료.", flush=True)
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"[{config_key}] 오류 발생: {error_details}", flush=True)
        # send_slack_notification(config_key, table_name, False, error_message=str(e))
        raise # Airflow 태스크 실패를 위해 예외 다시 발생

with DAG(
    dag_id='GS-update-01_japan_consult_raw',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval=None, # 스케줄러에서 수동으로 트리거하거나, Cloud Scheduler에서 HTTP 요청으로 트리거
    catchup=False,
    tags=['google_sheet', 'bigquery', 'data_load'],
) as dag:
    load_data_task = PythonOperator(
        task_id='load_japan_consult_raw_data',
        python_callable=run_data_load_task,
        op_kwargs={'config_key': 'GS-update-01_japan_consult_raw'},
    ) 