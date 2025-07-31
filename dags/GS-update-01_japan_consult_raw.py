from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
import os
import json
import pandas as pd
import gspread
from google.auth import default
from google.cloud import bigquery

# Slack 알림을 위한 더미 함수 (실제 구현 필요)
# def send_slack_notification(config_key, table_name, success, num_rows=None, error_message=None):
#     print(f"Slack Notification: config_key={config_key}, table_name={table_name}, success={success})")
#     if success:
#         print(f"Successfully loaded {num_rows} rows.")
#     else:
#         print(f"Error: {error_message})")

# [수정] DAG 파일의 위치를 기준으로 절대 경로를 한 번만 정의
DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))
CONFIG_PATH = os.path.join(DAG_FOLDER, 'configs', 'main_configs.json')
SCHEMA_PATH = os.path.join(DAG_FOLDER, 'schemas', '01_japan_consult_raw.csv')

def get_google_credentials():
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets.readonly',
        'https://www.googleapis.com/auth/bigquery'
    ]
    credentials, project = default(scopes=scopes)
    return credentials

# [수정] config, schema 경로와 credentials를 인자로 받도록 변경
def load_sheet_to_bigquery(config: dict, schema_df: pd.DataFrame, credentials):
    try:
        gc = gspread.authorize(credentials)
        spreadsheet = gc.open_by_key(config['google_sheet_id'])
        worksheet = spreadsheet.worksheet(config['sheet_name'])
        
        print(f"[{config['google_sheet_id']}] Google Sheet '{config['sheet_name']}'에서 데이터 읽기 시작...", flush=True)
        data = worksheet.get(config['column_range'])
        
        if len(data) < 2:
            print(f"[{config['google_sheet_id']}] 시트에 데이터가 없거나 헤더만 존재. 작업 종료.", flush=True)
            return 0

        print(f"[{config['google_sheet_id']}] Google Sheet에서 {len(data) - 1}개 행 데이터 읽기 완료.", flush=True)

        sheet_header = [h.strip() for h in data[0]]
        schema_header = schema_df['기존 컬럼명'].tolist()

        if sheet_header != schema_header:
            error_message = (
                f"컬럼명/순서 불일치 오류!\n"
                f"> Google Sheet 헤더: {sheet_header}\n"
                f"> 스키마 파일 헤더: {schema_header}"
            )
            print(f"[{config['google_sheet_id']}] {error_message}", flush=True)
            raise ValueError(error_message)

        print(f"[{config['google_sheet_id']}] 컬럼명 및 순서 검증 완료: Google Sheet와 스키마 파일 헤더 일치.", flush=True)

        expected_col_count = len(schema_header)
        normalized_data = []
        for i, row in enumerate(data[1:]):
            row_len = len(row)
            if row_len != expected_col_count:
                print(f"[{config['google_sheet_id']}] 경고: {i + 4}행 길이({row_len}개)가 헤더({expected_col_count}개)와 다름. 조정합니다.", flush=True)
                normalized_data.append(row + [''] * (expected_col_count - row_len) if row_len < expected_col_count else row[:expected_col_count])
            else:
                normalized_data.append(row)

        if not normalized_data:
            print(f"[{config['google_sheet_id']}] 처리할 데이터 없음. 작업 종료.", flush=True)
            return 0

        df = pd.DataFrame(normalized_data, columns=schema_df['영어 컬럼명'].tolist())
        df = df[schema_df['영어 컬럼명'].tolist()]
        
        print(f"[{config['google_sheet_id']}] 데이터프레임 생성 및 영문 컬럼명 할당 완료. 총 {len(df)} 행.", flush=True)
        
        for _, schema_row in schema_df.iterrows():
            col_name, col_type = schema_row['영어 컬럼명'], schema_row['데이터 타입'].upper()
            if col_name in df.columns:
                try:
                    if col_type == 'STRING':
                        df[col_name] = df[col_name].astype(str).fillna('')
                    elif col_type in ['INTEGER', 'INT64']:
                        df[col_name] = pd.to_numeric(df[col_name].astype(str).str.replace(',', '', regex=False), errors='coerce').fillna(0).astype(int)
                    elif col_type in ['FLOAT', 'FLOAT64']:
                        df[col_name] = pd.to_numeric(df[col_name].astype(str).str.replace(',', '', regex=False), errors='coerce').fillna(0.0).astype(float)
                    elif col_type in ['BOOLEAN', 'BOOL']:
                        df[col_name] = df[col_name].str.lower().isin(['true', 't', '1']).astype(bool)
                    elif col_type == 'DATE':
                        df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.date
                    elif col_type == 'TIME':
                        df[col_name] = pd.to_datetime(df[col_name], errors='coerce').dt.time
                    elif col_type in ['DATETIME', 'TIMESTAMP']:
                        df[col_name] = pd.to_datetime(df[col_name], errors='coerce')
                except Exception as e:
                    print(f"[{config['google_sheet_id']}] 경고: 컬럼 '{col_name}'({col_type}) 변환 오류. 데이터를 건너뜁니다. 오류: {e}", flush=True)
                    df[col_name] = pd.NA
        
        print(f"[{config['google_sheet_id']}] 데이터 타입 변환 완료.", flush=True)

        table_id = config['bigquery_table_id']
        project_id = table_id.split('.')[0]
        bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
        
        bq_schema = [bigquery.SchemaField(row['영어 컬럼명'], row['데이터 타입']) for _, row in schema_df.iterrows()]
        
        job_config = bigquery.LoadJobConfig(schema=bq_schema, write_disposition="WRITE_TRUNCATE", create_disposition="CREATE_IF_NEEDED")
        
        print(f"[{config['google_sheet_id']}] BigQuery 테이블 '{table_id}'에 데이터 로드 시작...", flush=True)
        print(f"[{config['google_sheet_id']}] 데이터프레임 미리보기 (상위 5행):\n{df.head().to_string()}", flush=True)
        print(f"[{config['google_sheet_id']}] 데이터프레임 컬럼 타입:\n{df.dtypes.to_string()}", flush=True)

        job = bigquery_client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()

        print(f"[{config['google_sheet_id']}] 성공: {job.output_rows}개 행을 BigQuery 테이블 {table_id}에 로드했습니다.", flush=True)
        return job.output_rows

    except Exception as e:
        print(f"load_sheet_to_bigquery 함수에서 오류 발생: {e}", flush=True)
        raise

def run_data_load_task(config_key: str):
    table_name = "Unknown Table"
    try:
        print(f"[{config_key}] 작업 시작...", flush=True)
        
        with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
            all_configs = json.load(f)
        
        config = all_configs.get(config_key)
        if not config:
            raise ValueError(f"'{config_key}' 설정을 'main_configs.json'에서 찾을 수 없습니다.")

        schema_df = pd.read_csv(SCHEMA_PATH)
        credentials = get_google_credentials()
        table_name = config.get('bigquery_table_id', 'Unknown Table')
        
        num_rows_loaded = load_sheet_to_bigquery(config, schema_df, credentials)
        
        # send_slack_notification(config_key, table_name, True, num_rows=num_rows_loaded)
        print(f"[{config_key}] 작업 성공적으로 완료.", flush=True)
    
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"[{config_key}] 오류 발생: {error_details}", flush=True)
        # send_slack_notification(config_key, table_name, False, error_message=str(e))
        raise

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