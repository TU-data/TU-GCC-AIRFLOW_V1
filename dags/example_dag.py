from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    # DAG의 고유한 식별자입니다. Airflow UI에 표시되는 이름이 됩니다.
    dag_id="example_dag",

    # DAG의 실행 주기를 정의합니다.
    # None으로 설정하면, DAG는 스케줄에 따라 자동으로 실행되지 않고 수동으로만 트리거할 수 있습니다.
    # 예: "0 0 * * *" (매일 자정), "@daily" (매일), timedelta(days=1) (하루에 한 번)
    schedule=None,

    # DAG가 처음으로 실행될 수 있는 날짜입니다.
    # 이 날짜 이전의 스케줄은 무시됩니다.
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),

    # True로 설정하면, start_date부터 현재까지 실행되지 않은 모든 스케줄된 DAG 실행을 한 번에 실행합니다.
    # 예를 들어, 어제 하루 동안 Airflow가 꺼져 있었다면, catchup=True일 경우 어제의 DAG 실행을 오늘 실행합니다.
    # 일반적으로 False로 설정하여 예기치 않은 대량 실행을 방지합니다.
    catchup=False,

    # DAG를 분류하고 필터링하는 데 사용되는 태그입니다.
    # Airflow UI에서 태그를 클릭하여 관련된 DAG만 모아볼 수 있습니다.
    tags=["example"],
) as dag:
    # This is a test commit to trigger Cloud Build.
    bash_task = BashOperator(task_id="bash_task", bash_command="echo 'Hello World'")
 
