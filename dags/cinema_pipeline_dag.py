from __future__ import annotations

import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator


def build_dag():
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)

    with DAG(
        dag_id="cinema_data_pipeline",
        description="End-to-end cinema data pipeline: ingest, format, combine, index",
        schedule="@daily",
        start_date=yesterday,
        catchup=False,
        tags=["cinema", "bigdata", "nikhil"],
    ) as dag:

        ingest = BashOperator(
            task_id="ingest",
            bash_command=
                "source ~/bigdata-env/bin/activate && cd ~/NIKHIL/BIG-DATA/cinema-data-pipeline && python scripts/ingest_tmdb.py",
        )

        format_task = BashOperator(
            task_id="format",
            bash_command=
                "source ~/bigdata-env/bin/activate && cd ~/NIKHIL/BIG-DATA/cinema-data-pipeline && python scripts/format_spark.py",
        )

        combine = BashOperator(
            task_id="combine",
            bash_command=
                "source ~/bigdata-env/bin/activate && cd ~/NIKHIL/BIG-DATA/cinema-data-pipeline && python scripts/combine_spark.py",
        )

        index = BashOperator(
            task_id="index",
            bash_command=
                "source ~/bigdata-env/bin/activate && cd ~/NIKHIL/BIG-DATA/cinema-data-pipeline && python scripts/index_elasticsearch.py",
        )

        ingest >> format_task >> combine >> index

    return dag


dag = build_dag()
