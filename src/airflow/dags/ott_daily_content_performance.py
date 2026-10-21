from datetime import datetime

from airflow.sdk import DAG  # type: ignore[reportMissingImports]
from airflow.providers.standard.sensors.filesystem import (  # type: ignore[reportMissingImports]
    FileSensor,
)
from airflow.providers.standard.operators.bash import (  # type: ignore[reportMissingImports]
    BashOperator,
)


with DAG(
    dag_id="ott_daily_content_performance",
    description="Daily OTT content performance workflow",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["ott", "content-performance"],
) as dag:

    wait_for_watch_events = FileSensor(
        task_id="wait_for_watch_events",
        filepath="/home/mohit/ott-media-engagement/data/generated/watch_events.csv",
        poke_interval=30,
        timeout=60 * 10,
        mode="poke",
    )

    validate_watch_events = BashOperator(
        task_id="validate_watch_events",
        bash_command=(
            "echo 'Validating OTT watch events...' && "
            "wc -l "
            "/home/mohit/ott-media-engagement/data/generated/watch_events.csv"
        ),
    )

    generate_content_report = BashOperator(
        task_id="generate_content_report",
        bash_command=(
            "echo 'Generating daily OTT content performance report...' && "
            "echo 'Content performance workflow completed successfully.'"
        ),
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command=(
            "echo 'OTT daily content performance workflow completed.'"
        ),
    )

    (
        wait_for_watch_events
        >> validate_watch_events
        >> generate_content_report
        >> notify_success
    )