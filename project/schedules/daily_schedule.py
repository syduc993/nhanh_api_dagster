from dagster import schedule, RunRequest
from datetime import datetime, timedelta
from ..jobs.nhanh_data_pipeline import nhanh_daily_job

@schedule(
    job=nhanh_daily_job,
    cron_schedule="0 2 * * *",  # Run at 2 AM daily
    tags={
        "schedule": "daily",
        "pipeline": "nhanh_data"
    }
)
def nhanh_daily_schedule(context):
    """Daily schedule for Nhanh.vn data pipeline."""
    # Run for yesterday's data
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    return RunRequest(
        partition_key=yesterday,
        tags={
            "scheduled_date": yesterday,
            "trigger": "schedule"
        }
    )
