# project/repository.py
from dagster import Definitions
from .jobs.nhanh_data_pipeline import nhanh_daily_job, nhanh_backfill_job
from .schedules.daily_schedule import nhanh_daily_schedule
from .resources.nhanh_api_resource import nhanh_api_resource
from .resources.database_resource import database_resource

defs = Definitions(
    jobs=[nhanh_daily_job, nhanh_backfill_job],
    schedules=[nhanh_daily_schedule],
    resources={
        "nhanh_api": nhanh_api_resource,
        "database": database_resource
    }
)
