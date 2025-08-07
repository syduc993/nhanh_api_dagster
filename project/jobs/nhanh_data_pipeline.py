from dagster import job, DailyPartitionsDefinition
from ..config import CONFIG
from ..ops.bills_ops import extract_bills_op, load_bills_op
from ..ops.imexs_ops import extract_imexs_op, load_imexs_op
from ..ops.utils_ops import validate_date_op  # ← Bỏ setup_pipeline_op khỏi import
from ..resources.nhanh_api_resource import nhanh_api_resource
from ..resources.database_resource import database_resource

# Partition definition
daily_partitions = DailyPartitionsDefinition(
    start_date="2024-01-01",
    end_offset=0
)

@job(
    partitions_def=daily_partitions,
    resource_defs={
        "nhanh_api": nhanh_api_resource.configured(CONFIG["api"]),
        "database": database_resource.configured(CONFIG["database"])
    },
    tags={
        "pipeline": "nhanh_data",
        "frequency": "daily"
    }
)
def nhanh_daily_job():
    """Daily job to extract and load Nhanh.vn data."""
    
    # Get partition date and validate
    validated_date = validate_date_op()
    
    # Extract data (không cần setup_pipeline_op nữa)
    bills_data = extract_bills_op(validated_date)
    imexs_data = extract_imexs_op(validated_date)
    
    # Load data - truyền date thay vì pipeline object
    bills_result = load_bills_op(bills_data, validated_date)
    imexs_result = load_imexs_op(imexs_data, validated_date)

@job(
    resource_defs={
        "nhanh_api": nhanh_api_resource.configured(CONFIG["api"]),
        "database": database_resource.configured(CONFIG["database"])
    },
    tags={
        "pipeline": "nhanh_data",
        "frequency": "adhoc"
    }
)
def nhanh_backfill_job():
    """Ad-hoc job for backfilling historical data."""
    
    # Get and validate date
    validated_date = validate_date_op()
    
    # Extract data
    bills_data = extract_bills_op(validated_date)
    imexs_data = extract_imexs_op(validated_date)
    
    # Load data
    bills_result = load_bills_op(bills_data, validated_date)
    imexs_result = load_imexs_op(imexs_data, validated_date)
