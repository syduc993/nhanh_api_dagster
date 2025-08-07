from datetime import datetime, timedelta
from dagster import op, In, Out, get_dagster_logger

@op(
    out=Out(str, description="Validated date string"),
    tags={"kind": "validation"}
)
def validate_date_op(context) -> str:
    """Validate and return date string in YYYY-MM-DD format."""
    logger = get_dagster_logger()

    # Lấy date từ partition key
    date_str = context.partition_key

    try:
        # Validate date format
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        validated_date = parsed_date.strftime("%Y-%m-%d")
        logger.info(f"Date validated: {validated_date}")
        return validated_date
    except ValueError as e:
        logger.error(f"Invalid date format {date_str}: {str(e)}")
        raise

@op(
    out=Out(str, description="Today's date"),
    tags={"kind": "utility"}
)
def get_today_op(context) -> str:
    """Get today's date."""
    today = datetime.now().strftime("%Y-%m-%d")
    get_dagster_logger().info(f"Today's date: {today}")
    return today

@op(
    out=Out(str, description="Yesterday's date"),
    tags={"kind": "utility"}
)
def get_yesterday_op(context) -> str:
    """Get yesterday's date."""
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    get_dagster_logger().info(f"Yesterday's date: {yesterday}")
    return yesterday