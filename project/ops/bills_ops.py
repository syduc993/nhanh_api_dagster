from dagster import op, In, Out, get_dagster_logger, Field
import dlt

@op(
    required_resource_keys={"nhanh_api"},
    ins={
        "date": In(str, description="Date to extract bills for")
    },
    out=Out(list, description="Bills data extracted from API"),
    config_schema={
        "depot_id": Field(int, default_value=155286),
        "mode": Field(int, default_value=6)
    },
    tags={"kind": "extract", "source": "bills"}
)
def extract_bills_op(context, date: str) -> list:
    """Extract bills data from Nhanh.vn API."""
    logger = get_dagster_logger()
    api = context.resources.nhanh_api
    
    depot_id = context.op_config["depot_id"]
    mode = context.op_config["mode"]
    
    data_dict = {
        "depotId": depot_id,
        "mode": mode,
        "fromDate": date,
        "toDate": date
    }
    
    logger.info(f"Extracting bills for date {date} with params: {data_dict}")
    
    bills_data = api.extract_data(
        endpoint="bill/search",
        data_dict=data_dict,
        data_key="bill"
    )
    
    logger.info(f"Successfully extracted {len(bills_data)} bills records")
    return bills_data

@dlt.resource
def bills_dlt_resource(bills_data: list):
    """DLT resource for bills data."""
    if bills_data:
        yield bills_data
    else:
        # Yield empty list to avoid DLT errors
        yield []

@op(
    required_resource_keys={"database"},  # Thêm database resource
    ins={
        "bills_data": In(list, description="Bills data to load"),
        "date": In(str, description="Date for pipeline setup")  # Đổi từ pipeline object sang date
    },
    out=Out(str, description="Load result message"),
    tags={"kind": "load", "destination": "duckdb"}
)
def load_bills_op(context, bills_data: list, date: str) -> str:
    """Load bills data using DLT pipeline."""
    logger = get_dagster_logger()
    
    # Tạo pipeline mới trong op này
    pipeline_name = f"nhanh_bills_pipeline_{date.replace('-', '')}"
    db_path = context.resources.database.get_db_path(date)
    
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name=f"bills_{date.replace('-', '')}"
    )
    
    # Create DLT resource
    bills_resource = bills_dlt_resource(bills_data)
    
    try:
        # Run the pipeline
        load_info = pipeline.run(
            [bills_resource],
            write_disposition="replace"
        )
        
        result_msg = f"Bills loaded successfully: {len(bills_data)} records"
        logger.info(result_msg)
        logger.debug(f"Load info: {load_info}")
        
        return result_msg
    except Exception as e:
        error_msg = f"Failed to load bills data: {str(e)}"
        logger.error(error_msg)
        raise
