from dagster import op, In, Out, get_dagster_logger, Field
import dlt

@op(
    required_resource_keys={"nhanh_api"},
    ins={
        "date": In(str, description="Date to extract imexs for")
    },
    out=Out(list, description="Imexs data extracted from API"),
    config_schema={
        "icpp": Field(int, default_value=20),
        "mode": Field(int, default_value=2)
    },
    tags={"kind": "extract", "source": "imexs"}
)
def extract_imexs_op(context, date: str) -> list:
    """Extract imexs data from Nhanh.vn API."""
    logger = get_dagster_logger()
    api = context.resources.nhanh_api
    
    icpp = context.op_config["icpp"]
    mode = context.op_config["mode"]
    
    data_dict = {
        "icpp": icpp,
        "mode": mode,
        "fromDate": date,
        "toDate": date
    }
    
    logger.info(f"Extracting imexs for date {date} with params: {data_dict}")
    
    imexs_data = api.extract_data(
        endpoint="bill/imexs",
        data_dict=data_dict,
        data_key="imexs"
    )
    
    logger.info(f"Successfully extracted {len(imexs_data)} imexs records")
    return imexs_data

@dlt.resource
def imexs_dlt_resource(imexs_data: list):
    """DLT resource for imexs data."""
    if imexs_data:
        yield imexs_data
    else:
        # Yield empty list to avoid DLT errors
        yield []

@op(
    required_resource_keys={"database"},  # Thêm database resource
    ins={
        "imexs_data": In(list, description="Imexs data to load"),
        "date": In(str, description="Date for pipeline setup")  # Đổi từ pipeline object sang date
    },
    out=Out(str, description="Load result message"),
    tags={"kind": "load", "destination": "duckdb"}
)
def load_imexs_op(context, imexs_data: list, date: str) -> str:
    """Load imexs data using DLT pipeline."""
    logger = get_dagster_logger()
    
    # Tạo pipeline mới trong op này
    pipeline_name = f"nhanh_imexs_pipeline_{date.replace('-', '')}"
    db_path = context.resources.database.get_db_path(date)
    
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination=dlt.destinations.duckdb(str(db_path)),
        dataset_name=f"imexs_{date.replace('-', '')}"
    )
    
    # Create DLT resource
    imexs_resource = imexs_dlt_resource(imexs_data)
    
    try:
        # Run the pipeline
        load_info = pipeline.run(
            [imexs_resource],
            write_disposition="replace"
        )
        
        result_msg = f"Imexs loaded successfully: {len(imexs_data)} records"
        logger.info(result_msg)
        logger.debug(f"Load info: {load_info}")
        
        return result_msg
    except Exception as e:
        error_msg = f"Failed to load imexs data: {str(e)}"
        logger.error(error_msg)
        raise
