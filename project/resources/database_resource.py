import dlt
from pathlib import Path
from dagster import resource, get_dagster_logger, Field, StringSource

@resource(
    config_schema={
        "path": Field(StringSource),
        "name_template": Field(StringSource),
        "driver": Field(StringSource, default_value="duckdb")
    }
)
def database_resource(context) -> "DatabaseResource":
    """Resource for database operations."""
    return DatabaseResource(
        path=context.resource_config["path"],
        name_template=context.resource_config["name_template"], 
        driver=context.resource_config["driver"],
        logger=get_dagster_logger()
    )

class DatabaseResource:
    def __init__(self, path: str, name_template: str, driver: str, logger):
        self.path = Path(path)
        self.name_template = name_template
        self.driver = driver
        self.logger = logger
        
        # Ensure data directory exists
        self.path.mkdir(parents=True, exist_ok=True)

    def get_db_path(self, date: str) -> Path:
        """Get database path for given date."""
        db_name = self.name_template.format(date=date.replace('-', ''))
        return self.path / db_name

    def create_pipeline(self, pipeline_name: str, date: str):
        """Create DLT pipeline for given date."""
        db_path = self.get_db_path(date)
        self.logger.info(f"Creating pipeline with DB path: {db_path}")
        
        # Tạo pipeline với working directory
        pipeline = dlt.pipeline(
            pipeline_name=pipeline_name,
            destination=dlt.destinations.duckdb(str(db_path)),
            dataset_name=f"nhanh_data_{date.replace('-', '')}"
        )
        
        return pipeline

