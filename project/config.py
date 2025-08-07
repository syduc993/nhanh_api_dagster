# project/config.py (file mới)
import os

CONFIG = {
    "api": {
        "app_id": os.getenv("NHANH_APP_ID", "74951"),
        "business_id": os.getenv("NHANH_BUSINESS_ID", "8901"),
        "access_token": os.getenv("NHANH_ACCESS_TOKEN", 
            "twf9P1xFZCUUgwt8zR0XgNeB6V5jsbq2KHb14bxovqK1ppCxyADwOK8FzQlCEeEGABRZINXoUCSzM50kjhwcrUSBWTY5nSvyhfnH2X2cI0pC7pNczSVxc1ratdDmxF85q7hUTUNCrUnpPTG5ZwLNO7bkMlEEJTCdPhgYaC"),
        "base_url": "https://pos.open.nhanh.vn/api",
        "version": "2.0",
        "timeout": 30,
        "max_retries": 3
    },
    "database": {
        "path": os.getenv("DATABASE_PATH", "data"),
        "name_template": "nhanh_data_{date}.duckdb"
    },
    "data": {
        "default_depot_id": 155286,
        "default_mode_bills": 6,
        "default_mode_imexs": 2,
        "default_icpp": 20,
        "max_pages": 100
    }
}
