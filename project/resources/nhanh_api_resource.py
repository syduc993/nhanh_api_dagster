import requests
import json
from typing import Dict, Any, Generator
from dagster import resource, get_dagster_logger, Field, StringSource

@resource(
    config_schema={
        "app_id": Field(StringSource),
        "business_id": Field(StringSource), 
        "access_token": Field(StringSource),
        "base_url": Field(StringSource),
        "version": Field(StringSource, default_value="2.0"),
        "timeout": Field(int, default_value=30),
        "max_retries": Field(int, default_value=3)
    }
)
def nhanh_api_resource(context) -> "NhanhAPIResource":
    """Resource for interacting with Nhanh.vn API."""
    return NhanhAPIResource(
        app_id=context.resource_config["app_id"],
        business_id=context.resource_config["business_id"],
        access_token=context.resource_config["access_token"],
        base_url=context.resource_config["base_url"],
        version=context.resource_config["version"],
        timeout=context.resource_config["timeout"],
        max_retries=context.resource_config["max_retries"],
        logger=get_dagster_logger()
    )

class NhanhAPIResource:
    def __init__(self, app_id: str, business_id: str, access_token: str, 
                 base_url: str, version: str, timeout: int, max_retries: int, logger):
        self.app_id = app_id
        self.business_id = business_id
        self.access_token = access_token
        self.base_url = base_url
        self.version = version
        self.timeout = timeout
        self.max_retries = max_retries
        self.logger = logger

    def build_payload(self, data_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Build payload for Nhanh.vn API."""
        return {
            "version": self.version,
            "appId": self.app_id,
            "businessId": self.business_id,
            "accessToken": self.access_token,
            "data": json.dumps(data_dict)
        }

    def call_api_paginated(self, endpoint: str, data_dict: Dict[str, Any], 
                          page_key: str = "page", max_pages: int = 100) -> Generator[Dict[str, Any], None, None]:
        """Call API with pagination support."""
        page = 1
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        while page <= max_pages:
            data_dict[page_key] = page
            payload = self.build_payload(data_dict)
            
            try:
                response = requests.post(url, data=payload, timeout=self.timeout)
                response.raise_for_status()
            except requests.RequestException as e:
                self.logger.error(f"HTTP Request failed for page {page}: {str(e)}")
                break
            
            try:
                resp_json = response.json()
            except json.JSONDecodeError:
                self.logger.error(f"Invalid JSON response for page {page}")
                break

            api_code = resp_json.get("code")
            page_data = resp_json.get("data", {})
            
            if api_code != 1:
                self.logger.warning(f"API returned code {api_code} for page {page}")
                if not page_data:
                    break
            
            if page_data:
                yield page_data
            else:
                break

            current_page = page_data.get("page", page_data.get("currentPage", page))
            total_pages = page_data.get("totalPages", page_data.get("totalPage", page))
            
            if current_page >= total_pages:
                break
                
            page += 1

    def extract_data(self, endpoint: str, data_dict: Dict[str, Any], 
                    data_key: str, page_key: str = "page", max_pages: int = 100) -> list:
        """Extract data from paginated API calls."""
        all_data = []
        
        try:
            for page_data in self.call_api_paginated(endpoint, data_dict, page_key, max_pages):
                if data_key in page_data:
                    data_on_page = page_data[data_key]
                    if isinstance(data_on_page, dict):
                        all_data.extend(list(data_on_page.values()))
                    elif isinstance(data_on_page, list):
                        all_data.extend(data_on_page)
                    else:
                        self.logger.warning(f"Unexpected data structure for {data_key}: {type(data_on_page)}")
        except Exception as e:
            self.logger.error(f"Error extracting data from {endpoint}: {str(e)}")
            
        self.logger.info(f"Extracted {len(all_data)} records from {endpoint}")
        return all_data
