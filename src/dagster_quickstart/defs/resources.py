"""
Resources for serverless Dagster+ deployment
"""

import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
import pandas as pd

import dagster as dg
from dagster import (
    ConfigurableResource, InitResourceContext, resource, 
    IOManager, InputContext, OutputContext, AssetKey,
    ConfigurableIOManager, EnvVar
)
from pydantic import Field
from dagster_duckdb import DuckDBResource


class S3IOManager(ConfigurableResource):
    """Mock S3 I/O Manager for serverless"""
    bucket_name: str = "my-dagster-bucket"
    prefix: str = "dagster-data"
    
    def get_path(self, asset_key: str, partition_key: str = None) -> str:
        """Generate S3 path for asset"""
        path = f"s3://{self.bucket_name}/{self.prefix}/{asset_key}"
        if partition_key:
            path += f"/partition={partition_key}"
        return path
    
    def load_input(self, context) -> Any:
        """Simulate loading from S3"""
        path = self.get_path(str(context.asset_key))
        print(f"📥 Loading data from {path}")
        # In real implementation, would use boto3 to load from S3
        return {"data": f"loaded_from_{context.asset_key}", "source": "s3"}
    
    def handle_output(self, context, obj) -> None:
        """Simulate saving to S3"""
        partition_key = context.partition_key if hasattr(context, 'partition_key') else None
        path = self.get_path(str(context.asset_key), partition_key)
        print(f"💾 Saving data to {path}")
        # In real implementation, would use boto3 to save to S3


class DatabaseResource(ConfigurableResource):
    """Mock database connection for serverless"""
    connection_string: str = "postgresql://localhost:5432/dagster"
    
    def query(self, sql: str) -> Dict[str, Any]:
        """Simulate database query"""
        print(f"🗄️  Executing query: {sql[:50]}...")
        return {
            "rows": [{"id": 1, "name": "sample"}, {"id": 2, "name": "data"}],
            "count": 2
        }
    
    def execute(self, sql: str) -> bool:
        """Simulate database execution"""
        print(f"✏️  Executing: {sql[:50]}...")
        return True


class APIClient(ConfigurableResource):
    """Mock API client for external data sources"""
    base_url: str = "https://api.example.com"
    api_key: str = "mock-api-key"
    
    def fetch_data(self, endpoint: str) -> Dict[str, Any]:
        """Simulate API data fetch"""
        print(f"🌐 Fetching from {self.base_url}/{endpoint}")
        return {
            "data": [
                {"id": 1, "value": "api_data_1", "timestamp": "2024-01-01T00:00:00Z"},
                {"id": 2, "value": "api_data_2", "timestamp": "2024-01-01T01:00:00Z"}
            ],
            "status": "success"
        }


class SlackResource(ConfigurableResource):
    """Mock Slack notification resource"""
    webhook_url: str = "https://hooks.slack.com/services/mock/webhook"
    channel: str = "#data-alerts"
    
    def send_message(self, message: str, channel: str = None) -> bool:
        """Simulate sending Slack message"""
        target_channel = channel or self.channel
        print(f"💬 Slack notification to {target_channel}: {message}")
        return True
    
    def send_alert(self, title: str, details: str) -> bool:
        """Send formatted alert message"""
        message = f"🚨 *{title}*\n{details}"
        return self.send_message(message)


# Original DuckDB resource
database_resource = DuckDBResource(
    database=dg.EnvVar("DUCKDB_DATABASE_URL") 
)

# Resource instances for use in definitions
s3_io_manager = S3IOManager(
    bucket_name=os.getenv("DAGSTER_S3_BUCKET", "dagster-serverless-bucket"),
    prefix="serverless-data"
)

database = DatabaseResource(
    connection_string=os.getenv("DATABASE_URL", "postgresql://localhost:5432/dagster")
)

api_client = APIClient(
    base_url=os.getenv("API_BASE_URL", "https://jsonplaceholder.typicode.com"),
    api_key=os.getenv("API_KEY", "demo-key")
)

slack = SlackResource(
    webhook_url=os.getenv("SLACK_WEBHOOK_URL", "https://mock.slack.webhook"),
    channel=os.getenv("SLACK_CHANNEL", "#dagster-alerts")
)

# ============================================================================
# ADVANCED I/O MANAGERS FOR SERVERLESS
# ============================================================================

class ServerlessS3IOManager(ConfigurableIOManager):
    """Advanced S3 I/O Manager with partitioning and type-aware serialization"""
    bucket_name: str = Field(description="S3 bucket name")
    prefix: str = Field(default="dagster-data", description="S3 key prefix")
    region: str = Field(default="us-east-1", description="AWS region")
    
    def _get_s3_key(self, context: OutputContext) -> str:
        """Generate S3 key based on asset key and partition"""
        key_parts = [self.prefix] + list(context.asset_key.path)
        
        if context.has_partition_key:
            key_parts.append(f"partition_key={context.partition_key}")
            
        # Add format based on output type
        if hasattr(context.dagster_type, 'typing_type'):
            if 'DataFrame' in str(context.dagster_type.typing_type):
                key_parts.append("data.parquet")
            elif 'Dict' in str(context.dagster_type.typing_type):
                key_parts.append("data.json")
            else:
                key_parts.append("data.pkl")
        else:
            key_parts.append("data.json")
            
        return "/".join(key_parts)
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Save object to S3 with appropriate serialization"""
        s3_key = self._get_s3_key(context)
        s3_uri = f"s3://{self.bucket_name}/{s3_key}"
        
        context.log.info(f"💾 Saving asset {context.asset_key} to {s3_uri}")
        
        # Simulate serialization and upload
        if isinstance(obj, pd.DataFrame):
            context.log.info(f"📊 Serializing DataFrame with {len(obj)} rows to Parquet")
            # In real implementation: obj.to_parquet(s3_uri)
        elif isinstance(obj, dict):
            context.log.info(f"📋 Serializing dict with {len(obj)} keys to JSON")
            # In real implementation: json.dump(obj, s3_uri)
        else:
            context.log.info(f"🔧 Serializing {type(obj)} to pickle")
            # In real implementation: pickle.dump(obj, s3_uri)
        
        context.add_output_metadata({
            "s3_uri": s3_uri,
            "s3_key": s3_key,
            "object_type": str(type(obj)),
            "partition_key": context.partition_key if context.has_partition_key else None
        })
    
    def load_input(self, context: InputContext) -> Any:
        """Load object from S3 with appropriate deserialization"""
        s3_key = self._get_s3_key(context)  # type: ignore
        s3_uri = f"s3://{self.bucket_name}/{s3_key}"
        
        context.log.info(f"📥 Loading asset {context.asset_key} from {s3_uri}")
        
        # Simulate deserialization based on file extension
        if s3_key.endswith(".parquet"):
            context.log.info("📊 Deserializing Parquet to DataFrame")
            # In real implementation: return pd.read_parquet(s3_uri)
            return pd.DataFrame({"mock": ["data"], "loaded_from": [s3_uri]})
        elif s3_key.endswith(".json"):
            context.log.info("📋 Deserializing JSON to dict")
            # In real implementation: return json.load(s3_uri)
            return {"mock": "data", "loaded_from": s3_uri}
        else:
            context.log.info("🔧 Deserializing pickle")
            # In real implementation: return pickle.load(s3_uri)
            return {"mock": "data", "loaded_from": s3_uri}


class CloudDataWarehouseIOManager(ConfigurableIOManager):
    """I/O Manager for cloud data warehouses (Snowflake, BigQuery, etc.)"""
    connection_string: str = Field(description="Database connection string")
    schema_name: str = Field(default="dagster", description="Schema for asset tables")
    table_prefix: str = Field(default="", description="Prefix for table names")
    
    def _get_table_name(self, asset_key: AssetKey) -> str:
        """Generate table name from asset key"""
        table_name = "_".join(asset_key.path)
        if self.table_prefix:
            table_name = f"{self.table_prefix}_{table_name}"
        return table_name
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Save object to data warehouse table"""
        table_name = self._get_table_name(context.asset_key)
        full_table_name = f"{self.schema_name}.{table_name}"
        
        context.log.info(f"🏛️  Saving asset {context.asset_key} to table {full_table_name}")
        
        if isinstance(obj, pd.DataFrame):
            context.log.info(f"📊 Writing {len(obj)} rows to {full_table_name}")
            # In real implementation: obj.to_sql(table_name, connection, schema=schema_name, if_exists='replace')
        else:
            context.log.info(f"⚠️  Warning: Non-DataFrame object saved as JSON column")
            # Convert to DataFrame for storage
            df = pd.DataFrame([{"data": json.dumps(obj), "created_at": datetime.now().isoformat()}])
            # In real implementation: df.to_sql(table_name, connection, schema=schema_name, if_exists='replace')
        
        context.add_output_metadata({
            "table_name": full_table_name,
            "row_count": len(obj) if isinstance(obj, pd.DataFrame) else 1,
            "schema": self.schema_name,
            "partition_key": context.partition_key if context.has_partition_key else None
        })
    
    def load_input(self, context: InputContext) -> Any:
        """Load object from data warehouse table"""
        table_name = self._get_table_name(context.asset_key)
        full_table_name = f"{self.schema_name}.{table_name}"
        
        context.log.info(f"📥 Loading asset {context.asset_key} from table {full_table_name}")
        
        # Simulate query
        query = f"SELECT * FROM {full_table_name}"
        if context.has_partition_key:
            query += f" WHERE partition_key = '{context.partition_key}'"
        
        context.log.info(f"🔍 Executing query: {query}")
        
        # In real implementation: return pd.read_sql(query, connection)
        return pd.DataFrame({
            "mock": ["data", "from", "warehouse"],
            "table": [full_table_name] * 3,
            "loaded_at": [datetime.now().isoformat()] * 3
        })


class RedisIOManager(ConfigurableIOManager):
    """I/O Manager for Redis caching layer"""
    redis_url: str = Field(description="Redis connection URL")
    ttl_seconds: int = Field(default=3600, description="Time to live for cached objects")
    key_prefix: str = Field(default="dagster:", description="Prefix for Redis keys")
    
    def _get_redis_key(self, context) -> str:
        """Generate Redis key from asset key"""
        key_parts = [self.key_prefix] + list(context.asset_key.path)
        if hasattr(context, 'partition_key') and context.partition_key:
            key_parts.append(f"partition:{context.partition_key}")
        return ":".join(key_parts)
    
    def handle_output(self, context: OutputContext, obj: Any) -> None:
        """Cache object in Redis"""
        redis_key = self._get_redis_key(context)
        
        context.log.info(f"🔴 Caching asset {context.asset_key} in Redis with key {redis_key}")
        
        # Serialize object
        if isinstance(obj, pd.DataFrame):
            serialized_data = obj.to_json(orient='records')
            object_type = "dataframe"
        elif isinstance(obj, dict):
            serialized_data = json.dumps(obj)
            object_type = "dict"
        else:
            serialized_data = json.dumps(str(obj))
            object_type = "string"
        
        # In real implementation:
        # redis_client = redis.from_url(self.redis_url)
        # redis_client.setex(redis_key, self.ttl_seconds, serialized_data)
        # redis_client.setex(f"{redis_key}:type", self.ttl_seconds, object_type)
        
        context.add_output_metadata({
            "redis_key": redis_key,
            "object_type": object_type,
            "ttl_seconds": self.ttl_seconds,
            "cache_size_bytes": len(serialized_data)
        })
    
    def load_input(self, context: InputContext) -> Any:
        """Load cached object from Redis"""
        redis_key = self._get_redis_key(context)
        
        context.log.info(f"📥 Loading cached asset {context.asset_key} from Redis key {redis_key}")
        
        # In real implementation:
        # redis_client = redis.from_url(self.redis_url)
        # serialized_data = redis_client.get(redis_key)
        # object_type = redis_client.get(f"{redis_key}:type")
        
        # Simulate cache hit/miss
        import random
        if random.random() > 0.1:  # 90% cache hit rate
            context.log.info("✅ Cache hit")
            # Simulate cached data
            return {"cached_data": f"from_redis_{context.asset_key}", "cache_key": redis_key}
        else:
            context.log.info("❌ Cache miss")
            raise Exception(f"Cache miss for key {redis_key}")


# ============================================================================
# ADVANCED RESOURCE CONFIGURATIONS
# ============================================================================

class AdvancedAPIClient(ConfigurableResource):
    """Enhanced API client with retry logic, rate limiting, and auth"""
    base_url: str = Field(description="API base URL")
    api_key: str = Field(description="API authentication key")
    timeout_seconds: int = Field(default=30, description="Request timeout in seconds")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    rate_limit_per_second: int = Field(default=10, description="Max requests per second")
    
    def fetch_data(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Fetch data with retry logic and rate limiting"""
        full_url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        print(f"🌐 Fetching from {full_url} (timeout: {self.timeout_seconds}s, retries: {self.max_retries})")
        
        # Simulate rate limiting
        print(f"⏱️  Rate limiting: {self.rate_limit_per_second} req/sec")
        
        # Simulate retry logic
        for attempt in range(1, self.max_retries + 1):
            try:
                print(f"🔄 Attempt {attempt}/{self.max_retries}")
                
                # Simulate API call
                mock_data = {
                    "data": [
                        {"id": i, "value": f"api_data_{i}", "timestamp": datetime.now().isoformat(), "endpoint": endpoint}
                        for i in range(1, 6)
                    ],
                    "status": "success",
                    "request_id": f"req_{datetime.now().timestamp()}",
                    "attempt": attempt
                }
                
                if params:
                    mock_data["request_params"] = params
                
                print("✅ API request successful")
                return mock_data
                
            except Exception as e:
                print(f"❌ Attempt {attempt} failed: {str(e)}")
                if attempt == self.max_retries:
                    raise Exception(f"API request failed after {self.max_retries} attempts")
                print(f"⏳ Retrying in {2 ** attempt} seconds...")
    
    def fetch_paginated(self, endpoint: str, page_size: int = 100) -> List[Dict[str, Any]]:
        """Fetch paginated data automatically"""
        all_data = []
        page = 1
        
        while True:
            print(f"📄 Fetching page {page} (size: {page_size})")
            
            params = {"page": page, "page_size": page_size}
            response = self.fetch_data(endpoint, params)
            
            page_data = response.get("data", [])
            all_data.extend(page_data)
            
            # Simulate pagination logic
            if len(page_data) < page_size or page >= 3:  # Max 3 pages for demo
                break
                
            page += 1
        
        print(f"📚 Fetched {len(all_data)} total records across {page} pages")
        return all_data


class EnhancedSlackResource(ConfigurableResource):
    """Enhanced Slack resource with templating and different message types"""
    webhook_url: str = Field(description="Slack webhook URL")
    default_channel: str = Field(default="#data-alerts", description="Default channel")
    username: str = Field(default="Dagster Bot", description="Bot username")
    enable_threading: bool = Field(default=True, description="Enable message threading")
    
    def send_message(self, message: str, channel: Optional[str] = None, thread_ts: Optional[str] = None) -> Dict[str, Any]:
        """Send message with enhanced options"""
        target_channel = channel or self.default_channel
        
        payload = {
            "channel": target_channel,
            "username": self.username,
            "text": message,
            "timestamp": datetime.now().isoformat()
        }
        
        if thread_ts and self.enable_threading:
            payload["thread_ts"] = thread_ts
        
        print(f"💬 Slack message to {target_channel}: {message[:100]}...")
        return payload
    
    def send_alert(self, title: str, details: str, severity: str = "warning", channel: Optional[str] = None) -> Dict[str, Any]:
        """Send formatted alert with severity levels"""
        emoji_map = {
            "info": "ℹ️",
            "warning": "⚠️",
            "error": "🚨",
            "success": "✅"
        }
        
        emoji = emoji_map.get(severity, "📢")
        color_map = {
            "info": "#36a64f",
            "warning": "#ff9500", 
            "error": "#ff0000",
            "success": "#36a64f"
        }
        
        message = f"{emoji} *{title}*\n{details}"
        
        return self.send_message(message, channel)
    
    def send_asset_materialization_summary(self, asset_key: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Send formatted asset materialization notification"""
        message = f"📊 *Asset Materialized: {asset_key}*\n"
        
        for key, value in metadata.items():
            if isinstance(value, (int, float)):
                if key.endswith('_count') or key.endswith('_records'):
                    message += f"• {key.replace('_', ' ').title()}: {value:,}\n"
                else:
                    message += f"• {key.replace('_', ' ').title()}: {value}\n"
            elif isinstance(value, str):
                message += f"• {key.replace('_', ' ').title()}: {value}\n"
        
        return self.send_message(message)


# Enhanced resource instances
serverless_s3_io_manager = ServerlessS3IOManager(
    bucket_name=os.getenv("DAGSTER_S3_BUCKET", "dagster-serverless-bucket"),
    prefix=os.getenv("S3_PREFIX", "serverless-data"),
    region=os.getenv("AWS_REGION", "us-east-1")
)

warehouse_io_manager = CloudDataWarehouseIOManager(
    connection_string=os.getenv("WAREHOUSE_CONNECTION_STRING", "postgresql://localhost:5432/warehouse"),
    schema_name=os.getenv("WAREHOUSE_SCHEMA", "dagster_assets"),
    table_prefix=os.getenv("TABLE_PREFIX", "serverless")
)

redis_cache_manager = RedisIOManager(
    redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    ttl_seconds=int(os.getenv("CACHE_TTL_SECONDS", "3600")),
    key_prefix=os.getenv("REDIS_KEY_PREFIX", "dagster:")
)

advanced_api_client = AdvancedAPIClient(
    base_url=os.getenv("API_BASE_URL", "https://jsonplaceholder.typicode.com"),
    api_key=os.getenv("API_KEY", "demo-key"),
    timeout_seconds=int(os.getenv("API_TIMEOUT_SECONDS", "30")),
    max_retries=int(os.getenv("API_MAX_RETRIES", "3")),
    rate_limit_per_second=int(os.getenv("API_RATE_LIMIT", "10"))
)

enhanced_slack = EnhancedSlackResource(
    webhook_url=os.getenv("SLACK_WEBHOOK_URL", "https://mock.slack.webhook"),
    default_channel=os.getenv("SLACK_CHANNEL", "#dagster-alerts"),
    username=os.getenv("SLACK_USERNAME", "Dagster Serverless Bot"),
    enable_threading=os.getenv("SLACK_ENABLE_THREADING", "true").lower() == "true"
)

@dg.definitions  
def resources():
    return dg.Definitions(
        resources={
            # Original resources
            "duckdb": database_resource,
            "s3_io_manager": s3_io_manager,
            "database": database,
            "api_client": api_client,
            "slack": slack,
            
            # Enhanced serverless resources
            "serverless_s3_io_manager": serverless_s3_io_manager,
            "warehouse_io_manager": warehouse_io_manager,
            "redis_cache_manager": redis_cache_manager,
            "advanced_api_client": advanced_api_client,
            "enhanced_slack": enhanced_slack,
        }
    )
