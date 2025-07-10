from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from google.cloud import bigquery
from typing import Optional, List, Dict, Any
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime

# Find and load .env file from parent directories
current_dir = Path(__file__).parent
for i in range(4):  # Look up to 4 directories up
    env_path = current_dir / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        print(f"Loaded environment variables from: {env_path}")
        break
    current_dir = current_dir.parent
else:
    load_dotenv()  # Fallback to default behavior

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="MCP BigQuery Server",
    description="A FastAPI server with MCP protocol for BigQuery integration",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models
class QueryRequest(BaseModel):
    query: str
    dry_run: Optional[bool] = False

class NaturalLanguageRequest(BaseModel):
    query: str
    context: Optional[str] = None

class MCPToolRequest(BaseModel):
    name: str
    arguments: Dict[str, Any]

class QueryResponse(BaseModel):
    success: bool
    rows: Optional[List[Dict]] = None
    metadata: Optional[Dict] = None
    error: Optional[str] = None

# BigQuery client initialization
def get_bigquery_client():
    project_id = os.getenv("GCP_PROJECT_ID")
    if not project_id:
        raise ValueError("GCP_PROJECT_ID environment variable not set")
    
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if credentials_path and os.path.exists(credentials_path):
        logger.info(f"Using credentials from: {credentials_path}")
        return bigquery.Client(project=project_id)
    elif credentials_path:
        logger.info(f"Credentials path specified but not found: {credentials_path}")
        logger.info("Attempting to use default credentials...")
    
    return bigquery.Client(project=project_id)

# Schema information for AI agents
SCHEMA_INFO = {
    "ga_sessions": {
        "description": "Google Analytics sessions data",
        "table_pattern": "bigquery-public-data.google_analytics_sample.ga_sessions_*",
        "columns": {
            "visitId": "Unique visit identifier",
            "visitNumber": "Session number for this visitor",
            "visitStartTime": "Timestamp of when session started",
            "date": "Date of session (YYYYMMDD format)",
            "trafficSource": {
                "source": "Traffic source (google, direct, etc.)",
                "medium": "Traffic medium (organic, cpc, etc.)",
                "campaign": "Campaign name"
            },
            "device": {
                "operatingSystem": "Operating system",
                "browser": "Browser used",
                "deviceCategory": "Device category (desktop, mobile, tablet)"
            },
            "geoNetwork": {
                "country": "Country",
                "region": "Region/state", 
                "city": "City"
            },
            "totals": {
                "visits": "Number of visits",
                "hits": "Number of hits",
                "pageviews": "Number of pageviews",
                "bounces": "Number of bounces"
            }
        }
    }
}

# Health check endpoint
@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "service": "MCP BigQuery Server",
        "version": "1.0.0"
    }

# Original query endpoint
@app.post("/query", response_model=QueryResponse)
def run_query(req: QueryRequest):
    try:
        # Validate query for safety
        forbidden_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'CREATE', 'ALTER', 'TRUNCATE']
        if any(keyword in req.query.upper() for keyword in forbidden_keywords):
            raise HTTPException(status_code=400, detail="Query contains forbidden operations")
        
        client = get_bigquery_client()
        
        if req.dry_run:
            job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
            query_job = client.query(req.query, job_config=job_config)
            return QueryResponse(
                success=True,
                metadata={
                    "valid": True,
                    "bytes_processed": query_job.total_bytes_processed,
                    "query": req.query
                }
            )
        else:
            query_job = client.query(req.query)
            results = query_job.result()
            
            rows = [dict(row) for row in results]
            
            return QueryResponse(
                success=True,
                rows=rows,
                metadata={
                    "row_count": len(rows),
                    "total_bytes_processed": query_job.total_bytes_processed,
                    "job_id": query_job.job_id,
                    "query": req.query
                }
            )
            
    except Exception as e:
        logger.error(f"Query execution error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Natural language to SQL endpoint
@app.post("/generate-sql")
def generate_sql_endpoint(req: NaturalLanguageRequest):
    try:
        result = generate_sql_from_natural_language(req.query, req.context or "")
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

def generate_sql_from_natural_language(query: str, context: str = "") -> Dict[str, Any]:
    """Generate SQL from natural language using pattern matching"""
    
    lower_query = query.lower()
    
    # Pattern matching for common queries
    patterns = [
        {
            "pattern": ["top", "operating system"],
            "sql": """
                SELECT 
                    device.operatingSystem as operating_system,
                    COUNT(*) as visits
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE device.operatingSystem IS NOT NULL
                GROUP BY device.operatingSystem
                ORDER BY visits DESC
                LIMIT 5
            """,
            "confidence": 0.9
        },
        {
            "pattern": ["sessions", "month"],
            "sql": """
                SELECT 
                    FORMAT_DATE('%Y-%m', PARSE_DATE('%Y%m%d', date)) as month,
                    COUNT(*) as sessions
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                GROUP BY month
                ORDER BY month
            """,
            "confidence": 0.9
        },
        {
            "pattern": ["bounce rate", "traffic source"],
            "sql": """
                SELECT 
                    trafficSource.source as traffic_source,
                    COUNT(*) as total_sessions,
                    SUM(totals.bounces) as total_bounces,
                    ROUND(SAFE_DIVIDE(SUM(totals.bounces), COUNT(*)) * 100, 2) as bounce_rate_percent
                FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
                WHERE trafficSource.source IS NOT NULL
                GROUP BY trafficSource.source
                ORDER BY total_sessions DESC
                LIMIT 10
            """,
            "confidence": 0.9
        }
    ]
    
    # Find best matching pattern
    for pattern_info in patterns:
        if all(keyword in lower_query for keyword in pattern_info["pattern"]):
            return {
                "sql": pattern_info["sql"].strip(),
                "explanation": f"Generated SQL for: '{query}'",
                "confidence": pattern_info["confidence"],
                "pattern_matched": pattern_info["pattern"]
            }
    
    # Fallback query
    fallback_sql = """
        SELECT 
            COUNT(*) as total_sessions,
            COUNT(DISTINCT visitId) as unique_visitors
        FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    """
    
    return {
        "sql": fallback_sql.strip(),
        "explanation": f"No specific pattern matched for '{query}'. Returning basic statistics.",
        "confidence": 0.3,
        "pattern_matched": "fallback"
    }
    
@app.get("/discover-schema")
def discover_schema():
    """Discover all datasets and tables in the BigQuery project"""
    try:
        client = get_bigquery_client()
        project_id = os.getenv("GCP_PROJECT_ID")
        
        schema_info = {
            "project_id": project_id,
            "datasets": {},
            "discovery_time": datetime.now().isoformat()
        }
        
        # Get all datasets
        datasets = list(client.list_datasets())
        
        for dataset_ref in datasets:
            dataset_id = dataset_ref.dataset_id
            
            try:
                # Get full dataset object for description
                dataset = client.get_dataset(dataset_ref.reference)
                
                schema_info["datasets"][dataset_id] = {
                    "description": getattr(dataset, 'description', None) or "No description",
                    "tables": {}
                }
                
                # Get all tables in this dataset
                tables = list(client.list_tables(dataset_ref.reference))
                
                for table_ref in tables:
                    try:
                        # Get full table object
                        table = client.get_table(table_ref.reference)
                        
                        # Get column information
                        columns = {}
                        for field in table.schema:
                            columns[field.name] = {
                                "type": field.field_type,
                                "mode": getattr(field, 'mode', None) or "NULLABLE",
                                "description": getattr(field, 'description', None) or "No description"
                            }
                        
                        schema_info["datasets"][dataset_id]["tables"][table_ref.table_id] = {
                            "description": getattr(table, 'description', None) or "No description",
                            "num_rows": getattr(table, 'num_rows', None) or 0,
                            "columns": columns,
                            "full_table_id": f"{project_id}.{dataset_id}.{table_ref.table_id}"
                        }
                        
                    except Exception as table_error:
                        logger.warning(f"Could not access table {table_ref.table_id}: {table_error}")
                        schema_info["datasets"][dataset_id]["tables"][table_ref.table_id] = {
                            "error": f"Access denied: {str(table_error)}"
                        }
                        
            except Exception as dataset_error:
                logger.warning(f"Could not access dataset {dataset_id}: {dataset_error}")
                schema_info["datasets"][dataset_id] = {
                    "error": f"Access denied: {str(dataset_error)}"
                }
        
        return schema_info
        
    except Exception as e:
        logger.error(f"Schema discovery error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Schema discovery failed: {str(e)}")
@app.post("/ai-tool")
def ai_tool_endpoint(request: dict):
    """Universal AI tool endpoint that handles both schema discovery and queries"""
    action = request.get("action", "")

    if action == "discover-schema":
        schema_info = discover_schema()
        return {
            "answer": "Here's what I found in your BigQuery project.",
            "data": schema_info["datasets"] if isinstance(schema_info, dict) else schema_info
        }

    elif action == "query":
        sql_query = request.get("sql_query", "") or request.get("query", "")  # Support both keys
        if not sql_query:
            raise HTTPException(status_code=400, detail="SQL query required for query action")
        
        # Run the query
        query_req = QueryRequest(query=sql_query)
        response = run_query(query_req)

        return {
            "answer": "Here's your query result." if response.success else "There was an issue with your query.",
            "data": response.rows if response.success else [],
        }

    else:
        raise HTTPException(status_code=400, detail="Action must be 'discover-schema' or 'query'")

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("BIGQUERY_SERVER_PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)