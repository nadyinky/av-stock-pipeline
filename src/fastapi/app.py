from typing import Dict, Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from airflow.api.common.experimental.trigger_dag import trigger_dag

app = FastAPI()


class IngestionRequest(BaseModel):
    dag_id: str
    execution_date: str
    args: Dict[str, Any]


@app.post("/trigger-ingestion/")
async def trigger_ingestion(request: IngestionRequest):
    try:
        trigger_dag(
            dag_id=request.dag_id,
            run_id=f"manual_run_{request.execution_date}",
            conf=request.args,
            execution_date=request.execution_date,
        )
        return {"message": "Ingestion triggered successfully!"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error triggering ingestion: {str(e)}")