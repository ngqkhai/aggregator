import os
import logging
import asyncio
from fastapi import FastAPI
from dotenv import load_dotenv
from app.message_broker import AggregatorMessageBroker

# Load environment variables
oad = load_dotenv()
# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

app = FastAPI(
    title="Aggregator Service",
    description="Orchestrator that waits for voice+image results, enriches with script, and emits script.ready events",
    version="1.0.0"
)

broker = AggregatorMessageBroker()

@app.on_event("startup")
async def startup_event():
    logging.getLogger("aggregator.startup").info("🚀 Aggregator startup")
    await broker.connect()
    asyncio.create_task(broker.start())

@app.on_event("shutdown")
async def shutdown_event():
    await broker.close()

@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8003))
    uvicorn.run(app, host="0.0.0.0", port=port) 