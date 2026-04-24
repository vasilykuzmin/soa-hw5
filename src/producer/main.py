import asyncio
import logging
import re
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from google.protobuf.json_format import ParseDict, ParseError
from movie_event_pb2 import MovieEvent
from producer import KafkaProducer
from generator import EventGenerator
from dotenv import load_dotenv
import os

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9093")
TOPIC = os.getenv("KAFKA_TOPIC", "movie-events")
SYNTHETIC_MODE = os.getenv("SYNTHETIC_MODE", "false").lower() == "true"
USERS_COUNT = int(os.getenv("SYNTHETIC_USERS", 5))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

producer = None
generator = None
generator_task = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer, generator, generator_task
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, topic=TOPIC)
    logger.info("Kafka producer initialized")

    if SYNTHETIC_MODE:
        generator = EventGenerator(producer, users_count=USERS_COUNT)
        generator_task = asyncio.create_task(generator.start())
        logger.info("Synthetic event generator started")

    yield

    if generator_task:
        generator.stop()
        await generator_task
    logger.info("Shutdown complete")

app = FastAPI(lifespan=lifespan)

@app.post("/event")
async def accept_event(request: Request):
    try:
        body = await request.json()
        event = ParseDict(body, MovieEvent())
    except ParseError as e:
        raise HTTPException(status_code=400, detail=f"Invalid event schema: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {str(e)}")

    uuid_regex = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
    if not re.match(uuid_regex, event.event_id):
        raise HTTPException(status_code=400, detail="event_id must be a valid UUID")

    key = event.user_id

    try:
        result = await producer.send_event(key=key, event=event)
        return {"status": "ok", "event_id": event.event_id}
    except Exception as e:
        logger.error(f"Failed to send event: {e}")
        raise HTTPException(status_code=500, detail="Kafka producer error")

@app.get("/health")
async def health():
    return {"status": "alive"}