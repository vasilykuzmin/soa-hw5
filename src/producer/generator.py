import asyncio
import random
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional
import logging

from movie_event_pb2 import MovieEvent, EventType, DeviceType
logger = logging.getLogger(__name__)

class EventGenerator:
    def __init__(self, producer, users_count: int = 10, movies_db: List[str] = None):
        self.producer = producer
        self.users_count = users_count
        self.movies_db = movies_db or [f"movie_{i}" for i in range(1, 51)]
        self.active_sessions: Dict[str, Dict] = {}
        self.is_running = False
        self.tasks = []

    async def generate_user_session(self, user_id: str):
        session_id = str(uuid.uuid4())
        movie_id = random.choice(self.movies_db)
        device = random.choice(list(DeviceType.values()))
        total_duration = random.randint(1800, 7200)
        progress = 0

        event = self._create_event(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            movie_id=movie_id,
            event_type=EventType.VIEW_STARTED,
            device=device,
            session_id=session_id,
            progress=0
        )
        await self.producer.send_event(key=user_id, event=event)
        await asyncio.sleep(random.uniform(0.5, 2))

        while progress < total_duration:
            if progress > 0 and random.random() < 0.3:
                pause_event = self._create_event(
                    event_id=str(uuid.uuid4()),
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=EventType.VIEW_PAUSED,
                    device=device,
                    session_id=session_id,
                    progress=progress
                )
                await self.producer.send_event(key=user_id, event=pause_event)
                await asyncio.sleep(random.uniform(1, 5))

                resume_event = self._create_event(
                    event_id=str(uuid.uuid4()),
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=EventType.VIEW_RESUMED,
                    device=device,
                    session_id=session_id,
                    progress=progress
                )
                await self.producer.send_event(key=user_id, event=resume_event)
                await asyncio.sleep(random.uniform(0.5, 2))

            step = random.randint(30, 300)
            progress = min(progress + step, total_duration)

            if random.random() < 0.2:
                like_event = self._create_event(
                    event_id=str(uuid.uuid4()),
                    user_id=user_id,
                    movie_id=movie_id,
                    event_type=EventType.LIKED,
                    device=device,
                    session_id=session_id,
                    progress=progress
                )
                await self.producer.send_event(key=user_id, event=like_event)
                await asyncio.sleep(0.2)

            await asyncio.sleep(random.uniform(0.2, 1.0))

        finish_event = self._create_event(
            event_id=str(uuid.uuid4()),
            user_id=user_id,
            movie_id=movie_id,
            event_type=EventType.VIEW_FINISHED,
            device=device,
            session_id=session_id,
            progress=total_duration
        )
        await self.producer.send_event(key=user_id, event=finish_event)

        if random.random() < 0.5:
            search_event = self._create_event(
                event_id=str(uuid.uuid4()),
                user_id=user_id,
                movie_id="",
                event_type=EventType.SEARCHED,
                device=device,
                session_id=str(uuid.uuid4()),
                progress=0
            )
            await self.producer.send_event(key=user_id, event=search_event)

    def _create_event(self, event_id, user_id, movie_id, event_type, device, session_id, progress):
        event = MovieEvent()
        event.event_id = event_id
        event.user_id = user_id
        event.movie_id = movie_id
        event.event_type = event_type
        now_utc = datetime.now(timezone.utc)
        event.timestamp = now_utc.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        event.device_type = device
        event.session_id = session_id
        event.progress_seconds = progress
        return event

    async def start(self):
        self.is_running = True
        users = [f"user_{i}" for i in range(1, self.users_count + 1)]
        while self.is_running:
            tasks = [asyncio.create_task(self.generate_user_session(u)) for u in users]
            await asyncio.gather(*tasks)
            await asyncio.sleep(5)

    def stop(self):
        self.is_running = False
