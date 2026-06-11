import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest
import os
import sys

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)


# Assuming the module to test is in 'event_generator'
from src.producer.generator import EventGenerator, EventType, DeviceType
from movie_event_pb2 import MovieEvent, EventType, DeviceType


@pytest.fixture
def mock_producer():
    """Create a mock Kafka producer with an async send_event method."""
    producer = AsyncMock()
    producer.send_event = AsyncMock()
    return producer


@pytest.fixture
def event_generator(mock_producer):
    """Create an EventGenerator instance with controlled parameters."""
    movies = ["movie_1", "movie_2"]
    return EventGenerator(producer=mock_producer, users_count=2, movies_db=movies)


def test_create_event(event_generator):
    """Test that _create_event correctly builds a MovieEvent protobuf message."""
    # Fixed values for deterministic test
    event_id = "evt-123"
    user_id = "user_42"
    movie_id = "movie_99"
    event_type = EventType.VIEW_STARTED
    device = DeviceType.MOBILE
    session_id = "sess-abc"
    progress = 120

    # Freeze time for timestamp comparison
    fixed_now = datetime(2025, 1, 15, 12, 0, 0, 123456, tzinfo=timezone.utc)
    with patch(f"{EventGenerator.__module__}.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.strftime = datetime.strftime  # keep original strftime

        event = event_generator._create_event(
            event_id=event_id,
            user_id=user_id,
            movie_id=movie_id,
            event_type=event_type,
            device=device,
            session_id=session_id,
            progress=progress,
        )

    assert event.event_id == event_id
    assert event.user_id == user_id
    assert event.movie_id == movie_id
    assert event.event_type == event_type
    # Timestamp format: YYYY-MM-DDTHH:MM:SS.fff (3-digit ms)
    expected_timestamp = fixed_now.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    assert event.timestamp == expected_timestamp
    assert event.device_type == device
    assert event.session_id == session_id
    assert event.progress_seconds == progress

@pytest.mark.asyncio
async def test_generate_user_session_basic_flow(event_generator, mock_producer):
    """Test a complete user session without pauses, likes, or search."""
    user_id = "user_1"

    # Override random choices to control behavior:
    # - Choose first movie
    # - Use DESKTOP device
    # - total_duration small (300s)
    # - No pauses (random.random() > 0.3)
    # - No likes (random.random() > 0.2)
    # - No search (random.random() > 0.5)
    with patch("random.choice") as mock_choice, patch(
        "random.randint"
    ) as mock_randint, patch("random.random") as mock_random, patch(
        "asyncio.sleep", new_callable=AsyncMock
    ) as mock_sleep:

        mock_choice.side_effect = [
            "movie_1",  # movie_id
            DeviceType.DESKTOP,  # device
        ]
        mock_randint.return_value = 300  # total_duration
        # random.random() returns >0.3, >0.2, >0.5 respectively
        mock_random.side_effect = [0.5, 0.5, 0.5, 0.6]  # extra calls for loop condition

        # Run the session (should finish quickly because step mocks are not used - but we need to step)
        # Actually generate_user_session uses random.randint(30,300) for step.
        # To finish quickly we can set step to >= total_duration on first iteration.
        # We'll patch randint again inside the loop; easier to patch it to return a large value.
        # But since we already used mock_randint for total_duration, we need to let it return different values.
        # Let's redefine mock_randint to return total_duration for the first call (total_duration),
        # then return a step that equals total_duration for the first step.
        mock_randint.reset_mock()
        mock_randint.side_effect = [300, 300]  # first call total_duration, second call step

        # Also ensure that while loop condition progress < total_duration runs only once
        # Because step >= total_duration will set progress = total_duration
        await event_generator.generate_user_session(user_id)

    # Verify the expected events were sent
    # Order: VIEW_STARTED, VIEW_FINISHED (no pause/resume/like/search)
    assert mock_producer.send_event.call_count == 2

    calls = mock_producer.send_event.call_args_list
    # First call: VIEW_STARTED
    first_event = calls[0][1]["event"]
    assert first_event.user_id == user_id
    assert first_event.movie_id == "movie_1"
    assert first_event.event_type == EventType.VIEW_STARTED
    assert first_event.progress_seconds == 0
    # Second call: VIEW_FINISHED
    second_event = calls[1][1]["event"]
    assert second_event.event_type == EventType.VIEW_FINISHED
    assert second_event.progress_seconds == 300

    # Sleeps should have been called (once for initial delay, once for each step loop)
    # We don't need to assert exact sleeps count, just that they were awaited.

@pytest.mark.asyncio
async def test_start_creates_tasks_and_loops(event_generator):
    """Test the start method runs a continuous loop and can be stopped."""
    # Override generate_user_session to be a no-op async method for this test
    event_generator.generate_user_session = AsyncMock()

    # Run start in a task and stop after a short time
    run_task = asyncio.create_task(event_generator.start())
    await asyncio.sleep(0.1)  # let the loop run a couple of iterations
    event_generator.stop()
    await run_task  # ensure it finishes cleanly

    # Verify that generate_user_session was called for each user, at least once
    # Since users_count=2, and the loop runs at least one full iteration
    assert event_generator.generate_user_session.call_count >= 2
    # Verify that the loop slept for 5 seconds (simulated by asyncio.sleep)
    # In the real test asyncio.sleep is not mocked, but we don't need to assert it.

def test_stop_sets_flag(event_generator):
    """Test that stop() sets is_running to False."""
    event_generator.is_running = True
    event_generator.stop()
    assert event_generator.is_running is False
