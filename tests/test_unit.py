import asyncio
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, parent_dir)


# Assuming the module to test is in 'event_generator'
from src.producer.event_generator import EventGenerator, MovieEvent, EventType, DeviceType


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


class TestEventGenerator:
    """Unit tests for EventGenerator class."""

    def test_create_event(self, event_generator):
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
        with patch("event_generator.datetime") as mock_datetime:
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
    async def test_generate_user_session_basic_flow(self, event_generator, mock_producer):
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
    async def test_generate_user_session_with_pause_and_resume(self, event_generator, mock_producer):
        """Test that a pause and resume event pair is generated when random condition triggers."""
        user_id = "user_2"

        with patch("random.choice") as mock_choice, patch(
            "random.randint"
        ) as mock_randint, patch("random.random") as mock_random, patch(
            "asyncio.sleep", new_callable=AsyncMock
        ) as mock_sleep:

            mock_choice.side_effect = ["movie_2", DeviceType.TABLET]
            # total_duration=200, step=200 (finish in one iteration), but we want pause to happen
            # pause occurs if random.random() < 0.3 at the beginning of while loop (before progress update)
            mock_randint.side_effect = [200, 200]  # total_duration, step
            # Random values:
            # - first random() in while condition: 0.2 (triggers pause)
            # - after pause and resume, we need to exit loop: next random() for like (0.5) and loop condition (0.5)
            # - search: random() after finish (0.6 -> no search)
            mock_random.side_effect = [0.2, 0.5, 0.5, 0.6]

            await event_generator.generate_user_session(user_id)

        # Expected events: VIEW_STARTED, VIEW_PAUSED, VIEW_RESUMED, VIEW_FINISHED
        assert mock_producer.send_event.call_count == 4

        events = [call[1]["event"] for call in mock_producer.send_event.call_args_list]
        event_types = [e.event_type for e in events]
        assert event_types == [
            EventType.VIEW_STARTED,
            EventType.VIEW_PAUSED,
            EventType.VIEW_RESUMED,
            EventType.VIEW_FINISHED,
        ]

        # Check progress values: pause/resume at the same progress (0 initially)
        assert events[1].progress_seconds == 0
        assert events[2].progress_seconds == 0

    @pytest.mark.asyncio
    async def test_generate_user_session_with_like(self, event_generator, mock_producer):
        """Test that a LIKE event is sent when random condition is met."""
        user_id = "user_3"

        with patch("random.choice") as mock_choice, patch(
            "random.randint"
        ) as mock_randint, patch("random.random") as mock_random, patch(
            "asyncio.sleep", new_callable=AsyncMock
        ) as mock_sleep:

            mock_choice.side_effect = ["movie_3", DeviceType.MOBILE]
            mock_randint.side_effect = [100, 100]  # total_duration, step
            # First random() for pause: 0.5 (no pause)
            # Second random() for like: 0.1 (<0.2) triggers like
            # Third random() for loop condition: 0.5
            # Fourth random() for search: 0.6 (no search)
            mock_random.side_effect = [0.5, 0.1, 0.5, 0.6]

            await event_generator.generate_user_session(user_id)

        events = [call[1]["event"] for call in mock_producer.send_event.call_args_list]
        event_types = [e.event_type for e in events]
        # Order: VIEW_STARTED, LIKE, VIEW_FINISHED
        assert event_types == [EventType.VIEW_STARTED, EventType.LIKED, EventType.VIEW_FINISHED]
        # Like event should have progress > 0
        assert events[1].progress_seconds > 0

    @pytest.mark.asyncio
    async def test_generate_user_session_with_search(self, event_generator, mock_producer):
        """Test that a SEARCH event is sent after finishing if random condition is met."""
        user_id = "user_4"

        with patch("random.choice") as mock_choice, patch(
            "random.randint"
        ) as mock_randint, patch("random.random") as mock_random, patch(
            "asyncio.sleep", new_callable=AsyncMock
        ) as mock_sleep:

            mock_choice.side_effect = ["movie_4", DeviceType.SMART_TV]
            mock_randint.side_effect = [50, 50]  # total_duration, step
            # No pause, no like, but search triggers (0.4 < 0.5)
            mock_random.side_effect = [0.5, 0.5, 0.5, 0.4]  # last one is for search

            await event_generator.generate_user_session(user_id)

        events = [call[1]["event"] for call in mock_producer.send_event.call_args_list]
        event_types = [e.event_type for e in events]
        # Order: VIEW_STARTED, VIEW_FINISHED, SEARCHED
        assert event_types == [
            EventType.VIEW_STARTED,
            EventType.VIEW_FINISHED,
            EventType.SEARCHED,
        ]
        # SEARCHED event has empty movie_id
        assert events[2].movie_id == ""
        assert events[2].progress_seconds == 0

    @pytest.mark.asyncio
    async def test_start_creates_tasks_and_loops(self, event_generator):
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

    def test_stop_sets_flag(self, event_generator):
        """Test that stop() sets is_running to False."""
        event_generator.is_running = True
        event_generator.stop()
        assert event_generator.is_running is False