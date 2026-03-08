import pytest
import time
from unittest.mock import AsyncMock, MagicMock
from producer import worker

@pytest.mark.asyncio
async def test_worker_generates_correct_data_structure():
    """Test that the worker correctly constructs db records with proper types and bounds."""
    # Setup mock pool and connection
    mock_conn = AsyncMock()
    mock_pool = MagicMock()
    mock_pool.acquire.return_value.__aenter__.return_value = mock_conn

    # We will test the worker for a single iteration by raising an Exception after one loop
    # or just mocking sleep to throw an error so we break out of the infinite while True.
    mock_sleep = AsyncMock(side_effect=Exception("Break loop"))

    with pytest.MonkeyPatch.context() as m:
        m.setattr("asyncio.sleep", mock_sleep)
        try:
            await worker(mock_pool, "job_01", ["channel_0001", "channel_0002"])
        except Exception as e:
            assert str(e) == "Break loop"

    # Verify the database was called
    assert mock_conn.executemany.call_count == 1

    query, records = mock_conn.executemany.call_args[0]
    assert "INSERT INTO public.raw_metrics" in query
    assert len(records) == 2  # Two channels

    # Verify the records format: (timestamp_ms, job_id, channel_id, value)
    for record in records:
        assert len(record) == 4

        timestamp_ms = record[0]
        assert isinstance(timestamp_ms, int)

        # Verify timestamp is in milliseconds and recent
        current_ms = int(time.time() * 1000)
        assert current_ms - timestamp_ms < 5000  # Should be within 5 seconds

        assert record[1] == "job_01"
        assert record[2] in ["channel_0001", "channel_0002"]
        assert isinstance(record[3], float)
