from seedr_tg.db.models import JobPhase, JobRecord
from seedr_tg.worker.progress import format_job_status, progress_bar


def test_progress_bar_bounds() -> None:
    assert progress_bar(-10) == "[------------]"
    assert progress_bar(100) == "[############]"


def test_format_job_status_includes_failure_reason() -> None:
    job = JobRecord(
        id=1,
        magnet_link="magnet:?xt=urn:btih:abc",
        source_chat_id=1,
        source_message_id=1,
        admin_message_id=1,
        target_chat_id=2,
        phase=JobPhase.FAILED,
        queue_position=1,
        torrent_name="Example",
        total_size_bytes=1024,
        seedr_torrent_id=1,
        seedr_folder_id=2,
        seedr_folder_name="Example",
        progress_percent=50,
        current_step="Failed",
        local_path=None,
        upload_file_count=1,
        uploaded_file_count=0,
        failure_reason="Bad torrent",
        last_error="Bad torrent",
        cancel_requested=False,
        created_at="2026-03-12T00:00:00+00:00",
        updated_at="2026-03-12T00:00:00+00:00",
    )
    payload = format_job_status(job)
    assert "Bad torrent" in payload
