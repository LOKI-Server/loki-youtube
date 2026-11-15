CREATE TABLE IF NOT EXISTS youtube_jobs (
    id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    video_id TEXT NOT NULL,
    title TEXT NOT NULL DEFAULT '',
    quality TEXT NOT NULL DEFAULT '',
    size INTEGER,
    created INTEGER NOT NULL,
    status TEXT NOT NULL,
    expires INTEGER
);
CREATE INDEX IF NOT EXISTS youtube_jobs_user ON youtube_jobs(user_id, created);
