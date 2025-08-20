-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS user_activity
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Use the keyspace
USE user_activity;

-- Create sample table
CREATE TABLE IF NOT EXISTS event_logs (
    id UUID PRIMARY KEY,
    name TEXT,
    username TEXT,
    action TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Create index for time-based queries
CREATE INDEX IF NOT EXISTS idx_created_at ON event_logs (created_at);

-- Insert sample data
INSERT INTO event_logs (id, name, username, action, created_at, updated_at)
VALUES (uuid(), 'John Doe', 'john@example.com', 'click', toTimestamp(now()), toTimestamp(now()));

INSERT INTO event_logs (id, name, username, action, created_at, updated_at)
VALUES (uuid(), 'John Doe', 'john@example.com', 'render', toTimestamp(now()), toTimestamp(now()));

INSERT INTO event_logs (id, name, username, action, created_at, updated_at)
VALUES (uuid(), 'John Doe', 'john@example.com', 'click', toTimestamp(now()), toTimestamp(now()));