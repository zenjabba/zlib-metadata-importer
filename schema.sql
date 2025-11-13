-- Anna's Archive ZLib Metadata Database Schema

-- Files table: maps zlibrary_id to MD5
CREATE TABLE IF NOT EXISTS zlib_files (
    aacid TEXT PRIMARY KEY,
    zlibrary_id INTEGER NOT NULL,
    md5 TEXT NOT NULL,
    data_folder TEXT
);

CREATE INDEX IF NOT EXISTS idx_files_zlibrary_id ON zlib_files(zlibrary_id);
CREATE INDEX IF NOT EXISTS idx_files_md5 ON zlib_files(md5);

-- Records table: full book metadata
CREATE TABLE IF NOT EXISTS zlib_records (
    aacid TEXT PRIMARY KEY,
    zlibrary_id INTEGER NOT NULL UNIQUE,
    md5_reported TEXT,
    title TEXT,
    author TEXT,
    publisher TEXT,
    language TEXT,
    series TEXT,
    volume TEXT,
    edition TEXT,
    year TEXT,
    pages TEXT,
    description TEXT,
    extension TEXT,
    filesize_reported INTEGER,
    date_added TEXT,
    date_modified TEXT,
    cover_path TEXT,
    isbns TEXT, -- JSON array stored as text
    category_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_records_zlibrary_id ON zlib_records(zlibrary_id);
CREATE INDEX IF NOT EXISTS idx_records_md5 ON zlib_records(md5_reported);
CREATE INDEX IF NOT EXISTS idx_records_language ON zlib_records(language);
CREATE INDEX IF NOT EXISTS idx_records_extension ON zlib_records(extension);
CREATE INDEX IF NOT EXISTS idx_records_author ON zlib_records(author);
CREATE INDEX IF NOT EXISTS idx_records_year ON zlib_records(year);

-- Import progress tracking
CREATE TABLE IF NOT EXISTS import_progress (
    table_name TEXT PRIMARY KEY,
    records_imported INTEGER DEFAULT 0,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
