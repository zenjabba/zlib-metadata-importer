# Anna's Archive ZLib Metadata Database

This project imports and indexes Anna's Archive ZLib metadata files into a SQLite database for fast querying and efficient book import operations.

## Overview

The metadata files contain:
- **zlib3_files**: 8.3M entries mapping zlibrary_id to MD5 hashes
- **zlib3_records**: 98.4M book metadata records (title, author, language, etc.)

## Quick Start

### Using Docker (Recommended)

1. Build the container:
```bash
docker build -t anna-metadata-importer .
```

2. Run the import (replace with your actual file names):
```bash
docker run --rm -v $(pwd):/data anna-metadata-importer \
  annas_archive_meta__aacid__zlib3_files__20230808T051503Z--20251027T180442Z.jsonl.seekable.zst \
  annas_archive_meta__aacid__zlib3_records__20240809T171652Z--20251027T190001Z.jsonl.seekable.zst
```

The import will:
- Create `zlib_metadata.db` in the current directory
- Stream and decompress the .zst files
- Import in batches with progress tracking
- Can be interrupted and resumed
- Container is automatically removed after completion (--rm flag)

3. Query the database:
```bash
docker run --rm -v $(pwd):/data anna-metadata-importer python3 /app/query_metadata.py stats

docker run --rm -v $(pwd):/data anna-metadata-importer python3 /app/query_metadata.py md5 <hash>

docker run --rm -v $(pwd):/data anna-metadata-importer python3 /app/query_metadata.py title "your search"
```

### Using Python Directly

If you prefer to run without Docker:

```bash
python3 import_metadata.py <files.zst> <records.zst>
```

Example:
```bash
python3 import_metadata.py \
  annas_archive_meta__aacid__zlib3_files__20230808T051503Z--20251027T180442Z.jsonl.seekable.zst \
  annas_archive_meta__aacid__zlib3_records__20240809T171652Z--20251027T190001Z.jsonl.seekable.zst
```

Requirements:
- Python 3.7+
- zstd command-line tool

### Command-Line Options

```
usage: import_metadata.py [-h] [--db DB] files_zst records_zst

positional arguments:
  files_zst   Path to the zlib3_files .jsonl.seekable.zst file
  records_zst Path to the zlib3_records .jsonl.seekable.zst file

optional arguments:
  -h, --help  show this help message and exit
  --db DB     Database file path (default: zlib_metadata.db)
```

## Files

- `schema.sql` - Database schema definition
- `import_metadata.py` - Import script with progress tracking
- `query_metadata.py` - Query tool for lookups
- `Dockerfile` - Container definition
- `docker-compose.yml` - Docker Compose configuration

## Database Schema

### zlib_files
- Maps zlibrary_id to MD5 hashes
- Indexed on: zlibrary_id, md5

### zlib_records
- Full book metadata
- Indexed on: zlibrary_id, md5_reported, language, extension, author, year

## Query Examples

### Get database statistics:
```bash
# Using Python directly
python3 query_metadata.py stats

# Using Docker
docker run --rm -v $(pwd):/data anna-metadata-importer python3 /app/query_metadata.py stats
```

### Lookup by MD5:
```bash
python3 query_metadata.py md5 63332c8d6514aa6081d088de96ed1d4f
```

### Lookup by zlibrary_id:
```bash
python3 query_metadata.py id 22430000
```

### Search by title:
```bash
python3 query_metadata.py title "Python"
```

### Search by author:
```bash
python3 query_metadata.py author "Tolkien"
```

### Filter books:
```bash
python3 query_metadata.py filter --lang=english --ext=epub
```

## Performance

- Import time: 1-2 hours for 98M records
- Database size: ~15-25 GB
- Query performance: Milliseconds with indexes

## Monthly Updates

When new metadata files are released:

1. Download new .zst files
2. Stop any running queries
3. Run import again (it will detect existing data)
4. Option A: Full reimport (delete zlib_metadata.db first)
5. Option B: Incremental update (script handles duplicates)

## Direct SQLite Access

You can also query the database directly:

```bash
sqlite3 zlib_metadata.db
```

```sql
-- Find all English EPUBs from 2023
SELECT title, author FROM zlib_records
WHERE language='english' AND extension='epub' AND year='2023'
LIMIT 10;

-- Get book by MD5
SELECT * FROM zlib_records
WHERE md5_reported='63332c8d6514aa6081d088de96ed1d4f';

-- Count books by language
SELECT language, COUNT(*) FROM zlib_records
GROUP BY language
ORDER BY COUNT(*) DESC;
```

## Troubleshooting

### Import interrupted
The script saves progress every 100k records. Just run it again to resume.

### Out of disk space
The database will be ~15-25 GB. Ensure you have at least 30 GB free.

### Schema file not found (Docker)
Make sure you're running from the directory containing the metadata files.
