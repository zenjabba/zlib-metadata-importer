#!/usr/bin/env python3
"""
Import Anna's Archive ZLib metadata into SQLite database.
Handles large datasets efficiently with batch processing and progress tracking.
"""

import sqlite3
import json
import subprocess
import sys
import argparse
from datetime import datetime
from pathlib import Path

DB_FILE = "zlib_metadata.db"
BATCH_SIZE = 10000
PROGRESS_INTERVAL = 100000

class MetadataImporter:
    def __init__(self, db_path=DB_FILE):
        self.db_path = db_path
        self.conn = None

    def connect(self):
        """Connect to database and set optimizations for bulk import"""
        self.conn = sqlite3.connect(self.db_path)
        # Performance optimizations for bulk import
        self.conn.execute("PRAGMA journal_mode = OFF")
        self.conn.execute("PRAGMA synchronous = OFF")
        self.conn.execute("PRAGMA cache_size = 1000000")
        self.conn.execute("PRAGMA locking_mode = EXCLUSIVE")
        self.conn.execute("PRAGMA temp_store = MEMORY")

    def close(self):
        if self.conn:
            self.conn.close()

    def init_schema(self):
        """Initialize database schema"""
        print("Initializing database schema...")
        # Try both locations for schema file (local and Docker)
        schema_paths = ["schema.sql", "/app/schema.sql"]
        for schema_path in schema_paths:
            if Path(schema_path).exists():
                with open(schema_path, "r") as f:
                    self.conn.executescript(f.read())
                self.conn.commit()
                return
        raise FileNotFoundError("schema.sql not found")

    def get_progress(self, table_name):
        """Get import progress for a table"""
        cursor = self.conn.execute(
            "SELECT records_imported FROM import_progress WHERE table_name = ?",
            (table_name,)
        )
        row = cursor.fetchone()
        return row[0] if row else 0

    def update_progress(self, table_name, count):
        """Update import progress"""
        self.conn.execute("""
            INSERT INTO import_progress (table_name, records_imported, last_updated)
            VALUES (?, ?, ?)
            ON CONFLICT(table_name) DO UPDATE SET
                records_imported = ?,
                last_updated = ?
        """, (table_name, count, datetime.now(), count, datetime.now()))
        self.conn.commit()

    def import_files(self, zst_file):
        """Import zlib_files from compressed JSONL"""
        print(f"\nImporting files from {zst_file}...")

        # Check if already imported
        existing = self.get_progress("zlib_files")
        if existing > 0:
            print(f"Found {existing} existing records. Skipping import.")
            return

        batch = []
        count = 0

        # Stream decompression
        proc = subprocess.Popen(
            ["zstd", "-d", "-c", zst_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        try:
            for line in proc.stdout:
                if not line.strip():
                    continue

                try:
                    data = json.loads(line)
                    metadata = data.get("metadata", {})

                    batch.append((
                        data.get("aacid"),
                        metadata.get("zlibrary_id"),
                        metadata.get("md5"),
                        data.get("data_folder")
                    ))

                    if len(batch) >= BATCH_SIZE:
                        self.conn.executemany("""
                            INSERT OR IGNORE INTO zlib_files
                            (aacid, zlibrary_id, md5, data_folder)
                            VALUES (?, ?, ?, ?)
                        """, batch)
                        count += len(batch)
                        batch = []

                        if count % PROGRESS_INTERVAL == 0:
                            self.update_progress("zlib_files", count)
                            print(f"  Imported {count:,} files...")

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON: {e}", file=sys.stderr)
                    continue

            # Insert remaining batch
            if batch:
                self.conn.executemany("""
                    INSERT OR IGNORE INTO zlib_files
                    (aacid, zlibrary_id, md5, data_folder)
                    VALUES (?, ?, ?, ?)
                """, batch)
                count += len(batch)

            self.conn.commit()
            self.update_progress("zlib_files", count)
            print(f"  Completed: {count:,} files imported")

        finally:
            proc.wait()

    def import_records(self, zst_file):
        """Import zlib_records from compressed JSONL"""
        print(f"\nImporting records from {zst_file}...")

        # Check if already imported
        existing = self.get_progress("zlib_records")
        if existing > 0:
            response = input(f"Found {existing:,} existing records. Continue from there? (y/n): ")
            if response.lower() != 'y':
                print("Skipping import.")
                return

        batch = []
        count = existing
        skipped = 0

        # Stream decompression
        proc = subprocess.Popen(
            ["zstd", "-d", "-c", zst_file],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        try:
            for line in proc.stdout:
                # Skip already imported records
                if skipped < existing:
                    skipped += 1
                    if skipped % PROGRESS_INTERVAL == 0:
                        print(f"  Skipping to record {skipped:,}...")
                    continue

                if not line.strip():
                    continue

                try:
                    data = json.loads(line)
                    metadata = data.get("metadata", {})

                    batch.append((
                        data.get("aacid"),
                        metadata.get("zlibrary_id"),
                        metadata.get("md5_reported"),
                        metadata.get("title"),
                        metadata.get("author"),
                        metadata.get("publisher"),
                        metadata.get("language"),
                        metadata.get("series"),
                        metadata.get("volume"),
                        metadata.get("edition"),
                        metadata.get("year"),
                        metadata.get("pages"),
                        metadata.get("description"),
                        metadata.get("extension"),
                        metadata.get("filesize_reported"),
                        metadata.get("date_added"),
                        metadata.get("date_modified"),
                        metadata.get("cover_path"),
                        json.dumps(metadata.get("isbns", [])),
                        metadata.get("category_id")
                    ))

                    if len(batch) >= BATCH_SIZE:
                        self.conn.executemany("""
                            INSERT OR IGNORE INTO zlib_records
                            (aacid, zlibrary_id, md5_reported, title, author, publisher,
                             language, series, volume, edition, year, pages, description,
                             extension, filesize_reported, date_added, date_modified,
                             cover_path, isbns, category_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, batch)
                        count += len(batch)
                        batch = []

                        if count % PROGRESS_INTERVAL == 0:
                            self.update_progress("zlib_records", count)
                            print(f"  Imported {count:,} records...")

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON at record {count}: {e}", file=sys.stderr)
                    continue

            # Insert remaining batch
            if batch:
                self.conn.executemany("""
                    INSERT OR IGNORE INTO zlib_records
                    (aacid, zlibrary_id, md5_reported, title, author, publisher,
                     language, series, volume, edition, year, pages, description,
                     extension, filesize_reported, date_added, date_modified,
                     cover_path, isbns, category_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, batch)
                count += len(batch)

            self.conn.commit()
            self.update_progress("zlib_records", count)
            print(f"  Completed: {count:,} records imported")

        finally:
            proc.wait()

    def create_indexes(self):
        """Create indexes after import for better performance"""
        print("\nCreating indexes (this may take a while)...")

        indexes = [
            ("idx_files_zlibrary_id", "zlib_files", "zlibrary_id"),
            ("idx_files_md5", "zlib_files", "md5"),
            ("idx_records_zlibrary_id", "zlib_records", "zlibrary_id"),
            ("idx_records_md5", "zlib_records", "md5_reported"),
            ("idx_records_language", "zlib_records", "language"),
            ("idx_records_extension", "zlib_records", "extension"),
            ("idx_records_author", "zlib_records", "author"),
            ("idx_records_year", "zlib_records", "year"),
        ]

        for idx_name, table, column in indexes:
            print(f"  Creating {idx_name}...")
            self.conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")

        self.conn.commit()
        print("Indexes created successfully")

    def optimize_db(self):
        """Optimize database after import"""
        print("\nOptimizing database...")
        self.conn.execute("PRAGMA journal_mode = WAL")
        self.conn.execute("PRAGMA synchronous = NORMAL")
        self.conn.execute("ANALYZE")
        self.conn.commit()
        print("Optimization complete")

    def get_stats(self):
        """Print database statistics"""
        print("\n" + "="*60)
        print("DATABASE STATISTICS")
        print("="*60)

        cursor = self.conn.execute("SELECT COUNT(*) FROM zlib_files")
        files_count = cursor.fetchone()[0]
        print(f"Files:   {files_count:,}")

        cursor = self.conn.execute("SELECT COUNT(*) FROM zlib_records")
        records_count = cursor.fetchone()[0]
        print(f"Records: {records_count:,}")

        # Get database file size
        db_size = Path(self.db_path).stat().st_size / (1024**3)
        print(f"DB Size: {db_size:.2f} GB")
        print("="*60)

def main():
    parser = argparse.ArgumentParser(
        description="Import Anna's Archive ZLib metadata into SQLite database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python3 import_metadata.py files.jsonl.seekable.zst records.jsonl.seekable.zst
  docker run --rm -v $(pwd):/data importer files.jsonl.seekable.zst records.jsonl.seekable.zst
        """
    )
    parser.add_argument(
        "files_zst",
        help="Path to the zlib3_files .jsonl.seekable.zst file"
    )
    parser.add_argument(
        "records_zst",
        help="Path to the zlib3_records .jsonl.seekable.zst file"
    )
    parser.add_argument(
        "--db",
        default=DB_FILE,
        help=f"Database file path (default: {DB_FILE})"
    )

    args = parser.parse_args()

    files_zst = args.files_zst
    records_zst = args.records_zst

    # Check files exist
    if not Path(files_zst).exists():
        print(f"Error: {files_zst} not found", file=sys.stderr)
        sys.exit(1)
    if not Path(records_zst).exists():
        print(f"Error: {records_zst} not found", file=sys.stderr)
        sys.exit(1)

    print("Anna's Archive ZLib Metadata Importer")
    print("="*60)
    print(f"Files:   {files_zst}")
    print(f"Records: {records_zst}")
    print(f"Database: {args.db}")
    print("="*60)

    importer = MetadataImporter(args.db)

    try:
        importer.connect()
        importer.init_schema()

        # Import files
        importer.import_files(files_zst)

        # Import records
        importer.import_records(records_zst)

        # Create indexes
        importer.create_indexes()

        # Optimize
        importer.optimize_db()

        # Show stats
        importer.get_stats()

        print("\nImport completed successfully!")

    except KeyboardInterrupt:
        print("\n\nImport interrupted. Progress has been saved.")
        print("Run the script again to continue from where you left off.")
        importer.get_stats()

    except Exception as e:
        print(f"\nError during import: {e}", file=sys.stderr)
        sys.exit(1)

    finally:
        importer.close()

if __name__ == "__main__":
    main()
