#!/usr/bin/env python3
"""
Query tool for Anna's Archive ZLib metadata database.
"""

import sqlite3
import json
import sys
from pathlib import Path

DB_FILE = "zlib_metadata.db"

class MetadataQuery:
    def __init__(self, db_path=DB_FILE):
        if not Path(db_path).exists():
            print(f"Error: Database {db_path} not found. Run import_metadata.py first.", file=sys.stderr)
            sys.exit(1)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row

    def close(self):
        if self.conn:
            self.conn.close()

    def get_by_md5(self, md5):
        """Get book metadata by MD5 hash"""
        cursor = self.conn.execute("""
            SELECT * FROM zlib_records WHERE md5_reported = ?
        """, (md5,))
        return cursor.fetchone()

    def get_by_zlibrary_id(self, zlib_id):
        """Get book metadata by zlibrary_id"""
        cursor = self.conn.execute("""
            SELECT * FROM zlib_records WHERE zlibrary_id = ?
        """, (zlib_id,))
        return cursor.fetchone()

    def search_by_title(self, title, limit=10):
        """Search books by title"""
        cursor = self.conn.execute("""
            SELECT zlibrary_id, title, author, year, language, extension, filesize_reported
            FROM zlib_records
            WHERE title LIKE ?
            ORDER BY zlibrary_id DESC
            LIMIT ?
        """, (f"%{title}%", limit))
        return cursor.fetchall()

    def search_by_author(self, author, limit=10):
        """Search books by author"""
        cursor = self.conn.execute("""
            SELECT zlibrary_id, title, author, year, language, extension, filesize_reported
            FROM zlib_records
            WHERE author LIKE ?
            ORDER BY zlibrary_id DESC
            LIMIT ?
        """, (f"%{author}%", limit))
        return cursor.fetchall()

    def filter_books(self, language=None, extension=None, year_from=None, year_to=None, limit=100):
        """Filter books by criteria"""
        conditions = []
        params = []

        if language:
            conditions.append("language = ?")
            params.append(language)
        if extension:
            conditions.append("extension = ?")
            params.append(extension)
        if year_from:
            conditions.append("year >= ?")
            params.append(str(year_from))
        if year_to:
            conditions.append("year <= ?")
            params.append(str(year_to))

        where_clause = " AND ".join(conditions) if conditions else "1=1"
        params.append(limit)

        cursor = self.conn.execute(f"""
            SELECT zlibrary_id, title, author, year, language, extension, filesize_reported
            FROM zlib_records
            WHERE {where_clause}
            ORDER BY zlibrary_id DESC
            LIMIT ?
        """, params)
        return cursor.fetchall()

    def get_stats(self):
        """Get database statistics"""
        stats = {}

        cursor = self.conn.execute("SELECT COUNT(*) FROM zlib_files")
        stats['total_files'] = cursor.fetchone()[0]

        cursor = self.conn.execute("SELECT COUNT(*) FROM zlib_records")
        stats['total_records'] = cursor.fetchone()[0]

        cursor = self.conn.execute("""
            SELECT language, COUNT(*) as count
            FROM zlib_records
            WHERE language IS NOT NULL AND language != ''
            GROUP BY language
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['top_languages'] = [(row[0], row[1]) for row in cursor.fetchall()]

        cursor = self.conn.execute("""
            SELECT extension, COUNT(*) as count
            FROM zlib_records
            WHERE extension IS NOT NULL AND extension != ''
            GROUP BY extension
            ORDER BY count DESC
            LIMIT 10
        """)
        stats['top_extensions'] = [(row[0], row[1]) for row in cursor.fetchall()]

        return stats

    def print_record(self, row):
        """Pretty print a database record"""
        if not row:
            print("No record found")
            return

        print("\n" + "="*80)
        print(f"ZLibrary ID: {row['zlibrary_id']}")
        print(f"Title:       {row['title']}")
        print(f"Author:      {row['author']}")
        print(f"Publisher:   {row['publisher']}")
        print(f"Year:        {row['year']}")
        print(f"Language:    {row['language']}")
        print(f"Extension:   {row['extension']}")
        print(f"Filesize:    {row['filesize_reported']:,} bytes" if row['filesize_reported'] else "Filesize:    N/A")
        print(f"MD5:         {row['md5_reported']}")
        if row['isbns']:
            isbns = json.loads(row['isbns'])
            if isbns:
                print(f"ISBNs:       {', '.join(isbns)}")
        if row['description']:
            desc = row['description'][:200] + "..." if len(row['description']) > 200 else row['description']
            print(f"Description: {desc}")
        print("="*80)

def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  query_metadata.py stats                              - Show database statistics")
        print("  query_metadata.py md5 <hash>                         - Lookup by MD5")
        print("  query_metadata.py id <zlibrary_id>                   - Lookup by zlibrary_id")
        print("  query_metadata.py title <search_term>                - Search by title")
        print("  query_metadata.py author <search_term>               - Search by author")
        print("  query_metadata.py filter --lang=en --ext=epub        - Filter books")
        sys.exit(1)

    db = MetadataQuery()

    try:
        command = sys.argv[1]

        if command == "stats":
            stats = db.get_stats()
            print("\n" + "="*80)
            print("DATABASE STATISTICS")
            print("="*80)
            print(f"Total Files:   {stats['total_files']:,}")
            print(f"Total Records: {stats['total_records']:,}")
            print("\nTop 10 Languages:")
            for lang, count in stats['top_languages']:
                print(f"  {lang:20} {count:,}")
            print("\nTop 10 Extensions:")
            for ext, count in stats['top_extensions']:
                print(f"  {ext:20} {count:,}")
            print("="*80)

        elif command == "md5":
            if len(sys.argv) < 3:
                print("Error: MD5 hash required", file=sys.stderr)
                sys.exit(1)
            row = db.get_by_md5(sys.argv[2])
            db.print_record(row)

        elif command == "id":
            if len(sys.argv) < 3:
                print("Error: zlibrary_id required", file=sys.stderr)
                sys.exit(1)
            row = db.get_by_zlibrary_id(int(sys.argv[2]))
            db.print_record(row)

        elif command == "title":
            if len(sys.argv) < 3:
                print("Error: search term required", file=sys.stderr)
                sys.exit(1)
            rows = db.search_by_title(sys.argv[2])
            print(f"\nFound {len(rows)} results:")
            for row in rows:
                print(f"  [{row['zlibrary_id']}] {row['title']} - {row['author']} ({row['year']})")

        elif command == "author":
            if len(sys.argv) < 3:
                print("Error: search term required", file=sys.stderr)
                sys.exit(1)
            rows = db.search_by_author(sys.argv[2])
            print(f"\nFound {len(rows)} results:")
            for row in rows:
                print(f"  [{row['zlibrary_id']}] {row['title']} - {row['author']} ({row['year']})")

        elif command == "filter":
            # Parse filter arguments
            lang = None
            ext = None
            for arg in sys.argv[2:]:
                if arg.startswith("--lang="):
                    lang = arg.split("=")[1]
                elif arg.startswith("--ext="):
                    ext = arg.split("=")[1]

            rows = db.filter_books(language=lang, extension=ext)
            print(f"\nFound {len(rows)} results:")
            for row in rows:
                print(f"  [{row['zlibrary_id']}] {row['title']} - {row['author']} ({row['year']}) [{row['extension']}]")

        else:
            print(f"Unknown command: {command}", file=sys.stderr)
            sys.exit(1)

    finally:
        db.close()

if __name__ == "__main__":
    main()
