FROM python:3.11-slim

# Install zstd for decompression
RUN apt-get update && \
    apt-get install -y --no-install-recommends zstd && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy application files
COPY import_metadata.py .
COPY query_metadata.py .

# Make scripts executable
RUN chmod +x import_metadata.py query_metadata.py

# Data directory for metadata files and database
VOLUME ["/data"]

# Set working directory to /data for runtime
WORKDIR /data

# Use ENTRYPOINT to allow passing file names as arguments
ENTRYPOINT ["python3", "/app/import_metadata.py"]
