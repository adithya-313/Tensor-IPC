# ---- TensorIPC Worker B: Shared-Memory Reader ----
# Uses a slim Python 3.11 base image so the container stays small.
FROM python:3.11-slim

# Copy dependency manifest first for better layer caching.
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt

# Create the directory structure that mirrors our domain layout.
WORKDIR /app
COPY src/ /app

# Run the reader domain logic as the container entrypoint.
# Run the reader from /app — src/ was copied directly into /app,
# so the package path is domain.reader (not src.domain.reader).
CMD ["python", "-m", "domain.reader"]
