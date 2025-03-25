# Stage 1: Build dependencies
FROM python:3.11-slim as builder

WORKDIR /crawler

# Install system dependencies for Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Copy only requirements to cache dependencies
COPY requirements.txt /crawler/requirements.txt
RUN pip install --user --no-cache-dir -r requirements.txt


# Stage 2: Final runtime image
FROM python:3.11-slim

WORKDIR /crawler

# Copy installed Python packages from builder stage
COPY --from=builder /root/.local /root/.local

# Update PATH to include pip-installed binaries
ENV PATH=/root/.local/bin:$PATH

# Copy project files
COPY . /crawler
COPY config.json /crawler/config.json

STOPSIGNAL SIGINT

CMD ["python", "main.py"]
