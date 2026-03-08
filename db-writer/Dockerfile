FROM python:3.13-slim

# Seamlessly install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

# Ensure Python output is sent straight to terminal (unbuffered)
ENV PYTHONUNBUFFERED=1

# Copy dependency configuration and install using uv into the system
COPY pyproject.toml .
RUN uv pip install --system asyncpg confluent-kafka psycopg2-binary

# Copy the db-writer execution script and other context files
COPY . .

# Run the data generator
CMD ["python", "db_writer.py"]
