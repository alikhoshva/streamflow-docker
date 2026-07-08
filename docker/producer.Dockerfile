# Dockerfile for Kafka Producer service
FROM python:3.11-slim
WORKDIR /app

# Copy only the files necessary to install dependencies
COPY pyproject.toml README.md ./
COPY src/ ./src/

# Install packages from pyproject.toml
RUN pip install --no-cache-dir .

# Command to execute the producer script
CMD ["python", "src/streamflow/producer.py"]
