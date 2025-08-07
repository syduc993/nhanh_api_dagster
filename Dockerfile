FROM python:3.10-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Create data directory
RUN mkdir -p data

# Set environment variables
ENV DAGSTER_HOME=/app
ENV PYTHONPATH=/app

# Expose port
EXPOSE 3000

# Command to run Dagster
CMD ["dagster", "dev", "--host", "0.0.0.0", "--port", "3000"]
