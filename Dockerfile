FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Default command: run the ETL pipeline
CMD ["python", "scripts/etl_pipeline.py"]
