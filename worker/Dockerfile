FROM python:3.11-slim

# Prevent Python from writing pyc files and buffering stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first to cache the Docker layer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the actual worker script
COPY splitter.py .

# Run the worker when the container starts
CMD ["python", "splitter.py"]