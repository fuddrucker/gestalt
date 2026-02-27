# Use the official Microsoft Playwright image (Ubuntu Jammy base)
FROM mcr.microsoft.com/playwright/python:v1.58.0-jammy

# Prevent Python from buffering stdout so logs show up instantly in CloudWatch
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Install our Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your actual scraper script into the container
COPY scraper.py .

# Command to run when Fargate spins up the machine
CMD ["python", "scraper.py"]