FROM python:3.11-slim

# Install system dependencies (FFmpeg for video merging)
RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code (Now named main.py)
COPY main.py .

# Create data directories
RUN mkdir -p data/cookies data/downloads data/temp

# Set environment variables
ENV PORT=5000
ENV PYTHONUNBUFFERED=1

# Run the application (Now main.py)
CMD ["python", "main.py"]
