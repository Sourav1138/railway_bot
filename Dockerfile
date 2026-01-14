FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY v.py .

# Create directories for data
RUN mkdir -p data/cookies data/downloads data/temp

ENV PORT=5000
ENV PYTHONUNBUFFERED=1

CMD ["python", "v.py"]
