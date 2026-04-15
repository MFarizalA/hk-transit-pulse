FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Bruin CLI
RUN curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | sh

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bruin/ .

CMD ["bruin", "run", "."]
