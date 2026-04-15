FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl tar && rm -rf /var/lib/apt/lists/*

# Install Bruin CLI directly to /usr/local/bin
RUN curl -L https://github.com/bruin-data/bruin/releases/latest/download/bruin_linux_amd64.tar.gz \
    | tar -xz -C /usr/local/bin bruin && chmod +x /usr/local/bin/bruin

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bruin/ .

CMD ["bruin", "run", "."]
