FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl tar git && rm -rf /var/lib/apt/lists/*

# Install Bruin CLI
RUN curl -LsSf https://raw.githubusercontent.com/bruin-data/bruin/main/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bruin/ .

# Bruin requires a git repo
RUN git init && git add -A && git commit -m "init" --allow-empty

CMD ["bruin", "run", "."]
