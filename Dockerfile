FROM python:3.12-slim

WORKDIR /app

# Install system dependencies (including Playwright dependencies)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    postgresql-client \
    git \
    wget \
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libpango-1.0-0 \
    libcairo2 \
    && rm -rf /var/lib/apt/lists/*

# Copy backend requirements and install Python dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers (only Chromium to save space)
RUN playwright install chromium

# Copy backend application code
COPY backend/ .

# Copy seed data
COPY data/asx_director_transactions.json /app/data/asx_director_transactions.json
COPY data/asx200_sectors.json /app/data/asx200_sectors.json

# Copy startup script
COPY start.sh /app/start.sh
RUN chmod +x /app/start.sh

# Expose port (Railway uses $PORT env var)
EXPOSE 8000

CMD ["/app/start.sh"]
