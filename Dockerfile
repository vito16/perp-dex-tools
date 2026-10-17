# Use Python 3.11 as base image (compatible with all exchanges)
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Configure apt to use Chinese mirrors for faster downloads in China
# Comment out these lines if you're not in China
RUN sed -i 's/deb.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list.d/debian.sources && \
    sed -i 's/security.debian.org/mirrors.ustc.edu.cn/g' /etc/apt/sources.list.d/debian.sources

# Install system dependencies for cryptography and other packages
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt .
COPY apex_requirements.txt .
COPY para_requirements.txt .

# Configure pip to use Chinese mirror for faster downloads in China
# Comment out this line if you're not in China
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir -r para_requirements.txt && \
    pip install --no-cache-dir -r apex_requirements.txt

# Copy the entire project
COPY . .

# Create logs directory
RUN mkdir -p logs

# Set environment variable for Python
ENV PYTHONUNBUFFERED=1

# Default command - can be overridden in docker-compose or docker run
ENTRYPOINT ["python3", "runbot.py"]

# Default arguments (can be overridden)
CMD ["--help"]
