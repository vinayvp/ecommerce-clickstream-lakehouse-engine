FROM python:3.10-slim

# Install OpenJDK 17 and curl
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /workspace

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source tree and configuration
COPY configs/ configs/
COPY src/ src/
COPY scripts/ scripts/
COPY tests/ tests/
COPY pyproject.toml .
COPY run_pipeline.py .

CMD ["python", "run_pipeline.py"]
