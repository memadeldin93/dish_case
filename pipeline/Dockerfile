# Use Python 3.12 (matches your local setup)
FROM python:3.12.1-slim

# Helpful Python settings
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Install system packages (curl is used to install uv)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Add uv to PATH (uv installs under /root/.local/bin by default)
ENV PATH="/root/.local/bin:${PATH}"

# Set working directory
WORKDIR /app

# Copy only dependency files first (better Docker layer caching)
COPY pyproject.toml uv.lock .python-version ./

# Install dependencies using uv (no dev dependencies)
RUN uv sync --frozen --no-dev

# Copy the rest of the project files
COPY data_pipeline.py ./
COPY .env.example .env.example

# Create output folders used by the pipeline
RUN mkdir -p raw_responses run_manifests

# Default command to run the ETL
CMD ["uv", "run", "data_pipeline.py"]