FROM python:3.11-slim

WORKDIR /app

# Install only what the API needs at runtime (no TensorFlow/training deps)
COPY api/requirements.docker.txt .
RUN pip install --no-cache-dir -r requirements.docker.txt

# Copy application source
COPY api/       ./api/
COPY etl/       ./etl/
COPY ml/        ./ml/

EXPOSE 8000

CMD ["gunicorn", "api.main:app", \
     "--workers", "2", \
     "--worker-class", "uvicorn.workers.UvicornWorker", \
     "--bind", "0.0.0.0:8000", \
     "--timeout", "120", \
     "--log-level", "info"]
