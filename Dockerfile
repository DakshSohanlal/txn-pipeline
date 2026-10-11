# One image for the consumer, API and producer; each service picks its own command.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Dependencies first, so this layer is cached until requirements.txt changes.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer/ consumer/
COPY producer/ producer/
COPY pipeline/ pipeline/
COPY api/ api/

# Run as a non-root user.
RUN useradd --create-home appuser
USER appuser

CMD ["python", "-m", "consumer.main"]
