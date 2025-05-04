FROM python:3.13-bookworm

WORKDIR /app
COPY requirements.txt server_scanner.py ./
RUN apt-get update && apt-get upgrade -y && \
    python -m venv .venv

ENV PATH="/app/.venv/bin:$PATH"
RUN pip install -r requirements.txt
CMD ["python", "server_scanner.py"]
