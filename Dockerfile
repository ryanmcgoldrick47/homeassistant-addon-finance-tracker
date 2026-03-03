ARG BUILD_FROM=ghcr.io/home-assistant/amd64-base-python:3.11-alpine3.18
FROM $BUILD_FROM

WORKDIR /app

# Install build deps for Python packages
RUN apk add --no-cache gcc musl-dev libffi-dev

COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY run.sh /
RUN chmod +x /run.sh

COPY app/ ./

CMD ["/run.sh"]
