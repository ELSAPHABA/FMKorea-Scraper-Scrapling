FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml requirements.lock README.md ./
COPY src ./src
COPY docker-entrypoint.sh ./
RUN pip install --no-cache-dir -r requirements.lock && pip install --no-deps . && scrapling install && chmod +x docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
