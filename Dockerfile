FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update -y \
    && apt-get install --no-install-recommends -y curl git \
    && curl -fsSL https://deb.nodesource.com/setup_22.x | bash - \
    && apt-get install -y nodejs \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir uv==0.8.3

RUN uv tool install --from git+https://github.com/elephant-xyz/AI-Agent test-evaluator-agent

WORKDIR /data
CMD ["uvx", "test-evaluator-agent"]
