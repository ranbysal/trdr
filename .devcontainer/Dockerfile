FROM mcr.microsoft.com/devcontainers/python:1-3.11

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        python3-numpy \
        python3-pandas \
        python3-yaml \
        python3-pytest \
    && rm -rf /var/lib/apt/lists/*
