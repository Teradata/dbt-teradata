##
#  Generic dockerfile for dbt image building.
#  See README for operational details
##

# Top level build args
ARG build_for=linux/amd64

##
# base image (abstract)
##
FROM --platform=$build_for python:3.10-slim as base

# System setup
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     git \
     ssh-client \
     make \
     build-essential \
     ca-certificates \
     libpq-dev \
  && rm -rf /var/lib/apt/lists/*

# Env vars
ENV PYTHONIOENCODING=utf-8
ENV LANG=C.UTF-8

# Upgrade python tooling
RUN python -m pip install --upgrade pip setuptools wheel --no-cache-dir

# Set docker basics
WORKDIR /usr/app/dbt/
VOLUME /usr/app
ENTRYPOINT ["dbt"]

##
# dbt-core
##
FROM base as dbt-core

# Install dbt-teradata and dbt-core
RUN python -m pip install --no-cache-dir dbt-teradata==1.10.1 \
    && python -m pip install --no-cache-dir "dbt-core>=1.10.0"
