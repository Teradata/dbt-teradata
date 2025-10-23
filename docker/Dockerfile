##
#  Generic dockerfile for dbt image building.
#  See README for operational details
##

# Top level build args
ARG build_for=linux/amd64

##
# base image (abstract)
##
FROM --platform=$build_for python:3.13-slim as base

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

# Upgrade Python tooling
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel

# Set docker basics
WORKDIR /usr/app/dbt/
VOLUME /usr/app
ENTRYPOINT ["dbt"]

##
# dbt-core
##
FROM base as dbt-core

# Install dbt-teradata and dbt-core
RUN python -m pip install --no-cache-dir dbt-core dbt-teradata==1.10.1
