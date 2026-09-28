# The Delta maintenance job (#643): scripts/delta_maintenance.py with its
# hash-pinned `deltalake`, preinstalled so the CronJob needs no package index
# at runtime. Published as ghcr.io/pinax-network/firehose-parquet-maintenance
# next to the fireparq image (.github/workflows/docker-publish.yml).
FROM python:3.12-slim-bookworm AS builder
COPY scripts/delta_maintenance.requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check \
      --only-binary=:all: --require-hashes --target /opt/site \
      -r /tmp/requirements.txt

FROM python:3.12-slim-bookworm
COPY --from=builder /opt/site /opt/site
COPY scripts/delta_maintenance.py /opt/delta_maintenance.py
ENV PYTHONPATH=/opt/site \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HOME=/tmp
USER 65534:65534
ENTRYPOINT ["python", "/opt/delta_maintenance.py"]
