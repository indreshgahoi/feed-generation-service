# NOTE: this image is large (sentence-transformers pulls in torch) --
# see doc/trade-offs.md for the honest trade-off (a production build
# would pin a CPU-only torch wheel or use a multi-stage venv-copy trick;
# not attempted here).
#
# Pinned to 3.10 (matching the host toolchain this was developed/tested
# against), not 3.12: kafka-python==2.0.2's vendored `six` shim breaks on
# 3.12 (`ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`)
# -- a known kafka-python/3.12 incompatibility, found by actually running
# this container rather than assuming the latest slim tag would work.
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
ENTRYPOINT ["python3", "main.py"]
