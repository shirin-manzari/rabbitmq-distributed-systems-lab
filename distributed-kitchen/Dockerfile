FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
ARG PIP_INDEX_URL=https://pypi.org/simple
RUN pip install --no-cache-dir --index-url "$PIP_INDEX_URL" -r requirements.txt
COPY api.py contracts.py messaging.py state.py worker.py store.py runtime.py supervisor.py ./
COPY web ./web
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
