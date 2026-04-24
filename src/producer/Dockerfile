FROM python:3.11-slim

WORKDIR /app

ENV PYTHONPATH="/protos:${PYTHONPATH}"

RUN apt-get update && apt-get install -y --no-install-recommends \
curl gcc protobuf-compiler \
&& rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY --from=schemas movie_event.proto .
RUN protoc --python_out=. movie_event.proto

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
