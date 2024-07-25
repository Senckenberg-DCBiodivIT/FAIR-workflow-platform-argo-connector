FROM python:3.12.3-slim

RUN apt update -y \
  && apt install -y git \
  && apt install -y libmagic-dev

WORKDIR /

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./app /app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--log-level", "debug"]
