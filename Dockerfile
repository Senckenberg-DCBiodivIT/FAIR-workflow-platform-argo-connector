FROM python:3.12.3

WORKDIR /

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade -r requirements.txt

COPY ./app /app

CMD ["fastapi", "run", "app/main.py", "--port", "80"]
