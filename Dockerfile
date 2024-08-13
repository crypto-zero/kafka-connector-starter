FROM python:3.12-alpine
RUN pip install poetry
WORKDIR /app
COPY pyproject.toml poetry.lock /app/
RUN poetry install --no-root
COPY . /app
ENTRYPOINT ["poetry", "run", "python", "kafka_connector_starter/main.py"]