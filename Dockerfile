FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

# Download PostgreSQL JDBC driver 
RUN wget -O /tmp/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# Set environment variables for Spark  JDBC driver
ENV SPARK_CLASSPATH=/tmp/postgresql-42.6.0.jar
ENV PYSPARK_SUBMIT_ARGS="--driver-class-path /tmp/postgresql-42.6.0.jar --jars /tmp/postgresql-42.6.0.jar pyspark-shell"

WORKDIR /app

# Copy poetry files
COPY pyproject.toml poetry.lock ./

# Install poetry and dependencies
RUN pip install poetry && \
    poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-root

#  application code
COPY . .

CMD ["python", "main.py"]