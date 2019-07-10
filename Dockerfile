FROM python:3.6

ENV APP_NAME=pysoni

RUN pip install pipenv

# Install dependencies before copy source file to cache dependencies
RUN mkdir -p /${APP_NAME}/
WORKDIR /${APP_NAME}/
COPY ./Pipfile* ./
ENV PIPENV_MAX_RETRIES=3
ENV PIPENV_TIMEOUT=600
RUN pipenv install --system --deploy --ignore-pipfile --dev
