FROM astrocrpublic.azurecr.io/runtime:3.3-12

ENV PYTHONPATH="/usr/local/airflow/src:${PYTHONPATH}"

COPY src /usr/local/airflow/src