FROM astrocrpublic.azurecr.io/runtime:3.0-6

# create dbt virtual environment
RUN python -m venv /usr/local/airflow/dbt_venv && \
    . /usr/local/airflow/dbt_venv/bin/activate && \
    pip install --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
        "protobuf==4.25.3" \
        "dbt-core==1.7.0" \
        "dbt-mysql==1.7.0" \
        "importlib-metadata<7" \
        "setuptools>=65.0.0" && \
    deactivate