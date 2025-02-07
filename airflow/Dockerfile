FROM apache/airflow:2.9.3

ENV PIP_USER=false

RUN pip install -r requirements.txt

WORKDIR "/opt/airflow"

COPY dbt-requirements.txt ./

RUN python -m venv dbt_venv && source dbt_venv/bin/activate && \
    pip install --no-cache-dir -r dbt-requirements.txt && deactivate

ENV PIP_USER=true