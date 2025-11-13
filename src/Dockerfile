FROM apache/airflow:2.9.1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY airflow-movielens /opt/airflow/packages/airflow-movielens

USER root

RUN chown -R airflow: /opt/airflow/packages/airflow-movielens

USER airflow
RUN pip install --no-cache-dir --no-build-isolation /opt/airflow/packages/airflow-movielens

# ENTRYPOINT ["tini", "--", "/entrypoint"] <-- 컨테이너가 시작할 때 실행할 명렁어
