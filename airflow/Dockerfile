# Servicio airflow (P4) en modo standalone.
#
# Las dependencias del proyecto van a un venv propio y no al Python de
# Airflow: Airflow instala con su fichero de restricciones y fijar ahí
# pandas 3 o SQLAlchemy 2 acaba en conflictos del resolutor. Los DAGs
# ejecutan el código de common/ y archivado/ con /opt/pids-venv/bin/python.
FROM apache/airflow:slim-3.3.2-python3.12

USER root
# /opt/airflow/db se crea aquí con dueño airflow para que el volumen con
# nombre que monta Compose herede el permiso; si no, SQLite no puede escribir.
RUN python -m venv /opt/pids-venv \
 && mkdir -p /opt/airflow/db \
 && chown -R airflow: /opt/pids-venv /opt/airflow/db
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN /opt/pids-venv/bin/pip install --no-cache-dir -r /tmp/requirements.txt

CMD ["standalone"]
