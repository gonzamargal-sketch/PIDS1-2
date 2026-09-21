"""
PIDS Parte 2 — Configuración común.

Un único sitio donde se leen las variables de entorno. Todos los
componentes importan de aquí para que nadie invente su propio nombre
de variable.
"""

import os
from dataclasses import dataclass, field


def _b(nombre: str, defecto: bool = False) -> bool:
    return os.getenv(nombre, str(defecto)).strip().lower() in ("1", "true", "si", "sí", "yes")


def _i(nombre: str, defecto: int) -> int:
    try:
        return int(os.getenv(nombre, defecto))
    except ValueError:
        return defecto


def _f(nombre: str, defecto: float) -> float:
    try:
        return float(os.getenv(nombre, defecto))
    except ValueError:
        return defecto


@dataclass(frozen=True)
class Postgres:
    host: str = os.getenv("POSTGRES_HOST", "localhost")
    port: int = _i("POSTGRES_PORT", 5432)
    db: str = os.getenv("POSTGRES_DB", "pids")
    user: str = os.getenv("POSTGRES_USER", "pids")
    password: str = os.getenv("POSTGRES_PASSWORD", "pids_dev_2026")

    @property
    def dsn(self) -> str:
        # Escapamos usuario y contraseña: si alguien pone una contraseña con
        # @, / o : el DSN se rompería sin dar una pista clara del motivo.
        from urllib.parse import quote_plus
        return (f"postgresql://{quote_plus(self.user)}:{quote_plus(self.password)}"
                f"@{self.host}:{self.port}/{self.db}")

    @property
    def kwargs(self) -> dict:
        return {"host": self.host, "port": self.port, "dbname": self.db,
                "user": self.user, "password": self.password}


@dataclass(frozen=True)
class Minio:
    endpoint: str = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key: str = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key: str = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin_dev_2026")
    bucket_bronze: str = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
    bucket_lakehouse: str = os.getenv("MINIO_BUCKET_LAKEHOUSE", "lakehouse")

    @property
    def url(self) -> str:
        return f"http://{self.endpoint}"


@dataclass(frozen=True)
class Redis:
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = _i("REDIS_PORT", 6379)
    ttl_metricas: int = _i("REDIS_TTL_METRICAS", 300)
    stream_maxlen: int = _i("REDIS_STREAM_MAXLEN", 10_000)


@dataclass(frozen=True)
class Iceberg:
    catalogo: str = os.getenv("ICEBERG_CATALOG", "pids_catalog")
    namespace: str = os.getenv("ICEBERG_NAMESPACE", "lakehouse")
    tabla: str = os.getenv("ICEBERG_TABLA", "trips")
    compresion: str = os.getenv("ICEBERG_COMPRESION", "zstd")
    # Sobrescribe el almacén. Vacío = MinIO (lo normal). Se usa en los
    # tests para apuntar a un file:// local y no necesitar MinIO.
    warehouse: str = os.getenv("ICEBERG_WAREHOUSE", "")

    @property
    def identificador(self) -> str:
        return f"{self.namespace}.{self.tabla}"


@dataclass(frozen=True)
class Kafka:
    bootstrap: str = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    topic_trips: str = os.getenv("KAFKA_TOPIC_TRIPS", "trips.raw")


@dataclass(frozen=True)
class Simulador:
    semilla_csv: str = os.getenv("SIMULADOR_SEMILLA_CSV", "datos/muestra_1000.csv")
    eventos_por_segundo: int = _i("SIMULADOR_EVENTOS_POR_SEGUNDO", 200)
    pct_suciedad_extra: float = _f("SIMULADOR_PCT_SUCIEDAD_EXTRA", 1.0)
    # Sin jitter las filas se repiten idénticas y la codificación por
    # diccionario de Parquet infla el ratio de compresión a ~35x, cuando
    # lo real con datos distintos es ~7,5x. Las métricas dejarían de ser
    # defendibles. No lo desactivéis salvo para depurar.
    jitter: bool = _b("SIMULADOR_JITTER", True)
    seed: int = _i("SIMULADOR_SEED", 42)


PG = Postgres()
MINIO = Minio()
REDIS = Redis()
ICEBERG = Iceberg()
KAFKA = Kafka()
SIMULADOR = Simulador()

# Solo para arrancar: la política real vive en la tabla retention_policy
RETENCION_CALIENTE_DIAS_DEFECTO = _i("RETENCION_CALIENTE_DIAS", 30)
PARTICIONES_ADELANTO_DIAS = _i("PARTICIONES_ADELANTO_DIAS", 7)
