# Conexion SQLAlchemy a Trino

Catalogo Trino configurado: `iceberg`.

## Reset desde cero

Si quieres "nukear" el entorno y comprobar el flujo completo desde un estado limpio, usa esta secuencia desde la raiz del proyecto:

```bash
docker compose down --volumes --remove-orphans
docker compose up -d --build
```

Si quieres dejar el estado aun mas limpio antes de arrancar, elimina tambien caches locales del proyecto si existen, por ejemplo:

```bash
rm -rf .pytest_cache .ipynb_checkpoints
```

Despues del arranque, valida en este orden:

1. `scripts/generar_datos.py`
2. `notebooks/01_bronze.ipynb`
3. `notebooks/02_silver.ipynb`
4. `notebooks/03_gold.ipynb`

## Reproducibilidad en otros entornos

El stack esta pensado para ser reproducible en otra maquina o workspace si mantienes constantes estas piezas:

- `docker-compose.yml` con los mismos servicios, puertos y volumenes.
- Las variables de entorno de Kafka, Iceberg, MinIO y Spark.
- La semilla o estrategia de generacion de datos en `scripts/generar_datos.py`.
- El orden de ejecucion de los notebooks Bronze -> Silver -> Gold.

Con la configuracion actual, puedes esperar el mismo comportamiento funcional, aunque el numero exacto de alertas puede variar si cambian la ventana temporal, la aleatoriedad del generador o el volumen de eventos por ejecucion.

## URI recomendadas

- Desde tu maquina (host): `trino://trino@localhost:8080/iceberg`
- Desde otro contenedor de Docker en la misma red: `trino://trino@trino:8080/iceberg`

## URI por esquema (datos de notebooks)

- Bronze: `trino://trino@localhost:8080/iceberg/bronze`
- Silver: `trino://trino@localhost:8080/iceberg/silver`
- Gold: `trino://trino@localhost:8080/iceberg/gold`

Los notebooks generan tablas en esas capas. Ejemplo esperado en Gold:

- `iceberg.gold.fraud_alerts`

## Ejemplo rapido en Python

```python
from sqlalchemy import create_engine, text

engine = create_engine("trino://trino@localhost:8080/iceberg/gold")

with engine.connect() as conn:
	rows = conn.execute(text("SELECT * FROM fraud_alerts LIMIT 10")).fetchall()
	print(rows)
```

Si aun no tienes el driver, instala:

```bash
pip install sqlalchemy-trino
```
