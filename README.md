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

## Orquestacion manual y Neo4j

El DAG manual de la parte final esta en [dags/fraud_neo4j_orchestration.py](dags/fraud_neo4j_orchestration.py). Hace tres cosas en cadena:

1. Compacta la tabla Iceberg indicada con `ALTER TABLE ... EXECUTE optimize`.
2. Exporta un subgrafo desde Trino a ficheros CSV temporales.
3. Carga nodos y relaciones en Neo4j por HTTP.

Parametros aceptados en el disparo manual:

```json
{
	"source_table": "gold.payment_relations",
	"start_ts": "2024-01-01T00:00:00Z",
	"end_ts": "2099-12-31T23:59:59Z",
	"graph_name": "fraud_graph"
}
```

## Consultas Cypher utiles

Dispositivos compartidos por varias tarjetas:

```cypher
MATCH (d:Device)<-[:FROM_DEVICE]-(p:Payment)-[:USES_CARD]->(c:Card)
WHERE p.graph_name = $graph_name
WITH d, count(DISTINCT c) AS cards
WHERE cards > 1
RETURN d.device_id AS device_id, cards
ORDER BY cards DESC;
```

Tarjetas usadas en varios paises:

```cypher
MATCH (c:Card)<-[:USES_CARD]-(p:Payment)-[:IN_COUNTRY]->(country:Country)
WHERE p.graph_name = $graph_name
WITH c, count(DISTINCT country) AS countries
WHERE countries > 1
RETURN c.card_id AS card_id, countries
ORDER BY countries DESC;
```

Comercios conectados a mas actividad sospechosa:

```cypher
MATCH (m:Merchant)<-[:AT_MERCHANT]-(p:Payment)
WHERE p.graph_name = $graph_name AND coalesce(p.risk_score, 0) >= 25
WITH m, count(*) AS alerts, collect(DISTINCT p.card_id) AS cards
RETURN m.merchant_id AS merchant_id, alerts, size(cards) AS distinct_cards
ORDER BY alerts DESC;
```

