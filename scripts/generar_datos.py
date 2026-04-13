"""
Generador de eventos de pago para Kafka - PARTE 1
Genera eventos realistas de transacciones con patrones normales y sospechosos.
"""

import os
import json
import time
import random
from typing import Optional
from datetime import datetime, timezone, timedelta
from uuid import uuid4

from confluent_kafka import Producer
from loguru import logger
from faker import Faker
from dotenv import load_dotenv

# Cargar variables de entorno desde .env
load_dotenv()

# Configuración de logging
logger.remove()
logger.add(
    lambda msg: print(msg, end=""),
    format="<green>[{time:HH:mm:ss}]</green> {message}",
)


class PaymentEventGenerator:
    """Generador de eventos de pago realistas para Kafka."""

    def __init__(self, bootstrap_servers="localhost:9092", topic="payment-events"):
        """
        Args:
            bootstrap_servers: Dirección del broker de Kafka (ej: 'kafka:9092')
            topic: Nombre del topic donde publicar los eventos
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.faker = Faker()
        self.events_sent = 0
        # Permite simular historico temporal para analitica en series temporales.
        self.randomize_event_time = (
            os.getenv("RANDOMIZE_EVENT_TIME", "true").lower() == "true"
        )
        self.event_lookback_days = int(os.getenv("EVENT_TIME_LOOKBACK_DAYS", "90"))

        # Pools de IDs predefinidos
        self.customers = [f"CUST_{i:05d}" for i in range(1, 51)]
        self.cards = [f"CARD_{i:05d}" for i in range(1, 200)]
        self.merchants = [f"MERCH_{i:05d}" for i in range(1, 100)]
        self.devices = [f"DEV_{i:05d}" for i in range(1, 100)]
        # Perfil base por tarjeta para que el trafico normal no parezca siempre anomalo.
        self.card_primary_device = {
            card_id: random.choice(self.devices) for card_id in self.cards
        }
        self.card_home_country = {
            card_id: self.faker.country_code() for card_id in self.cards
        }
        self.currencies = ["USD", "EUR", "GBP", "AUD", "CAD"]
        self.mcc_codes = [
            "5411",  # Grocery stores
            "5812",  # Restaurants
            "5942",  # Book stores
            "7011",  # Accommodations
            "4112",  # Passenger railways
            "6211",  # Securities brokers
        ]

        # Crear producer Kafka con confluent-kafka
        logger.info(f"📡 Conectando a Kafka: {bootstrap_servers}...")
        conf = {
            "bootstrap.servers": bootstrap_servers,
            "broker.address.family": "v4",  # Preferir IPv4
            "socket.timeout.ms": 30000,
            "client.id": "payment-generator",
            "api.version.request.timeout.ms": 5000,
        }

        try:
            self.producer = Producer(conf)
            logger.info("✓ Conexión a Kafka establecida\n")
        except Exception as e:
            logger.error(f"✗ No se pudo conectar a Kafka: {e}")
            raise

    def delivery_report(self, err, msg):
        """Callback para confirmar entrega de mensajes."""
        if err is not None:
            logger.error(f"✗ Error al enviar: {err}")
        else:
            self.events_sent += 1
            logger.info(
                f"✓ {msg.key().decode('utf-8') if msg.key() else 'EVENT'} | "
                f"Enviado a {msg.topic()} [{msg.partition()}]"
            )


    def generate_payment_event(
        self,
        payment_id=None,
        customer_id=None,
        card_id=None,
        merchant_id=None,
        device_id=None,
        country=None,
        amount=None,
        status="approved",
    ):
        """Genera un evento de pago individual con los 12 campos requeridos."""
        if payment_id is None:
            payment_id = str(uuid4())
        if customer_id is None:
            customer_id = random.choice(self.customers)
        if card_id is None:
            card_id = random.choice(self.cards)
        if merchant_id is None:
            merchant_id = random.choice(self.merchants)

        event = {
            "event_time": self.sample_event_time(),
            "payment_id": payment_id,
            "customer_id": customer_id,
            "card_id": card_id,
            "merchant_id": merchant_id,
            "device_id": device_id or random.choice(self.devices),
            "ip": self.faker.ipv4(),
            "country": country or self.faker.country_code(),
            "amount": round(amount if amount is not None else random.uniform(10, 500), 2),
            "currency": random.choice(self.currencies),
            "status": status,
            "mcc": random.choice(self.mcc_codes),
        }
        return event

    def sample_event_time(self) -> str:
        """Genera marca temporal UTC actual o aleatoria dentro de una ventana pasada."""
        if not self.randomize_event_time:
            dt = datetime.now(timezone.utc)
        else:
            now_utc = datetime.now(timezone.utc)
            max_seconds = max(1, self.event_lookback_days * 24 * 60 * 60)
            random_offset = random.randint(0, max_seconds)
            dt = now_utc - timedelta(seconds=random_offset)

        return dt.isoformat().replace("+00:00", "Z")

    def sample_normal_device_for_card(self, card_id: str) -> str:
        """La mayoria de pagos de una tarjeta se hacen desde su dispositivo habitual."""
        primary_device = self.card_primary_device[card_id]
        if random.random() < 0.9:
            return primary_device
        return random.choice(self.devices)

    def sample_country_for_card(self, card_id: str, force_foreign: bool = False) -> str:
        """Mantiene pais habitual, salvo cuando se fuerza comportamiento anomalo."""
        home_country = self.card_home_country[card_id]
        if not force_foreign and random.random() < 0.92:
            return home_country

        foreign_country = self.faker.country_code()
        while foreign_country == home_country:
            foreign_country = self.faker.country_code()
        return foreign_country

    def sample_amount(self) -> float:
        """Distribucion sesgada a importes medios-bajos con cola alta ocasional."""
        if random.random() < 0.97:
            return random.uniform(10, 320)
        return random.uniform(320, 800)

    def publish_event(self, event):
        """Publica un evento a Kafka de forma síncrona."""
        try:
            self.producer.produce(
                topic=self.topic,
                key=event["payment_id"].encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
                callback=self.delivery_report,
            )
            # Flush inmediato para asegurar envío
            self.producer.flush()
        except Exception as e:
            logger.error(f"✗ Error al enviar evento: {e}")


    def generate_normal_payment(self):
        """Genera un pago normal (aprobado)."""
        card_id = random.choice(self.cards)
        event = self.generate_payment_event(
            card_id=card_id,
            device_id=self.sample_normal_device_for_card(card_id),
            country=self.sample_country_for_card(card_id),
            amount=self.sample_amount(),
            status="approved",
        )
        self.publish_event(event)

    def generate_retry_sequence(self):
        """Simula reintentos: 2-3 pagos rechazados de la misma tarjeta seguidos."""
        customer_id = random.choice(self.customers)
        card_id = random.choice(self.cards)
        payment_base_id = str(uuid4())

        retries = 2
        for i in range(retries):
            event = self.generate_payment_event(
                payment_id=f"{payment_base_id}_retry_{i}",
                customer_id=customer_id,
                card_id=card_id,
                device_id=self.sample_normal_device_for_card(card_id),
                country=self.sample_country_for_card(card_id),
                amount=self.sample_amount(),
                status="declined",
            )
            self.publish_event(event)
            time.sleep(0.5)

    def generate_high_frequency_sequence(self):
        """Simula alta frecuencia: múltiples pagos de la misma tarjeta en < 5 segundos."""
        card_id = random.choice(self.cards)
        customer_id = random.choice(self.customers)

        num_transactions = random.randint(3, 5)
        for _ in range(num_transactions):
            event = self.generate_payment_event(
                customer_id=customer_id,
                card_id=card_id,
                device_id=self.sample_normal_device_for_card(card_id),
                country=self.sample_country_for_card(card_id),
                amount=self.sample_amount(),
                status=random.choice(["approved"] * 5 + ["declined"]),
            )
            self.publish_event(event)
            time.sleep(0.2)

    def generate_multi_country_sequence(self):
        """Simula transacciones en múltiples países en poco tiempo (sospechoso)."""
        card_id = random.choice(self.cards)
        customer_id = random.choice(self.customers)

        num_countries = random.randint(2, 3)
        for _ in range(num_countries):
            event = self.generate_payment_event(
                customer_id=customer_id,
                card_id=card_id,
                device_id=self.sample_normal_device_for_card(card_id),
                country=self.sample_country_for_card(card_id, force_foreign=True),
                amount=self.sample_amount(),
                status="approved",
            )
            self.publish_event(event)
            time.sleep(1.0)

    def generate_multi_merchant_sequence(self):
        """Simula uso de la misma tarjeta en varios comercios en poco tiempo."""
        card_id = random.choice(self.cards)
        customer_id = random.choice(self.customers)

        num_merchants = random.randint(2, 4)
        merchant_ids = random.sample(self.merchants, k=num_merchants)

        for merchant_id in merchant_ids:
            event = self.generate_payment_event(
                customer_id=customer_id,
                card_id=card_id,
                merchant_id=merchant_id,
                device_id=self.sample_normal_device_for_card(card_id),
                country=self.sample_country_for_card(card_id),
                amount=self.sample_amount(),
                status=random.choice(["approved"] * 4 + ["declined"]),
            )
            self.publish_event(event)
            time.sleep(0.7)

    def run(self, num_events: Optional[int] = 1000, delay=0.1):
        """
        Ejecuta el generador de eventos.

        Args:
            num_events: Número total de eventos a generar (None = infinito)
            delay: Delay en segundos entre eventos
        """
        logger.info(f"🚀 Iniciando generador de eventos de pago")
        logger.info(f"   Bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"   Topic: {self.topic}")
        if num_events:
            logger.info(f"   Eventos a generar: {num_events}")
        else:
            logger.info("   Modo: continuo (hasta Ctrl+C)")
        logger.info(f"   Delay: {delay}s\n")

        events_generated = 0

        try:
            while num_events is None or events_generated < num_events:
                scenario_roll = random.random()

                # 92% pagos normales
                if scenario_roll < 0.92:
                    self.generate_normal_payment()

                # 3% reintentos sospechosos
                elif scenario_roll < 0.95:
                    logger.warning("⚠️  [RETRY SEQUENCE]")
                    self.generate_retry_sequence()

                # 2% alta frecuencia
                elif scenario_roll < 0.97:
                    logger.warning("⚠️  [HIGH FREQUENCY]")
                    self.generate_high_frequency_sequence()

                # 2% múltiples países
                elif scenario_roll < 0.99:
                    logger.warning("⚠️  [MULTI-COUNTRY]")
                    self.generate_multi_country_sequence()

                # 1% múltiples comercios
                else:
                    logger.warning("⚠️  [MULTI-MERCHANT]")
                    self.generate_multi_merchant_sequence()

                events_generated += 1

                # Mostrar progreso cada 50 eventos
                if events_generated % 50 == 0:
                    logger.info(f"📊 Progreso: {events_generated} eventos enviados")

                time.sleep(delay)

        except KeyboardInterrupt:
            logger.info("\n⏸️  Generador interrumpido por el usuario")
        finally:
            logger.info("Cerrando producer...")
            self.producer.flush()
            logger.success(
                f"✅ Generador finalizado. {self.events_sent} eventos publicados."
            )



def main():
    """Función principal - genera eventos de pago de forma continua."""
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.getenv("KAFKA_TOPIC_PAYMENTS", "payment-events")

    generator = PaymentEventGenerator(bootstrap_servers, topic)
    # Ejecutar indefinidamente (hasta Ctrl+C)
    generator.run(num_events=None, delay=0.1)


if __name__ == "__main__":
    main()
