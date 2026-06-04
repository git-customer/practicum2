from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import IntegerSerializer, IntegerDeserializer, StringSerializer, StringDeserializer, SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONSerializer, JSONDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import json
import time
import logging
import random
import uuid
import socket

# Получаем имя данного клиента из параметра hostname в docker-compose.yml
client_name = socket.gethostname()
# Формирование списока доступных для отправки клиентов
all_clients = ["client1", "client2", "client3", "client4"]
available_clients = [c for c in all_clients if c != client_name]
# Варианты сообщений
msg_variants = ["Привет!", "Спишь?", "Как дела?", "Что делаешь?", "Это ты на фото?", "Пока", "Идиот", "Козёл"]
# Топик для заблокированных пользователей
blocks_topic = "blocked_users"

# Настройка логирования
logger = logging.getLogger(client_name)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
# Для консоли
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
# Для сохранения в файл
#log_filename = f"/app/logs/client.log"
#file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
#file_handler.setFormatter(formatter)
#logger.addHandler(file_handler)
# ======================
# Конфигурация продюсера
producer_config = {
    "bootstrap.servers": "kafka1:9091,kafka2:9092,kafka3:9093",
    # Гарантия доставки
    "acks": "all",
    # Повторы отправки при сетевых сбоях
    "retries": 5,
    # Пауза между повторными попытками в мc
    "retry.backoff.ms": 200
}
# Создание продюсера
producer = Producer(producer_config)
# Имя топика для отправки сообщений
output_topic = "messages"

# Конфигурация Schema Registry
schema_registry_config = {
   'url': 'http://schema-registry:8081'
}

# Определение JSON-схемы
json_schema_str = """
{
 "$schema": "http://json-schema.org/draft-07/schema#",
 "title": "Message",
 "type": "object",
 "properties": {
   "id": {
     "type": "string"
   },
   "from": {
     "type": "string"
   },
   "to": {
     "type": "string"
   },
   "msg_body": {
     "type": "string"
   }
 },
 "required": ["id", "from", "to"]
}
"""

# Инициализация клиента Schema Registry
schema_registry_client = SchemaRegistryClient(schema_registry_config)

# Создание JSON-сериализатора
json_serializer = JSONSerializer(json_schema_str, schema_registry_client)
# Определение сериализации ключа и значения
key_serializer = StringSerializer('utf-8')
value_serializer = json_serializer

# Функция обратного вызова для подтверждения доставки
def delivery_report(err, msg):
   if err is not None:
       #print(f"Сбой доставки сообщения: {err}", flush=True)
       logger.error(f"Сбой отправки сообщения: {err}")
   else:
       #print(f"Сообщение доставлено в топик {msg.topic()} и партицию {msg.partition()}", flush=True)
       logger.info(f"Сообщение отправлено в топик {msg.topic()}")

# ====================
# Настройка консьюмера
consumer_config = {
    "bootstrap.servers": "kafka1:9091,kafka2:9092,kafka3:9093",
    # Отдельная группа для этого типа консьюмера
    "group.id": client_name + "-consumer-group",
    "auto.offset.reset": "earliest",
    # Настройка для ручного коммита сообщений
    "enable.auto.commit": False,
    # Настройки размера вычитки и максимального времени ожидания для вычитки по 1 сообщению
    "fetch.min.bytes": 1,
    "fetch.wait.max.ms": 100
}
# Создание консьюмера
consumer = Consumer(consumer_config)
input_topic = "filtered_messages"
# Подписка на топик
consumer.subscribe([input_topic])

# Создание JSON-десериализатора
json_deserializer = JSONDeserializer(schema_str=json_schema_str, from_dict=None, schema_registry_client=schema_registry_client)
# Определение десериализации ключа и значения
key_deserializer = StringDeserializer('utf-8')
value_deserializer = json_deserializer

# Отправка и получение сообщений в бесконечном цикле
logger.info(f"{client_name} запущен и начинает работу")
try:
    while True:
        # Искусственная задержка для удобства отслеживания логов работы приложения в консоли в реальном времени
        time.sleep(5)

        # Формирование сообщения для отправки случайным образом
        message_key = client_name
        message_value = {"id": str(uuid.uuid4()), "from": client_name, "to": random.choice(available_clients), "msg_body": random.choice(msg_variants)}
        # Отправка сообщения
        producer.produce(
            # Отправка
            topic = output_topic,
            key = key_serializer(message_key, SerializationContext(output_topic, MessageField.KEY)),
            value = value_serializer(message_value, SerializationContext(output_topic, MessageField.VALUE)),
            on_delivery = delivery_report
        )
        producer.poll(0)

        # Получение сообщения
        msg = consumer.poll(0.1)
        if msg is None:
            continue
        if msg.error():
            logger.error(f"Ошибка при получении: {msg.error()}")
            continue
        key = key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
        json_message = value_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if json_message["to"] == client_name:
            logger.info(f"Получено сообщение с id {json_message['id']} от {json_message['from']} для {json_message['to']}: {json_message['msg_body']}")
            # Если получено зацензурированное сообщение, то эмулируем блокировку отправителя текущим пользователем
            if "заблокировано цензурой" in json_message["msg_body"].lower():
                blocks_key = client_name
                blocks_value = {"user": client_name, "blocks": json_message["from"]}
                producer.produce(
                    topic = blocks_topic,
                    key = blocks_key, #StringSerializer(blocks_key, SerializationContext(blocks_topic, MessageField.KEY)),
                    value = json.dumps(blocks_value) #StringSerializer(blocks_value, SerializationContext(blocks_topic, MessageField.VALUE))
                )
                producer.poll(0)
                logger.warning(f"Пользователь {json_message['to']} заблокировал пользователя {json_message['from']} за нецензурную лексику")
        #else:
            #logger.info(f"Получено чужое сообщение для {json_message['to']}, игнорируем")
        # Ручной коммит после обработки сообщения
        consumer.commit(msg, asynchronous=False)

except Exception as e:
    logger.error(f"Произошла критическая ошибка: {e}")
finally:
    # Ожидание завершения отправки всех сообщений
    producer.flush()
    # Закрытие консьюмера
    consumer.close()
