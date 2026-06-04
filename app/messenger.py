import faust
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer, JSONSerializer
from confluent_kafka.serialization import StringSerializer, StringDeserializer, SerializationContext, MessageField
import json
import asyncio
import logging

# Настройка логирования
logger = logging.getLogger("messenger")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
# Для консоли
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Конфигурация Schema Registry
schema_registry_config = {
    'url': 'http://schema-registry:8081'
}
# Инициализация клиента Schema Registry
schema_registry_client = SchemaRegistryClient(schema_registry_config)

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

# Создание JSON-десериализатора
json_deserializer = JSONDeserializer(schema_str=json_schema_str, from_dict=None, schema_registry_client=schema_registry_client)
# Определение десериализации ключа и значения
key_deserializer = StringDeserializer('utf-8')
value_deserializer = json_deserializer
# Создание JSON-сериализатора
json_serializer = JSONSerializer(json_schema_str, schema_registry_client)
# Определение сериализации ключа и значения
key_serializer = StringSerializer('utf-8')
value_serializer = json_serializer

# Конфигурация Faust-приложения
app = faust.App(
    "messenger-faust-app",
    broker="kafka1:9091,kafka2:9092,kafka3:9093",
    value_serializer="raw", # Работа с байтами (default: "json")
    store="rocksdb://"
)

# Определение топика для входных данных (с байтами)
input_topic = app.topic("messages", key_type=str, value_type=bytes)
# Определение топика для выходных данных (с байтами)
output_topic = app.topic("filtered_messages", key_type=str, value_type=bytes)
# Определение топика для заблокированных пользователей
blocked_users_topic = app.topic("blocked_users", value_type=str)
# Определение топика для запрещённых слов
bad_words_topic = app.topic("bad_words", value_type=str)

# Определение таблицы для списка запрещённых слов
bad_words_table = app.Table(
    "dynamic_bad_words",
    partitions=1,   # Количество партиций
    default=list    # Пустой список по-умолчанию
)
# Имя ключа в таблице запрещённых слов
WORDS_KEY = "current_blacklist"

# Агент, который читает топик запрещённых слов и наполняет соответствующую таблицу
@app.agent(bad_words_topic)
async def process_bad_words_updates(stream):
    async for event in stream:
        try:
            # Получаем входящее сообщение
            word = event
            if not word:
                continue
            # Получаем текущий список в таблице
            current_words = bad_words_table[WORDS_KEY]
            # Добавляем слово в таблицу, если его там ещё нет
            if word not in current_words:
                current_words.append(word)
                # Обновляем таблицу
                bad_words_table[WORDS_KEY] = current_words
                logger.info(f">>>>>> Слово '{word}' добавлено в черный список. Текущий список: {current_words}")
        except Exception as e:
            logger.error(f">>>>>> Ошибка при обновлении списка запрещенных слов: {e}")


# Определение таблицы для списка блокировок пользователей
blocked_users_table = app.Table(
    "blocked_users_list",
    partitions=1,   # Количество партиций
    default=list    # Пустой список по-умолчанию
)

# Агент, который читает топик заблокированных пользователей и наполняет соответствующую таблицу
@app.agent(blocked_users_topic)
async def process_user_blocks(stream):
    async for event in stream:
        try:
            # Получаем входящее сообщение
            data = json.loads(event)
            user = data.get("user")
            user_to_block = data.get("blocks")
            if user and user_to_block:
                # Получаем текущий список в таблице для пользователя
                current_blacklist = blocked_users_table[user]
                # Добавляем пользователя в черный список, если его там еще нет
                if user_to_block not in current_blacklist:
                    current_blacklist.append(user_to_block)
                    # Обновляем таблицу
                    blocked_users_table[user] = current_blacklist
                    logger.warning(f">>>>>> В messenger получны данные, что пользователь {user} заблокировал {user_to_block}. Текущий список: {blocked_users_table[user]}")
        except Exception as e:
            logger.error(f">>>>>> Ошибка при обновлении списка заблокированных пользователей: {e}")


# Функция, реализующая потоковую обработку данных
@app.agent(input_topic)
async def process(stream):
    async for received_value in stream:
        try:
            # Получение сообщения
            json_message = value_deserializer(received_value, SerializationContext(input_topic.get_topic_name(), MessageField.VALUE))
            # Обработка данных
            sender = json_message.get("from")
            recipient = json_message.get("to")
            msg_body = json_message.get("msg_body", "")

            # ПРОВЕРКА НА БЛОКИРОКУ ПОЛЬЗОВАТЕЛЯ
            if sender in blocked_users_table[recipient]:
                logger.warning(f"Сообщение от {sender} для {recipient} ЗАБЛОКИРОВАНО (отправитель в чёрном списке)")
                # Переходим на следущий цикл без пересылки сообщения в исходящий топик
                continue

            # ПРОВЕРКА НА ЦЕНЗУРУ
            # Получаем актуальный список слов из таблицы
            active_bad_words = bad_words_table[WORDS_KEY]
            # Если в тексте сообщения есть слово из черного списка - цензурируем его
            if active_bad_words and any(word in msg_body.lower() for word in active_bad_words):
                json_message["msg_body"] = "!!! Заблокировано цензурой !!!"

            # Отправка обработанного сообщения в выходной топик
            processed_value = value_serializer(json_message, SerializationContext(output_topic.get_topic_name(), MessageField.VALUE))
            logger.info(f"Обработано и передано сообщение с id {json_message['id']} от {json_message['from']} для {json_message['to']}: {json_message['msg_body']}")
            await output_topic.send(value=processed_value)
        except Exception as e:
            logger.error(f"Критическая ошибка в работе messenger: {e}")
