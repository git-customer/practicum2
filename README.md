# Общее описание

В составе решения присутствует кластер Kafka и приложение messenger на Python, выполняющее функции обмена сообщениями. Приложение работает внутри контейнеров docker.
Параметры кластера Kafka заданы в файле cluster/docker-compose.yml. Кластер состоит из 3 узлов broker, 1 узла zookeeper, а также service-registry и kafka-ui.
Для сохранения состояния кластера при выключении настроены volumes на диске. Для кластера Kafka и приложения настроена общая сеть "kafka_shared_network", что позволяет отдельным контейнерам общаться между собой.
Параметры приложения Python заданы в файлах app/docker-compose.yml и app/Dockerfile.

Приложение messenger.py выполняет следующие функции:
- потоковая обработка сообщений при помощи библиотеки Faust.
- блокировка передачи сообщений от одного пользователя к другому на основе динамических списков и компонента Table. 
- предварительная цензура сообщений перед перед отправкой получателю на основе динамического списка и компонента Table.


# Разворачивание Kafka-кластера.

1. Установить docker по инструкции https://docs.docker.com/engine/install
2. Запустить контейнеры кластера Kafka. Если не указывать ключ -d, при запуске сразу будут видны логи, по которым можно убедиться в отсутствии ошибок.  
`cd cluster && sudo docker compose up -d`  
**Примечание**: Если впоследствии потребуется полностью очистить данные кластера чтобы запустить тестирование "с нуля", то можно использовать команду, удаляющую volumes:
`sudo docker compose down -v`


# Проверка работы кластера Kafka в консоли сервера.

1. Вывести список запущенных контейнеров.  
`sudo docker ps`
3. Выполнить команду внутри контейнера. В общем случае команда выглядит так:  
`docker exec -it <kafka_container_name> kafka-topics.sh --list --bootstrap-server kafka:9092`  
В образах Confluent расширение .sh часто отсутствует, поэтому итоговая команда приведена ниже. Команда должна вернуть пустой список, так как топики ещё не созданы.  
`sudo docker exec -it cluster-kafka2-1 kafka-topics --list --bootstrap-server kafka2:9092`
4. Можно проверить логи конкретного контейнера и найти там значения настроенных параметров, например папок логов.  
```
sudo docker logs cluster-kafka2-1 | grep 'log.dirs ='
sudo docker logs -f cluster-kafka1-1
```


# Проверка работы кластера Kafka через Kafka UI.

WEB-интерфейс для проверки доступен на порту 8080. В случае обращения с ПК/сервера, на котором запущен docker, адрес будет http://localhost:8080
Статус кластера должен быть online, должна отображаться информация о версии, брокерах, партициях и т.д.


# Создание топиков через консоль.

Для работы приложения необходимо создать несколько топиков с 1 партицией и фактором репликации 3. 
Топик для исходящих от пользователей сообщений:  
`sudo docker exec -it cluster-kafka1-1 kafka-topics --create --topic messages --partitions 1 --replication-factor 3 --bootstrap-server kafka1:9091`  
Топик для получения сообщений пользователями:  
`sudo docker exec -it cluster-kafka1-1 kafka-topics --create --topic filtered_messages --partitions 1 --replication-factor 3 --bootstrap-server kafka1:9091`  
Топик для блокировки пользователями других пользователей:  
`sudo docker exec -it cluster-kafka1-1 kafka-topics --create --topic blocked_users --partitions 1 --replication-factor 3 --bootstrap-server kafka1:9091`  
Топик для запрещённых цензурой слов:  
`sudo docker exec -it cluster-kafka1-1 kafka-topics --create --topic bad_words --partitions 1 --replication-factor 3 --bootstrap-server kafka1:9091`  
В результате успешного выполнения каждой команды должно появиться сообщение:  
Created topic <имя топика>.


# Добавление запрещённых слов.

Для упрощения тестирования предполагается, что пользователи будут отправлять друг другу только два запрещённых слова: "идиот", "козёл".
По-умолчанию список запрещёных слов для приложения messenger пуст.
Пополнить его можно при помощи добавления соответствующих слов в топик bad_words через UI или следующие консольные команды:  
```
echo "козёл" | sudo docker exec -i cluster-kafka1-1 kafka-console-producer --topic bad_words --bootstrap-server kafka1:9091
echo "идиот" | sudo docker exec -i cluster-kafka1-1 kafka-console-producer --topic bad_words --bootstrap-server kafka1:9091
```  
Если сделать это уже после запуска приложения, то в логах отобразится соответствующее уведомление:
```
messenger-1  | 2026-06-04 14:57:57,668 INFO: >>>>>> Слово 'идиот' добавлено в черный список. Текущий список: ['козёл', 'идиот']
```


# Запуск приложения.

Команда:  
`cd app && sudo docker compose up --build`

Примером успешного запуска будут логи в консоли о запуске контейнеров, а также отсутствие ошибок в логах приложения.  
```
[+] up 10/10
 ✔ Image app-messenger       Built                                                                                                                                                       4.5s
 ✔ Image app-client1         Built                                                                                                                                                       4.5s
 ✔ Image app-client2         Built                                                                                                                                                       4.5s
 ✔ Image app-client3         Built                                                                                                                                                       4.5s
 ✔ Image app-client4         Built                                                                                                                                                       4.5s
 ✔ Container app-messenger-1 Created                                                                                                                                                     0.7s
 ✔ Container app-client3-1   Created                                                                                                                                                     1.2s
 ✔ Container app-client4-1   Created                                                                                                                                                     1.2s
 ✔ Container app-client1-1   Created                                                                                                                                                     1.0s
 ✔ Container app-client2-1   Created                                                                                                                                                     1.2s
Attaching to client1-1, client2-1, client3-1, client4-1, messenger-1
```

# Проверка работы приложения.

Для проверки работы приложения не требуются тестовые данные, т.к. в составе решения поставляется файл client.py, эмулирующий работу пользователя.  
В docker-compose.yml для приложения и в коде настроен запуск 4 пользователей, которые пишут фиксированный набор случайных сообщений в топик messages и вычитывают сообщения из топика filtered_messages.  
В случае отправки цензурируемого слова, messenger заменяет его на "!!! Заблокировано цензурой !!!".
Если пользователь видит фразу "!!! Заблокировано цензурой !!!" в адресованном ему сообщении, то он автоматически отправляет сообщение о блокировке отправителя в топик blocked_users.
После этого messenger начинает блокировать передачу сообщений согласно поступившему запросу, выводя в консольные логи уведомления о блокировке.
Судьбу сообщений удобно отслежвать по их id.  
Далее приведён пример работы приложения.  
```
messenger-1  | [2026-06-04 14:57:57,578] [1] [INFO] [^---Recovery]: Seek stream partitions to committed offsets.
messenger-1  | [2026-06-04 14:57:57,603] [1] [INFO] HTTP Request: GET http://schema-registry:8081/associations/resources/-/messages?resourceType=topic&associationType=value "HTTP/1.1 200 OK"
messenger-1  | [2026-06-04 14:57:57,622] [1] [INFO] HTTP Request: GET http://schema-registry:8081/schemas/ids/1?subject=messages-value "HTTP/1.1 200 OK"
messenger-1  | [2026-06-04 14:57:57,631] [1] [INFO] HTTP Request: GET http://schema-registry:8081/associations/resources/-/filtered_messages?resourceType=topic&associationType=value "HTTP/1.1 200 OK"
messenger-1  | [2026-06-04 14:57:57,658] [1] [INFO] HTTP Request: POST http://schema-registry:8081/subjects/filtered_messages-value/versions?normalize=False "HTTP/1.1 200 OK"
messenger-1  | 2026-06-04 14:57:57,662 INFO: Обработано и передано сообщение с id c63f218d-6bb4-4d85-bdd0-e879ff4f4c27 от client1 для client3: !!! Заблокировано цензурой !!!
messenger-1  | [2026-06-04 14:57:57,662] [1] [INFO] Обработано и передано сообщение с id c63f218d-6bb4-4d85-bdd0-e879ff4f4c27 от client1 для client3: !!! Заблокировано цензурой !!!
messenger-1  | 2026-06-04 14:57:57,668 INFO: >>>>>> Слово 'идиот' добавлено в черный список. Текущий список: ['козёл', 'идиот']
messenger-1  | [2026-06-04 14:57:57,668] [1] [INFO] >>>>>> Слово 'идиот' добавлено в черный список. Текущий список: ['козёл', 'идиот']
messenger-1  | [2026-06-04 14:57:57,670] [1] [INFO] [^---Recovery]: Worker ready
messenger-1  | 2026-06-04 14:57:57,684 INFO: Обработано и передано сообщение с id dd5b35df-6dbe-4b1d-8fd4-4ecde2024d02 от client2 для client3: Спишь?
messenger-1  | [2026-06-04 14:57:57,684] [1] [INFO] Обработано и передано сообщение с id dd5b35df-6dbe-4b1d-8fd4-4ecde2024d02 от client2 для client3: Спишь?
messenger-1  | [2026-06-04 14:57:57,685] [1] [INFO] [^Worker]: Ready
messenger-1  | 2026-06-04 14:57:57,689 INFO: Обработано и передано сообщение с id ef42e65b-4551-4b34-b0fe-f41d0ed5dc18 от client4 для client1: Спишь?
messenger-1  | [2026-06-04 14:57:57,689] [1] [INFO] Обработано и передано сообщение с id ef42e65b-4551-4b34-b0fe-f41d0ed5dc18 от client4 для client1: Спишь?
messenger-1  | 2026-06-04 14:57:57,695 INFO: Обработано и передано сообщение с id 07850545-6c69-4bec-8655-c5e318cf613f от client3 для client4: Что делаешь?
messenger-1  | [2026-06-04 14:57:57,695] [1] [INFO] Обработано и передано сообщение с id 07850545-6c69-4bec-8655-c5e318cf613f от client3 для client4: Что делаешь?
client1-1    | 2026-06-04 14:57:58,993 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:57:59,011 INFO: Обработано и передано сообщение с id 07850545-6c69-4bec-8655-c5e318cf613f от client3 для client4: Что делаешь?
messenger-1  | [2026-06-04 14:57:59,011] [1] [INFO] Обработано и передано сообщение с id 07850545-6c69-4bec-8655-c5e318cf613f от client3 для client4: Что делаешь?
messenger-1  | 2026-06-04 14:57:59,015 INFO: Обработано и передано сообщение с id 059937d8-b37d-4cc9-8e36-fd036c4fa89b от client1 для client3: !!! Заблокировано цензурой !!!
client1-1    | 2026-06-04 14:58:19,055 INFO: Сообщение отправлено в топик messages
client1-1    | 2026-06-04 14:58:19,057 INFO: Получено сообщение с id 4d07ebd4-f91e-40f6-be3b-9a9c23cbbbe4 от client3 для client1: !!! Заблокировано цензурой !!!
client1-1    | 2026-06-04 14:58:19,058 WARNING: Пользователь client1 заблокировал пользователя client3 за нецензурную лексику
messenger-1  | 2026-06-04 14:58:19,099 INFO: Обработано и передано сообщение с id 9f1006b2-20fb-44dc-baf3-21189f9009f5 от client1 для client2: Что делаешь?
messenger-1  | [2026-06-04 14:58:19,099] [1] [INFO] Обработано и передано сообщение с id 9f1006b2-20fb-44dc-baf3-21189f9009f5 от client1 для client2: Что делаешь?
messenger-1  | 2026-06-04 14:58:19,121 WARNING: >>>>>> В messenger получны данные, что пользователь client1 заблокировал client3. Текущий список: ['client3']
messenger-1  | [2026-06-04 14:58:19,121] [1] [WARNING] >>>>>> В messenger получны данные, что пользователь client1 заблокировал client3. Текущий список: ['client3']
client2-1    | 2026-06-04 14:58:19,276 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:19,299 INFO: Обработано и передано сообщение с id a09d2184-1794-4344-8a6d-a5399fa65f0a от client2 для client4: Привет!
messenger-1  | [2026-06-04 14:58:19,299] [1] [INFO] Обработано и передано сообщение с id a09d2184-1794-4344-8a6d-a5399fa65f0a от client2 для client4: Привет!
client4-1    | 2026-06-04 14:58:19,468 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:19,486 INFO: Обработано и передано сообщение с id ae4451bd-37c6-4f76-9927-25ba38158f3b от client4 для client3: Это ты на фото?
messenger-1  | [2026-06-04 14:58:19,486] [1] [INFO] Обработано и передано сообщение с id ae4451bd-37c6-4f76-9927-25ba38158f3b от client4 для client3: Это ты на фото?
client3-1    | 2026-06-04 14:58:19,514 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:19,528 WARNING: Сообщение от client3 для client1 ЗАБЛОКИРОВАНО (отправитель в чёрном списке)
messenger-1  | [2026-06-04 14:58:19,528] [1] [WARNING] Сообщение от client3 для client1 ЗАБЛОКИРОВАНО (отправитель в чёрном списке)
client1-1    | 2026-06-04 14:58:24,088 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:24,103 INFO: Обработано и передано сообщение с id 1f4c6bc6-230c-4bf6-93b3-0fda17eba95e от client1 для client4: Это ты на фото?
messenger-1  | [2026-06-04 14:58:24,103] [1] [INFO] Обработано и передано сообщение с id 1f4c6bc6-230c-4bf6-93b3-0fda17eba95e от client1 для client4: Это ты на фото?
client2-1    | 2026-06-04 14:58:24,291 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:24,328 INFO: Обработано и передано сообщение с id e037cc1f-4eec-46d6-8a1c-2d9522676376 от client2 для client1: Что делаешь?
messenger-1  | [2026-06-04 14:58:24,328] [1] [INFO] Обработано и передано сообщение с id e037cc1f-4eec-46d6-8a1c-2d9522676376 от client2 для client1: Что делаешь?
client4-1    | 2026-06-04 14:58:24,486 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:24,506 INFO: Обработано и передано сообщение с id fc76d4a1-98e7-471c-8666-a4b877682ef0 от client4 для client2: Привет!
messenger-1  | [2026-06-04 14:58:24,506] [1] [INFO] Обработано и передано сообщение с id fc76d4a1-98e7-471c-8666-a4b877682ef0 от client4 для client2: Привет!
client3-1    | 2026-06-04 14:58:24,522 INFO: Сообщение отправлено в топик messages
client3-1    | 2026-06-04 14:58:24,523 INFO: Получено сообщение с id 5ca75d39-76c2-42b5-abdb-d285b199bbd7 от client1 для client3: !!! Заблокировано цензурой !!!
client3-1    | 2026-06-04 14:58:24,523 WARNING: Пользователь client3 заблокировал пользователя client1 за нецензурную лексику
messenger-1  | 2026-06-04 14:58:24,550 WARNING: >>>>>> В messenger получны данные, что пользователь client3 заблокировал client1. Текущий список: ['client1']
messenger-1  | [2026-06-04 14:58:24,550] [1] [WARNING] >>>>>> В messenger получны данные, что пользователь client3 заблокировал client1. Текущий список: ['client1']
messenger-1  | 2026-06-04 14:58:24,563 WARNING: Сообщение от client3 для client1 ЗАБЛОКИРОВАНО (отправитель в чёрном списке)
messenger-1  | [2026-06-04 14:58:24,563] [1] [WARNING] Сообщение от client3 для client1 ЗАБЛОКИРОВАНО (отправитель в чёрном списке)
client1-1    | 2026-06-04 14:58:29,104 INFO: Сообщение отправлено в топик messages
client1-1    | 2026-06-04 14:58:29,105 INFO: Получено сообщение с id b1c5c6fb-282b-484f-9692-bea57e39856c от client4 для client1: Спишь?
messenger-1  | 2026-06-04 14:58:29,127 INFO: Обработано и передано сообщение с id c0d60327-31f5-49bf-90cd-883f37bcefd3 от client1 для client4: Это ты на фото?
messenger-1  | [2026-06-04 14:58:29,127] [1] [INFO] Обработано и передано сообщение с id c0d60327-31f5-49bf-90cd-883f37bcefd3 от client1 для client4: Это ты на фото?
client2-1    | 2026-06-04 14:58:29,321 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:29,349 INFO: Обработано и передано сообщение с id d6c0e578-0fe3-40b5-9e53-7f1b636381ff от client2 для client3: Это ты на фото?
messenger-1  | [2026-06-04 14:58:29,349] [1] [INFO] Обработано и передано сообщение с id d6c0e578-0fe3-40b5-9e53-7f1b636381ff от client2 для client3: Это ты на фото?
client4-1    | 2026-06-04 14:58:29,511 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:29,541 INFO: Обработано и передано сообщение с id 0b21d6ec-3fd3-47e0-9208-bfffb2772dc2 от client4 для client1: Спишь?
messenger-1  | [2026-06-04 14:58:29,541] [1] [INFO] Обработано и передано сообщение с id 0b21d6ec-3fd3-47e0-9208-bfffb2772dc2 от client4 для client1: Спишь?
client3-1    | 2026-06-04 14:58:29,552 INFO: Сообщение отправлено в топик messages
messenger-1  | 2026-06-04 14:58:29,609 INFO: Обработано и передано сообщение с id d3b1c18c-e494-4f41-8741-914621a9dbdd от client3 для client2: !!! Заблокировано цензурой !!!
messenger-1  | [2026-06-04 14:58:29,609] [1] [INFO] Обработано и передано сообщение с id d3b1c18c-e494-4f41-8741-914621a9dbdd от client3 для client2: !!! Заблокировано цензурой !!!
```
