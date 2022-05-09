1) как сделать запуск без клиента докера?
2) что такое mySql?
3) как прокинуть папки в файловую систему?
4) какая джава работает тут? на какой джаве написан Флинк?
5) как посмотреть Лаг в Флинке? Правильно я понимаю, что если есть отставание, то можно врубить больше нод и больше слотов?
6)  cd /tmp/flink-output/

docker-compose run --no-deps client flink list

docker-compose run --no-deps client flink stop df9a400c1be8dd5d9a22dabc2946b363


docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic input
docker-compose exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic output


docker-compose build

Сценарий №1
1) Запускаем на одной ноде в один слот, смотрим файлы, смотрим чекпоитны, смотрим Lag, смотрим лог джоб менеджера
   docker-compose logs -f jobmanager
   
2) Останавливаем вручную, смотрим чекпоинты, саейвпоинты:
    docker-compose run --no-deps client flink list
    docker-compose run --no-deps client flink stop 901f048d17bf5e5055cf1b7990aae5d7
   
3) Восстанавливаем с тремя слотами, вместо одного
   docker-compose run --no-deps client flink run -s /tmp/flink-savepoints-directory/savepoint-df9a40-0ccce756c01b -d /opt/DeliveryWriteFileJob.jar --bootstrap.servers kafka:9092 --checkpointing --event-time

   docker-compose run --no-deps client flink run -p 3 -s /tmp/flink-savepoints-directory/savepoint-08d932-aadd23e6a003 -d /opt/DeliveryWriteFileJob.jar --bootstrap.servers kafka:9092 --checkpointing --event-time

4) Добавляем еще три ноды, которые автоматически регистрируются в application master
   docker-compose scale taskmanager=4

5) Убиваем первую, на которой занято два слота
6) docker inspect operations-playground_taskmanager_1
   docker kill operations-playground_taskmanager_1
   docker kill operations-playground_taskmanager_2