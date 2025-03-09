
# Kafka Streams: Real-Time Analytics for Telegram Bot Events

Этот репозиторий содержит код для потоковой обработки данных, генерируемых Telegram ботом мероприятия ДВФУ, с использованием **Kafka Streams**. Приложение анализирует статистику в реальном времени, включая:
- Процентное соотношение команд бота.
- Средние значения закупок и вознаграждений.
- Количество выполненных активностей.

## Структура проекта
- `src/main/java/Stream/MyKafkaStreams.java` — Топология обработки потоков Kafka.
- `src/main/java/Consumer/MyConsumer.java` — Потребитель, сохраняющий результаты в файлы.
- `commandPercentage.txt`, `avg.txt`, `counters.txt` — Примеры выходных данных.

## Зависимости
- Java 11+
- Apache Kafka 3.0+
- Библиотеки Kafka Streams и Jackson для сериализации JSON.

## Связанные ресурсы
- [Telegram Bot Repo](https://github.com/xSICHx/DVFUTelegramBotC-) — Код бота, выступающего производителем событий.
- [Научно-исследовательская работа](https://github.com/xSICHx/research-works/blob/main/3%20sem%20Kafka/3B_222_Pogrebnikov.pdf) — Полное описание архитектуры и результатов.

---

_Проект разработан в рамках научно-исследовательской работы СПбГУ. Подробности в приложенной работе._
