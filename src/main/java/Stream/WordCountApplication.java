package Stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;

public class WordCountApplication {
    public static void main(String[] args) {
        Properties props = new Properties();
        /* У каждого приложения Kafka должен быть свой идентификатор приложения.
        Он используется для координации действий экземпляров приложения, а также
        именования внутренних локальных хранилищ и относящихся к ним тем. Среди
        приложений Kafka Streams, работающих в пределах одного кластера, идентификаторы не должны повторяться.*/
        props.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "wordcount");

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        /* Приложение должно выполнять сериализацию и десериализацию при чтении
        и записи данных, поэтому мы указываем классы, наследующие интерфейс Serde
        для использования по умолчанию*/
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //Создаем объект класса StreamsBuilder и приступаем к описанию потока, передавая название входной темы.
        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> source =
                builder.stream("wordcount-input");
        final Pattern pattern = Pattern.compile("\\W+");
        KStream counts = source.flatMapValues(value->
                        /* Все читаемые из темы-производителя события представляют собой строки слов. Мы разбиваем их с помощью регулярного выражения на последовательности отдельных слов. Затем вставляем каждое из слов (значение записи для какоголибо события) в ключ записи этого события для дальнейшего использованияв операции группировки*/
                        Arrays.asList(pattern.split(value.toLowerCase())))
                .map((key, value) -> new KeyValue<Object,
                        Object>(value, value))
                /* Отфильтровываем слово the просто для демонстрации того, как просто это делать*/
                .filter((key, value) -> (!value.equals("the")))
                /* И группируем по ключу, получая наборы событий для каждого уникального слова.*/
                .groupByKey()
                /*Подсчитываем количество событий в каждом наборе. Заносим результаты в значение типа Long. Преобразуем его в String для большей удобочитаемости результатов*/
                .count(Named.as("CountStore")).mapValues(value->
                        Long.toString(value)).toStream();
        /*Осталось только записать результаты обратно в Kafka*/
        counts.to("wordcount-output");

        /*Создаём топологию*/
        final Topology topology = builder.build();
        /*Описываем объект KafkaStreams на основе нашей топологии и заданных нами свойств*/
        final KafkaStreams streams = new KafkaStreams(topology, props);
        /*Обрабатываем остановку программы*/
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                streams.cleanUp();
                latch.countDown();
            }
        });
        /*Пробуем запустить Kafka Streams*/
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
