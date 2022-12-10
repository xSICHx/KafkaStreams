package Stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class MyKafkaStreams {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        /** command percentage*/
        final KStream<String, String> commandsStream = builder.stream("command-counter-input");
        final KeyValueBytesStoreSupplier cmStore = Stores.persistentKeyValueStore("cm-stream-store");
        final KTable<String, JsonCountAndSumCommands.CountAndSumCommands> cmTable = commandsStream
                .groupByKey()
                .aggregate(
                        ()->new JsonCountAndSumCommands.CountAndSumCommands(0L),
                        (key, value, aggregate) -> {
                            aggregate.setCount(aggregate.getCount() + 1);
                            JsonCountAndSumCommands.CountAndSumCommands.setSum(JsonCountAndSumCommands.CountAndSumCommands.getSum() + 1);
                            return aggregate;
                        },
                        Materialized.<String, JsonCountAndSumCommands.CountAndSumCommands>as(cmStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonCountAndSumCommands.JSONSerde<JsonCountAndSumCommands.CountAndSumCommands>())
                                .withLoggingDisabled()
                );

        final KeyValueBytesStoreSupplier percentStore = Stores.persistentKeyValueStore("percentCommands-store");
        final KTable<String, Double> commandsPercent = cmTable
                        .mapValues(value ->  (((double) value.getCount()) / (double) JsonCountAndSumCommands.CountAndSumCommands.getSum())*100,
                        Materialized.<String, Double>as(percentStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                                .withLoggingDisabled()
                        );
        commandsPercent.toStream().mapValues((v)->v.toString())
                .to("command-percent-output", Produced.with(Serdes.String(), Serdes.String()));



        /** avg*/
        final KStream<String, Long> purchaseStream = builder.stream("avg-input", Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues((v) -> Long.parseLong(v));
        final KeyValueBytesStoreSupplier purchaseStore = Stores.persistentKeyValueStore("avg-store");
        final KTable<String, JsonAvg.CountAndSum> purchaseTable = purchaseStream
                .groupByKey()
                .aggregate(
                        () -> new JsonAvg.CountAndSum(0, 0),
                        (key, value, aggregate) -> {
                            aggregate.setCount(aggregate.getCount() + 1);
                            aggregate.setSum(aggregate.getSum() + value);
                            return aggregate;
                        },
                        Materialized.<String, JsonAvg.CountAndSum>as(purchaseStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonAvg.JSONSerde<>())
                                .withLoggingDisabled()
                );

        final KeyValueBytesStoreSupplier purchaseAvgStore = Stores.persistentKeyValueStore("average-store");
        final KTable<String, Double> purchaseAvg = purchaseTable
                .mapValues(value ->  ((double)value.getSum()) / (double) value.getCount(),
                        Materialized.<String, Double>as(purchaseAvgStore)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                                .withLoggingDisabled()
                );
        purchaseAvg.toStream().mapValues((v)->v.toString())
                .to("average-output", Produced.with(Serdes.String(), Serdes.String()));

        /** counters*/
        final KeyValueBytesStoreSupplier trialsCounterStore = Stores.persistentKeyValueStore("trials-counter-store");
        final KTable<String, Long> trialsCounterStream = builder.stream("trials-input", Consumed.with(Serdes.String(), Serdes.String()))
                .groupByKey()
                .count(Materialized.<String, Long>as(trialsCounterStore)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long())
                        .withLoggingDisabled());
        trialsCounterStream.toStream().mapValues((v)->v.toString())
                .to("counters-output", Produced.with(Serdes.String(), Serdes.String()));







        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, props);

        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                streams.cleanUp();
                latch.countDown();
            }
        });
        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
