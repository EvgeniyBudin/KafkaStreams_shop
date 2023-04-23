package ru.netology.dsw.dsl;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class PurchacesSumAlertsApp {
    public static final String JOINED_TOPIC_NAME = "purchase_with_joined_product";
    public static final String RESULT_TOPIC = "purchases_sum_alerts";
    private static final long MAX_PURCHASES_SUM = 300L;

    //ЧЕСТНО СВОРОВАННЫЙ КЛАСС
    public static void main(String[] args) throws InterruptedException {
        // создаем клиент для общения со schema-registry
        var client = new CachedSchemaRegistryClient("http://localhost:8090", 16);
        var serDeProps = Map.of(
                // указываем сериализатору, что может самостояетльно регистрировать схемы
                KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, "true",
                KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8090"
        );

        // строим нашу топологию
        Topology topology = buildTopology(client, serDeProps);

        // если скопировать вывод этой команды вот сюда - https://zz85.github.io/kafka-streams-viz/
        // то можно получить красивую визуализацию топологии прямо в браузере
        System.out.println(topology.describe());

        KafkaStreams kafkaStreams = new KafkaStreams(topology, getStreamsConfig());
        // вызов latch.await() будет блокировать текущий поток
        // до тех пор пока latch.countDown() не вызовут 1 раз
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                latch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            // будет блокировать поток, пока из другого потока не будет вызван метод countDown()
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }

    //ЧЕСТНО СВОРОВАННЫЙ КЛАСС
    public static Properties getStreamsConfig() {
        Properties props = new Properties();
        // имя этого приложения для кафки
        // приложения с одинаковым именем объединятся в ConsumerGroup и распределят обработку партиций между собой
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "QuantityAlertsAppDSL");
        // адреса брокеров нашей кафки (у нас он 1)
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // если вы захотите обработать записи заново, не забудьте удалить папку со стейтами
        // а лучше воспользуйтесь методом kafkaStreams.cleanUp()
        props.put(StreamsConfig.STATE_DIR_CONFIG, "C:\\tmp\\states");
        return props;
    }

    public static Topology buildTopology(SchemaRegistryClient client, Map<String, String> serDeConfig) {
        var builder = new StreamsBuilder();
        var avroSerde = new GenericAvroSerde(client);
        avroSerde.configure(serDeConfig, false);
        // Получаем из кафки поток сообщений из топика покупок, к которомы добавлены значения продукта
        var joinedStream = builder.stream(
                JOINED_TOPIC_NAME,
                Consumed.with(new Serdes.StringSerde(), avroSerde)
        );

        Duration oneMinute = Duration.ofMinutes(1);
        joinedStream.groupBy((key, val) -> val.get("product_id").toString(), Grouped.with(new Serdes.StringSerde(), avroSerde))
                .windowedBy(
                        TimeWindows.of(oneMinute)
                                .advanceBy(oneMinute))
                .aggregate(
                        () -> 0.,
                        // Аггрегируем произведение количества на цену
                        (key, val, agg) -> {
                            return agg += ((Long) val.get("purchase_quantity") * (Double) val.get("product_price"));
                        },
                        Materialized.with(new Serdes.StringSerde(), new Serdes.DoubleSerde())
                )
                .filter((key, val) -> val > MAX_PURCHASES_SUM)
                .toStream()
                .map((key, val) -> {
                    Schema schema = SchemaBuilder.record("PurschaseSumAlert").fields()
                            .name("window_start")
                                .type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)))
                                .noDefault()
                            .requiredLong("purchases_sum")
                            .endRecord();
                    GenericRecord record = new GenericData.Record(schema);
                    record.put("window_start", key.window().start());
                    record.put("purchases_sum", val);
                    return KeyValue.pair(key.key(), record);
                })
                .to(RESULT_TOPIC, Produced.with(new Serdes.StringSerde(), avroSerde));

        return builder.build();
    }
}
