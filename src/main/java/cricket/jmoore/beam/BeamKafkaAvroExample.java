package cricket.jmoore.beam;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy;

import lombok.extern.slf4j.Slf4j;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.ConfluentSchemaRegistryDeserializerProvider;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.POutput;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import cricket.jmoore.avro.Product;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class BeamKafkaAvroExample {

  public static class ProductSerializer implements Serializer<Product> {

    private final KafkaAvroSerializer inner;

    public ProductSerializer() {
      inner = new KafkaAvroSerializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public byte[] serialize(String topic, Product product) {
      return inner.serialize(topic, product);
    }

  }

  public static class ProductDeserializer implements Deserializer<Product> {

    private final KafkaAvroDeserializer inner;

    public ProductDeserializer() {
      inner = new KafkaAvroDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
      inner.configure(configs, isKey);
    }

    @Override
    public Product deserialize(String s, byte[] bytes) {
      return (Product) inner.deserialize(s, bytes);
    }
  }

  public static void main(String[] args) throws Exception {

    final PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();

    final String topic = "foobar";
    Map<String, String> baseConfigs = ImmutableMap.of(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
        SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

    Pipeline p;
//    p = getWritePipeline(options, baseConfigs, topic);
    p = getReaderPipeline(options, ImmutableMap.<String, String>builder().putAll(baseConfigs)
        .put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase(Locale.ROOT))
        .put(ConsumerConfig.GROUP_ID_CONFIG, "beam-" + UUID.randomUUID())
        .build(), topic);

    p.run().waitUntilFinish();
  }

  private static Pipeline getReaderPipeline(PipelineOptions options, Map<String, String> consumerConfigs, String topic) {
    Pipeline p = Pipeline.create(options);

    final SubjectNameStrategy subjectStrategy = new TopicNameStrategy();
    final String valueSubject = subjectStrategy.subjectName(topic, false, null); // schema not used
    final ConfluentSchemaRegistryDeserializerProvider<Product> valueProvider =
        ConfluentSchemaRegistryDeserializerProvider.<Product>of(consumerConfigs.get(SCHEMA_REGISTRY_URL_CONFIG), valueSubject, null,
                                                                // TODO: This doesn't seem to work
                                                                ImmutableMap.of(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true));
    p
        .apply(KafkaIO.<byte[], Product>read()
                   .withBootstrapServers(consumerConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
                   .withTopic(topic)
                   .withKeyDeserializer(ByteArrayDeserializer.class)
                   .withValueDeserializer(valueProvider)
                   .withConsumerConfigUpdates(ImmutableMap.copyOf(consumerConfigs))
                   .withoutMetadata()
        ).apply(Values.create()) // drop keys
        // TODO: How to get SpecificRecord?
        // java.lang.ClassCastException: org.apache.avro.generic.GenericData$Record cannot be cast to cricket.jmoore.avro.Product
        .apply(MapElements.via(new SimpleFunction<Product, Void>() {
          @Override
          public Void apply(Product input) {
            log.info("{}", input);
            return null;
          }
        }));
    return p;
  }

  private static Pipeline getWritePipeline(PipelineOptions options, Map<String, String> producerConfigs, String topic) {
    Pipeline p = Pipeline.create(options);

    final List<Product> products = Arrays.asList(
        Product.newBuilder().setName("Hello").build(),
        Product.newBuilder().setName("World").build()
    );

    p
        .apply(Create.of(products))
//        .apply(MapElements.via(new SimpleFunction<Product, byte[]>() {
//          @Override
//          public byte[] apply(Product input) {
//            try {
//              return input.toByteBuffer().array();
//            } catch (IOException e) {
//              log.error("Error", e);
//              return null;
//            }
//          }
//        }))
//        .apply(Filter.by(Objects::nonNull))
        .apply(new PTransform<PCollection<Product>, POutput>() {
          @Override
          public POutput expand(PCollection<Product> input) {
            return input.apply("Write to Kafka", KafkaIO.<Void, Product>write()
                .withBootstrapServers(producerConfigs.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG))
                .withTopic(topic)
                .withValueSerializer(ProductSerializer.class) // BUG? Not able to use KafkaAvroSerializer
                .withProducerConfigUpdates(ImmutableMap.copyOf(producerConfigs))
                .values()
            );
          }
        });

    return p;
  }
}
