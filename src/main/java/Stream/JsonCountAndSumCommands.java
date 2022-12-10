package Stream;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JsonCountAndSumCommands {
    /**
     * A serde for any class that implements {@link JsonUser.JSONSerdeCompatible}. Note that the classes also need to
     * be registered in the {@code @JsonSubTypes} annotation on {@link JsonUser.JSONSerdeCompatible}.
     *
     * @param <T> The concrete type of the class that gets de/serialized
     */
    public static class JSONSerde<T extends JSONSerdeCompatible> implements Serializer<T>, Deserializer<T>, Serde<T> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {}

        @SuppressWarnings("unchecked")
        @Override

        public T deserialize(final String topic, final byte[] data) {

            if (data == null) {
                return null;
            }
            try {
                return (T) OBJECT_MAPPER.readValue(data, CountAndSumCommands.class);
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data == null) {
                return null;
            }

            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {}

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
    }
    @SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
//    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
//    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = CountAndSumCommands.class, name = "CountAndSumCommands")
    })
    public interface JSONSerdeCompatible {

    }

    @JsonTypeName("CountAndSumCommands")
    @JsonIgnoreProperties(ignoreUnknown = true)
//    public static class CountAndSumCommands implements JSONSerdeCompatible {
//        public long sum;
//        public long count;
//
//        public CountAndSumCommands(long sum, long count){
//            this.sum = sum;
//            this.count = count;
//        }
//        public CountAndSumCommands() {
//
//        }
//
//        public long getSum() {
//            return sum;
//        }
//
//        public void setSum(long sum) {
//            this.sum = sum;
//        }
//
//        public long getCount() {
//            return count;
//        }
//
//        public void setCount(long count) {
//            this.count = count;
//        }
//    }


        public static class CountAndSumCommands implements JSONSerdeCompatible{
        public static long sum = 0;
        public long count;

        public CountAndSumCommands(long count){
            this.count = count;
        }
        public CountAndSumCommands() {

        }

        public static long getSum() {
            return sum;
        }

        public static void setSum(long sum) {
            CountAndSumCommands.sum = sum;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }
    }


}
