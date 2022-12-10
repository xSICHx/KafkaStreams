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
import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"WeakerAccess", "unused"})
public class JsonUser {

    /**
     * A serde for any class that implements {@link JSONSerdeCompatible}. Note that the classes also need to
     * be registered in the {@code @JsonSubTypes} annotation on {@link JSONSerdeCompatible}.
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
                return (T) OBJECT_MAPPER.readValue(data, User.class);
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

    @JsonSubTypes({
            @JsonSubTypes.Type(value = User.class, name = "User")
    })
    public interface JSONSerdeCompatible {

    }


    @JsonTypeName("User")
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class User implements JSONSerdeCompatible{
        public String Id;//telegram name
        public long ChatId;
        public String Menu ;
        public int AdminFlag;
        public int AmountOfMoney;
        public HashMap<String, Integer> TrialsDict;
        public HashMap<String, Integer> ProductsPurchaced;

        public HashMap<String, Integer> getTrialsDict() {
            return TrialsDict;
        }

        public void setTrialsDict(HashMap<String, Integer> trialsDict) {
            TrialsDict = trialsDict;
        }

        public HashMap<String, Integer> getProductsPurchaced() {
            return ProductsPurchaced;
        }

        public void setProductsPurchaced(HashMap<String, Integer> productsPurchaced) {
            ProductsPurchaced = productsPurchaced;
        }

        public User() {
        }

        public String getId() {
                return Id;
            }

        public void setId(String id) {
            Id = id;
        }

        public long getChatId() {
            return ChatId;
        }

        public void setChatId(long chatId) {
            ChatId = chatId;
        }

        public String getMenu() {
            return Menu;
        }

        public void setMenu(String menu) {
            Menu = menu;
        }

        public int getAdminFlag() {
            return AdminFlag;
        }

        public void setAdminFlag(int adminFlag) {
            AdminFlag = adminFlag;
        }

        public int getAmountOfMoney() {
            return AmountOfMoney;
        }

        public void setAmountOfMoney(int amountOfMoney) {
            AmountOfMoney = amountOfMoney;
        }

    }


}