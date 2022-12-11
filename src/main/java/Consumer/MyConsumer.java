package Consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class MyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "Test");
        props.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        List<String> lst = new ArrayList<>();
        lst.add("command-percent-output");
        lst.add("counters-output");
        lst.add("average-output");
        consumer.subscribe(lst);
        try {
            HashMap<String, Double> commandPercentage = new HashMap<>();
            HashMap<String, Double> avg = new HashMap<>();
            HashMap<String, Long> counters = new HashMap<>();
            Scanner in = new Scanner(System.in);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                String command = in.nextLine();
                for (ConsumerRecord<String, String> record : records) {
                    if (Objects.equals(record.topic(), "command-percent-output"))
                        commandPercentage.put(record.key(), Double.parseDouble(record.value()));
                    if (Objects.equals(record.topic(), "counters-output"))
                        counters.put(record.key(), Long.parseLong(record.value()));
                    if (Objects.equals(record.topic(), "average-output"))
                        avg.put(record.key(), Double.parseDouble(record.value()));
                }
                //write in console 0 and consumer will close with data saved
                if (Objects.equals(command, "0")) {
                    saveChangesDouble(commandPercentage, "commandPercentage.txt");
                    saveChangesDouble(avg, "avg.txt");
                    saveChangesLong(counters, "counters.txt");
                    break;
                }
                //write in console 1 and data will appear on the screen
                else if (Objects.equals(command, "1")){
                    List<Map.Entry<String, Double>> commandList = new ArrayList<>(commandPercentage.entrySet());
                    final Comparator<Map.Entry<String, Double>> COMPARE_BY_COUNT = (lhs, rhs) -> {
                        if (Objects.equals(lhs.getValue(), rhs.getValue())) return 0;
                        else if (lhs.getValue() > rhs.getValue()) return 1;
                        else return -1;
                    };
                    commandList.sort(COMPARE_BY_COUNT);
                    for (Map.Entry<String, Double> entry: commandList) {
                        System.out.println(entry.getKey() + " " +  entry.getValue().toString());
                    }

                    System.out.println();
                    for (Map.Entry<String, Double> entry: avg.entrySet()) {
                        System.out.println(entry.getKey() + " " +  entry.getValue().toString());
                    }
                    System.out.println();
                    for (Map.Entry<String, Long> entry: counters.entrySet()) {
                        System.out.println(entry.getKey() + " " +  entry.getValue().toString());
                    }
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        finally {
            consumer.close();
        }
    }

    static void saveChangesDouble(HashMap<String, Double> map, String filename) {
        File f = new File(filename);
        try (FileWriter fileWriter = new FileWriter(f, false)){
            for (Map.Entry<String, Double> entry: map.entrySet()) {
                fileWriter.write(entry.getKey() + " " +  entry.getValue().toString() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void saveChangesLong(HashMap<String, Long> map, String filename) {
        File f = new File(filename);
        try (FileWriter fileWriter = new FileWriter(f, false)){
            for (Map.Entry<String, Long> entry: map.entrySet()) {
                fileWriter.write(entry.getKey() + " " +  entry.getValue().toString() + "\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
