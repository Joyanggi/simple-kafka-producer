import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CustomPartitioner implements Partitioner {
    private final static String TOPIC_NAME = "test";
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092";
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster){ //1. partition 메서드에는 레코드를 기반으로 파티션을 정하는 로직이 포함된다. 리턴값은 주어진 레코드가 들어갈 파티션 번호이다.
        if(keyBytes == null){ //2. 레코드에 메세지 키를 지정하지 않은 경우에는 비정상적인 데이터로 간주하고 InvalidRecordException을 발생시킨다.
            throw new InvalidRecordException("Need message key");
        }
        if(((String)key).equals("Pangyo")){ //3. 메세지 키가 Pangyo일 경우 파티션 0번으로 지정되도록 0을 리턴한다.
            return 0;
        }

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions; //4. Pangyo가 아닌 메세지 키를 가진 레코드는 해시값을 지정하여 특정 파티션에 매칭되도록 설정한다.
    }

    @Override
    public void configure(Map<String, ?> configs) {}

    @Override
    public void close() {}

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class);

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs);

        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "Pangyo", "Pangyo");
        producer.send(record);
        producer.flush();
        producer.close();
    }
}
