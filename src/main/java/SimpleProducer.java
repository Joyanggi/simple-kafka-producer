import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;

public class SimpleProducer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleProducer.class);
    private final static String TOPIC_NAME = "test"; //1. 프로듀서는 생성한 레코드를 전송하기 위해 전송하고자 하는 토픽을 알고 있어야 한다.
                                                    //토픽을 지정하지 않고서는 데이터를 전송할 수 없기 때문이다. 토픽 이름은 Producer Record 인스턴스를 생성할 때 사용된다.
    private final static String BOOTSTRAP_SERVERS = "my-kafka:9092"; //2. 전송하고자 하는 카프카 클러스터 서버의 host와 IP를 지정한다.

    public static void main(String[] args) {

        Properties configs = new Properties(); //3. Properties에는 KafkaProducer 인스턴스를 생성하기 위한 프로듀서 옵션들을 key/value 값으로 선언한다.
                                              //필수 옵션은 반드시 선언해야 하며, 선택 옵션은 선언하지 않아도 된다.
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //4. 메세지 키, 메세지 값을 직렬화하기 위한 직렬화 클래스를 선언한다.
        configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()); //여기서는 String 객체를 전송하기 위해 String을 직렬화하는 클래스인 카프카 라이브러리의 StringSerializer를 사용한다.

        KafkaProducer<String, String> producer = new KafkaProducer<>(configs); //5. Properties를 KafkaProducer의 생성파라미터로 추가하여 인스턴스를 생성한다. Producer 인스턴스는 ProducerRecord를 전송할 때 사용된다.

        String messageValue = "testMessage"; //6. 메세지 값을 선언한다.
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, messageValue); //7. 카프카 브로커로 데이터를 보내기 위해 ProducerRecord를 생성한다. ProducerRecord는 생성자를 여러 개 가지는데, 생성자 개수에 따라 오버로딩되어 생성된다.
                                                                                                //여기서는 토픽 이름과 메세지 값만 선언하였다. 메세지 키는 따로 선언하지 않았으므로 null로 설정되어 전송된다.
                                                                                                //ProducerRecord를 생성할 때 생성자에 2개의 제네릭 값이 들어가는데, 이 값은 메세지 키와 메세지 값의 타입을 뜻한다. 메세지 키와 메세지 값의 타입은 직렬화 클래스와 동일하게 설정한다.
        int partitionNo = 0;
        String messagekey = "key";
//        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, "log4j3", "test3"); //메세지 키를 가진 데이터를 전송
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME, partitionNo, messagekey, messageValue); //파티션을 직접 지정하여 데이터를 전송. 토픽이름, 파티션 번호, 메세지 키, 메세지 값을 순서대로 파라미터에 넣고 생성해야한다. 파티션번호는 토픽에 존재하는 번호로 설정해야한다.
        producer.send(record); //8. 생성한 ProducerRecord를 전송하기 위해 record를 파라미터로 가지는 send() 메서드를 호출했다. 프로듀서에서 send()는 즉각적인 전송을 뜻하는 것이 아니라, 파라미터로 들어간 record를 프로듀서 내부에 가지고 있다가 배치 형태로 묶어서 브로커에 전송한다.
                               //이러한 전송 방식을 '배치 전송'이라고 부른다. 배치 전송을 통해 카프카는 ㅌ아 메세지 플랫폼과 차별화된 전송 속도를 가지게 되었다.
        logger.info("{}", record);
        producer.flush(); //9. flush()를 통해 프로듀서 내부 버퍼에 가지고 있던 레코드 배치를 브로커로 전송한다.
        producer.close(); //10. 애플리케이션을 종료하기 전에 close() 메서드를 호출하여 producer 인스턴스의 리소스들을 안전하게 종료한다.
    }
}
