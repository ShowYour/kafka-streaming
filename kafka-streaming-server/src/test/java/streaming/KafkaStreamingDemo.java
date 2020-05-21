package streaming;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @ClassName KafkaStreaimgDemo
 * @Author xiaoyu
 * @Date 2020/5/15 11:11
 * @Description TODO
 **/
public class KafkaStreamingDemo {
    private static final Logger logger = LoggerFactory.getLogger(KafkaStreamingDemo.class);

//    @Before
    public void producerData(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "bigdata4:9092");
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        String topic = "streams-plaintext-input";
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>(topic, Integer.toString(i), Integer.toString(i)));
        producer.close();
    }

    @Test
    public void runStreaming(){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe-1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "bigdata4:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //
        Topology builder1 = new Topology();
        //
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> source = builder.stream("streams-plaintext-input");
        source.to("streams-pipe-output");
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        logger.info(topology.describe().toString());
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
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

//    public static void main(String[] args) throws IOException, InterruptedException {
//        CountDownLatch countDownLatch = new CountDownLatch(1);
//        Properties properties = PropertiesLoaderUtils.loadAllProperties("application.properties");
//        ZooKeeper zooKeeper = new ZooKeeper(properties.getProperty("zookeeper.url"), 3000, event -> {
//            if (event.getState()== Watcher.Event.KeeperState.SyncConnected){
//                countDownLatch.countDown();
//            }
//        });
//        countDownLatch.await();
//        System.out.println(zooKeeper.getState().isAlive());
//        System.out.println("等待。。。。。");
//
//        Watcher watcher = new Watcher() {
//
//            @Override
//            public void process(WatchedEvent event) {
//                System.out.println(event.toString());
//                try {
//                    byte[] data = zooKeeper.getData(rootPath + "test", this, null);
//                    System.out.println(new String(data));
//                    System.out.println(zooKeeper.getState().isAlive());
//                    System.out.println(zooKeeper.getState().isConnected());
//                } catch (KeeperException e) {
//                    e.printStackTrace();
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        };
//        try {
//            zooKeeper.getData(rootPath+"test",watcher,null);
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        }
//        Thread.sleep(30000);
//        System.out.println(zooKeeper.getState().isAlive());
//        System.out.println(zooKeeper.getState().isConnected());
//        Thread.sleep(Integer.MAX_VALUE);
//    }
}
