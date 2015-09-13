package ssy.dmp.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;
import ssy.dmp.common.Toolkit;
import ssy.dmp.kafka.filter.ConsumerFilterChain;
import ssy.dmp.kafka.filter.EndpointFilter;
import ssy.dmp.kafka.filter.Filter;
import ssy.dmp.kafka.filter.FilterChain;
import ssy.dmp.kafka.message.MessageExecutor;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * author: huangqian
 * data: 15/9/13
 */
public class KafkaConsumer {
    private static final Logger LOG = Logger.getLogger(KafkaConsumer.class);

    private ConsumerConfig config;          //Kafka 消费者配置
    private String topic;                   //主题
    private int partitionsNum;              //分区的数量
    private MessageExecutor executor;       //消息执行器
    private ConsumerConnector connector;    //Kafka消费者连接
    private ExecutorService threadPool;     //消费者池
    private ArrayList<ConsumerFilterChain> filterChains = new ArrayList<ConsumerFilterChain>();


    private KafkaConsumer(ConsumerConfig config, String topic, int partitionsNum, MessageExecutor executor){
        this.config = config;
        this.topic = topic;
        this.partitionsNum = partitionsNum;
        this.executor = executor;
        Filter endpointFilter = new EndpointFilter(executor);
        filterChains.add(new ConsumerFilterChain(endpointFilter));
    }

    public void addFilter(Filter filter){
        ConsumerFilterChain cfc = new ConsumerFilterChain(filter);
        if(filterChains.size() > 1){
            FilterChain previous = filterChains.get(filterChains.size() -2);
            previous.setNext(cfc);
        }
        cfc.setNext(filterChains.get(filterChains.size() -1));
        filterChains.add(filterChains.size() - 1,cfc);
    }

    public void addFilters(ArrayList<Filter> filters){
        if(Toolkit.isNotNull(filters)){
            for(Filter filter : filters){
                addFilter(filter);
            }
        }
    }

    public static KafkaConsumer newInstance(ConsumerConfig config,String topic,int partitionsNum,MessageExecutor executor){
        return new KafkaConsumer(config,topic,partitionsNum,executor);
    }

    /***
     * 启动消费者
     */
    public void start(){
        connector = Consumer.createJavaConsumerConnector(config);
        Map<String,Integer> topics = new HashMap<String,Integer>();
        topics.put(topic, partitionsNum);
        Map<String, List<KafkaStream<byte[], byte[]>>> streams = connector.createMessageStreams(topics);
        List<KafkaStream<byte[], byte[]>> partitions = streams.get(topic);
        threadPool = Executors.newFixedThreadPool(partitionsNum);
        int threadNum = 1;
        for(KafkaStream<byte[], byte[]> partition : partitions){
            threadPool.execute(new MessageRunner(partition,threadNum++));
        }
    }

    /***
     * 关闭消费者
     */
    public void shutdown(){
        try{
            threadPool.shutdownNow();
        }catch(Exception e){
            LOG.error("KafkaConsumer threadPool shutdown error",e);
        }finally{
            connector.shutdown();
        }
    }


    class MessageRunner implements Runnable{
        private KafkaStream<byte[], byte[]> partition;
        private int threadNum;

        MessageRunner(KafkaStream<byte[], byte[]> partition,int threadNum) {
            this.partition = partition;
            this.threadNum = threadNum;
        }

        public void run(){
            ConsumerIterator<byte[], byte[]> it = partition.iterator();
            ConsumerFilterChain entryPointer = filterChains.get(0);
            while(it.hasNext()){
                try {
                    MessageAndMetadata<byte[],byte[]> item = it.next();
                    long timestamp = System.currentTimeMillis();
                    String message = new String(item.message(), Charset.forName("UTF-8"));
                    entryPointer.doFilter(threadNum,timestamp,message);
                } catch (Exception e) {
                    LOG.error("ConsumerIterator iterator error",e);
                }
            }
        }
    }
}
