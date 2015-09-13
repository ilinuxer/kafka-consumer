package ssy.dmp.kafka.consumer;

import kafka.consumer.ConsumerConfig;
import org.apache.commons.lang3.StringUtils;
import ssy.dmp.common.Toolkit;
import ssy.dmp.kafka.filter.Filter;
import ssy.dmp.kafka.message.MessageExecutor;

import java.util.ArrayList;
import java.util.Properties;

/**
 * author: huangqian
 * data: 15/9/13
 */
public class KafkaConsumerBuilder implements ConsumerConstants{

    private ConsumerConfig config;          //Kafka 消费者配置
    private String topic;                   //主题
    private MessageExecutor executor;       //消息执行器
    private String groupId;                 //分组ID
    private String zkConnectUrl;            //ZooKeeper的url

    private int partitionsNum               = PARTITION_NUM_DEFAULT;                //分区的数量
    private String zkSessionTimeoutMs       = Zookeeper.SESSION_TIMEOUT_MS_DEFAULT; //ZK的session超时时间
    private String zkSyncTimeMs             = Zookeeper.SYNC_TIME_MS_DEFAULT;       //ZK同步的时间（毫秒）
    private String autoCommitIntervalMs     = AUTO_COMMIT_INTERVAL_MS_DEFAULT;      //自动提交的时间间隔（毫秒）
    private ArrayList<Filter> filters       = new ArrayList<Filter>();


    private KafkaConsumerBuilder(){
    }

    public KafkaConsumerBuilder setFilters(ArrayList<Filter> filterList){
        if(Toolkit.isNotEmpty(filters)){
            filters.addAll(filterList);
        }
        return this;
    }

    public KafkaConsumerBuilder addFilter(Filter filter){
        if(Toolkit.isNotNull(filter)){
            filters.add(filter);
        }
        return this;
    }

    /***
     * 初始化Kafka Consumer的配置对象
     */
    private void initConfig(){
        Properties props = new Properties();
        props.put("zookeeper.connect", zkConnectUrl);
        props.put("zookeeper.session.timeout.ms",zkSessionTimeoutMs);
        props.put("zookeeper.sync.time.ms", zkSyncTimeMs);
        props.put("group.id", groupId);
        props.put("auto.commit.interval.ms", autoCommitIntervalMs);
        //props.put("auto.offset.reset", "smallest");
        config = new ConsumerConfig(props);
    }

    public static KafkaConsumerBuilder getBuilder(){
        return new KafkaConsumerBuilder();
    }

    /***
     * 设置Kafka消费的Topic。必须设置。
     */
    public KafkaConsumerBuilder setTopic(String topic){
        this.topic = topic;
        return this;
    }

    /***
     * 设置分区的数量。默认值＝1
     * @param partitionNum 主题分区的数量
     */
    public KafkaConsumerBuilder setPartitionNum(int partitionNum){
        Toolkit.ifExpressionFalse(partitionNum >= 1,"partitionNum can't less than 1");
        this.partitionsNum = partitionNum;
        return this;
    }

    /***
     * 设置消息执行器。必须设置。
     * @param messageExecutor 消息执行器
     */
    public KafkaConsumerBuilder setMessageExecutor(MessageExecutor messageExecutor){
        this.executor = messageExecutor;
        return this;
    }

    /***
     * 设置Kafka托管的ZooKeeper连接的URL。必须设置。
     * @param zkConnectUrl ZooKeeper连接的URL
     */
    public KafkaConsumerBuilder setZkConnectUrl(String zkConnectUrl) {
        if(StringUtils.isBlank(zkConnectUrl)){
            throw new IllegalArgumentException("zkConnectUrl can't empty");
        }
        this.zkConnectUrl = zkConnectUrl;
        return this;
    }

    /***
     * ZooKeeper的Session的timeout时间（单位：ms）
     * @param zkSessionTimeoutMs
     */
    public KafkaConsumerBuilder setZkSessionTimeoutMs(String zkSessionTimeoutMs) {
        if(!StringUtils.isNumeric(zkSessionTimeoutMs)){
            throw new IllegalArgumentException("zkSessionTimeoutMs must be Numeric !");
        }
        this.zkSessionTimeoutMs = zkSessionTimeoutMs;
        return this;
    }

    /***
     * 设置ZooKeeper同步时间（单位：ms）
     * @param zkSyncTimeMs ZooKeeper同步时间
     */
    public KafkaConsumerBuilder setZkSyncTimeMs(String zkSyncTimeMs) {
        if(!StringUtils.isNumeric(zkSyncTimeMs)){
            throw new IllegalArgumentException("zkSyncTimeMs must be Numeric !");
        }
        this.zkSyncTimeMs = zkSyncTimeMs;
        return this;
    }

    /***
     * 设置Kafka的Consumer的Group Id
     * @param groupId Kafka Consumer的Group Id
     */
    public KafkaConsumerBuilder setGroupId(String groupId) {
        if(StringUtils.isBlank(groupId)){
            throw new IllegalArgumentException("groupId can't empty!");
        }
        this.groupId = groupId;
        return this;
    }

    /***
     * 设置自动提交的时间间隔（单位：ms）
     * @param autoCommitIntervalMs 自动提交时间间隔。
     */
    public KafkaConsumerBuilder setAutoCommitIntervalMs(String autoCommitIntervalMs) {
        if(!StringUtils.isNumeric(autoCommitIntervalMs)){
            throw new IllegalArgumentException("autoCommitIntervalMs must be numeric !");
        }
        this.autoCommitIntervalMs = autoCommitIntervalMs;
        return this;
    }


    /***
     * 构建Kafka消费者
     * @return 返回Kafka的消费者
     */
    public KafkaConsumer build(){
        validateParameter();
        this.initConfig();
        KafkaConsumer instance = KafkaConsumer.newInstance(config,topic,partitionsNum,executor);
        if(Toolkit.isNotEmpty(filters)){//set filter chain
            instance.addFilters(filters);
        }
        return instance;
    }

    private void validateParameter() {
        Toolkit.ifExpressionFalse(StringUtils.isNotBlank(zkConnectUrl), "zookeeper connect url can't blank !");
        Toolkit.ifExpressionFalse(StringUtils.isNotBlank(topic),"can't empty topic");
        Toolkit.ifNull(executor, "message executor can't be null !");
        Toolkit.ifExpressionFalse(StringUtils.isNotBlank(groupId),"group id can't empty!");
    }
}
