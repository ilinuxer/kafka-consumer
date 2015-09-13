# kafka-consumer

 kafka-consumer是基于Kafka-0.8.20封装的consumer。kafka-consumer的目的在于让业务开发人员不必了解kafka就能开发。并且提供消息过滤功能。
 

## Example

关于kafka-consumer的使用，可以参考如下代码。

		public class Example {

    private static final Logger LOG = Logger.getLogger(Example.class);

    private static  String topic = "test";

    private static int partitionNum = 1;

    private static String zkConnectUrl = "127.0.0.1:2181";

    private static String zkSessionTimeout = "3000";

    private static String zkSyncTime = "400";

    private static String autoCommitInterval = "600";

    private static String groupId = "test-kafka";

    private Filter logFilter = new Filter() {
        @Override
        public void doFilter(int threadNum, long timeStamp, String message, FilterChain filterChain) {
            String time = DateFormatUtils.format(new Date(timeStamp),"yyyy-MM-dd:HH:mm:ssS");
            System.out.println(String.format("threadNum=%d,time=%s,message=%s",threadNum,time,message));
        }
    };

    private KafkaConsumer consumer = KafkaConsumerBuilder.getBuilder()
            .setTopic(topic)
            .setPartitionNum(partitionNum)
            .setZkConnectUrl(zkConnectUrl)
            .setZkSessionTimeoutMs(zkSessionTimeout)
            .setZkSyncTimeMs(zkSyncTime)
            .setAutoCommitIntervalMs(autoCommitInterval)
            .setGroupId(groupId)
            .addFilter(logFilter)
            .setMessageExecutor(new MessageExecutor() {
                @Override
                public void execute(String message) {
                    System.out.println("message:" + message);
                }
            })
            .build();




    public void start(){
        LOG.info("--------启动Kafka消费者------------");
        LOG.info("topic=" + topic);
        LOG.info("partitionNum=" + partitionNum);
        LOG.info("zkConnectUrl=" + zkConnectUrl);
        LOG.info("zkSessionTimeout=" + zkSessionTimeout);
        LOG.info("zkSyncTime=" + zkSyncTime);
        LOG.info("autoCommitInterval=" + autoCommitInterval);
        LOG.info("groupId=" + groupId);
        if(Toolkit.isNotNull(consumer)) {
            consumer.start();
        }
    }


    public void shuntDown(){
        LOG.info("----------------关闭Kafka消费者------------------");
        if(Toolkit.isNotNull(consumer)){
            consumer.shutdown();
        }
    }
