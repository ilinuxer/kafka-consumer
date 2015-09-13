package ssy.dmp.kafka.consumer;

/**
 * author: huangqian
 * data: 15/9/13
 */
public interface ConsumerConstants {
    /***
     * zookeeper配置信息
     */
    public static interface Zookeeper{
        /***
         *  zookeeper session 超时时间（单位/毫秒）
         */
        public static final String SESSION_TIMEOUT_MS_DEFAULT = "3000";
        /***
         *zookeeper 同步时间（单位/毫秒）
         */
        public static final String SYNC_TIME_MS_DEFAULT = "200";
    }

    /***
     *  自动提交时间间隔（单位/毫秒）
     */
    public static final String AUTO_COMMIT_INTERVAL_MS_DEFAULT = "600";

    /***
     *  每个topic分成kafkaStream的数量
     */
    public static final int PARTITION_NUM_DEFAULT = 1;
}
