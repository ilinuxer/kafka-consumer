package ssy.dmp.kafka.filter;

import org.apache.log4j.Logger;
import ssy.dmp.common.Toolkit;
import ssy.dmp.kafka.message.MessageExecutor;


/**
 * author: huangqian
 * data: 15/9/12
 */
public final class EndpointFilter implements Filter {

    private static final Logger LOG = Logger.getLogger(EndpointFilter.class);

    private MessageExecutor messageExecutor;

    public EndpointFilter() {
    }

    public EndpointFilter(MessageExecutor messageExecutor) {
        this.messageExecutor = messageExecutor;
    }

    public MessageExecutor getMessageExecutor() {
        return messageExecutor;
    }

    public void setMessageExecutor(MessageExecutor messageExecutor) {
        this.messageExecutor = messageExecutor;
    }

    @Override
    public void doFilter(int threadNum, long timeStamp, String message, FilterChain filterChain) {
        if(Toolkit.isNotNull(messageExecutor)) {
            messageExecutor.execute(message);
        }else{
            LOG.info("message executor is null。");
        }
    }
}
