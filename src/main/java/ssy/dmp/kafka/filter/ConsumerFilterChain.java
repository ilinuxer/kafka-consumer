package ssy.dmp.kafka.filter;

/**
 * author: huangqian
 * data: 15/9/12
 */
public final class ConsumerFilterChain implements FilterChain{

    private FilterChain next;
    private Filter filter;

    public ConsumerFilterChain() {
    }

    public ConsumerFilterChain(Filter filter) {
        this.filter = filter;
    }

    @Override
    public void doFilter(int threadNum, long timeStamp, String message) {
            filter.doFilter(threadNum,timeStamp,message,next);
    }

    @Override
    public void setNext(FilterChain nextChain) {
        this.next = nextChain;
    }

    public void setFilter(Filter filter){
        this.filter = filter;
    }
}
