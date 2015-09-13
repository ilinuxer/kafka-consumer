package ssy.dmp.kafka.filter;

/**
 * author: huangqian
 * data: 15/9/13
 */
public final  class FilterChainFactory {

    public static FilterChain createFilterChain(Filter filter){
        return new ConsumerFilterChain();
    }
}
