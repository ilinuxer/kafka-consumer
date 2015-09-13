package ssy.dmp.kafka.message;

/**
 * author: huangqian
 * data: 15/9/13
 */
public interface MessageExecutor {

    public void execute(String message);
}
