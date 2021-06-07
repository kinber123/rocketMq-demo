package com.rocketmq.demo.service.transaction;

import com.rocketmq.demo.config.JmsConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.stereotype.Component;

import java.util.concurrent.*;

/**
 * \* @author wcy
 * \* @date: 2020-05-21 9:23
 * \* Description:  发送同步消息
 * \
 */
@Slf4j
@Component
public class TransactionProducer {

    private String producerGroup = "test_tran_producer";


    TransactionMQProducer producer;

    public TransactionProducer() {
        this.start();
    }

    public void start() {
        //1.创建消息生产者producer，并制定生产者组名
        producer = new TransactionMQProducer(producerGroup);
        //2.指定Nameserver地址
        producer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        // 设置超过多大进行compress压缩
        producer.setCompressMsgBodyOverHowmuch(1024 * 10);
        // 设置发送失败的尝试次数。
        producer.setRetryTimesWhenSendFailed(3);
        // 设置如果返回值不是send_ok，是否要重新发送
        producer.setRetryAnotherBrokerWhenNotStoreOK(false);
        // 设置限制最大的文件大小
        producer.setMaxMessageSize(1024*50);
        // 设置默认主题对应的队列数
        producer.setDefaultTopicQueueNums(4);
        // 设置发送超时时间 ms
        producer.setSendMsgTimeout(1000);
        //添加事务监听器
         TransactionListenerImpl transactionListener = new TransactionListenerImpl();
        producer.setTransactionListener(transactionListener);
        // 创建线程池
        ExecutorService executorService = new ThreadPoolExecutor(
                2,
                5,
                100,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(
                        200),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable runnable) {
                        Thread thread = new Thread(runnable);
                        thread.setName("client-transaction");
                        return thread;
                    }
                }
        );
        producer.setExecutorService(executorService);
        //3.启动producer
        try {
            producer.start();
        } catch (MQClientException e) {
            log.error(ExceptionUtils.getStackTrace(e));
        }
        log.info("事务生产者 启动成功=======");
    }

    /**
     * 生产者生产方法
     *
     * @param topic 主题
     * @param tags  标签，用来给消费者进行过滤的
     * @param keys  作为key
     * @param body  发送的内容
     */
    @SneakyThrows
    public SendResult producerSendMes(String topic, String tags, String keys, String body) {
        Message message = new Message(topic, tags, keys, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
        //发送
        SendResult sendResult = this.producer.sendMessageInTransaction(message, null);
        return sendResult;
    }
    /**
     * 生产者生产方法
     * @param topic 主题
     * @param tags 标签，用来给消费者进行过滤的
     * @param body 发送的内容
     */
    @SneakyThrows
    public SendResult producerSendMes(String topic, String tags, String body) {
        Message message = new Message(topic, tags, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
        //发送
        SendResult sendResult = this.producer.sendMessageInTransaction(message,"hello word !!!");
        return sendResult;
    }

    public TransactionMQProducer getProducer(){
        return producer;
    }

    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    public void shutdown() {
        producer.shutdown();
    }
}
