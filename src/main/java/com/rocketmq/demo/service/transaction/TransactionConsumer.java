package com.rocketmq.demo.service.transaction;

import com.rocketmq.demo.config.JmsConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.Message;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
/**
 * \* @author wcy
 * \* @date: 2020-05-21 9:23
 * \* Description:  消息的接受者
 * \
 */
@Slf4j
@Component
public class TransactionConsumer {


    /**
     * 消费者组
     */
    public static final String CONSUMER_GROUP = "test_tran_consumer";


    public TransactionConsumer() throws MQClientException {
        // 启动事务消息
        this.consumerInit();
    }

    public void  consumerInit() throws MQClientException {
        //1.创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(CONSUMER_GROUP);
        //2.指定Nameserver地址
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        //3.订阅主题Topic和Tag
        consumer.subscribe(JmsConfig.TOPIC, "*");


        //4.设置回调函数，处理消息
        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
            // msgs中只收集同一个topic，同一个tag，并且key相同的message
            // 会把不同的消息分别放置到不同的队列中
            try {
                for (Message msg : msgs) {
                    //消费者获取消息 这里只输出 不做后面逻辑处理
                    String body = new String(msg.getBody(), "utf-8");
                    log.info("TransactionConsumer-获取消息-主题topic为={}, 消费消息为={}", msg.getTopic(), body);
                }
            } catch (UnsupportedEncodingException e) {
                log.error("rocketMq error:{}", ExceptionUtils.getStackTrace(e));
                // 异常重试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
        //5.启动消费者consumer
        log.info("事务消费者 启动成功=======");
        consumer.start();
    }

}
