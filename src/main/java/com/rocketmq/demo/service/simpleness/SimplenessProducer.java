package com.rocketmq.demo.service.simpleness;

import com.rocketmq.demo.config.JmsConfig;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.stereotype.Component;
import org.apache.rocketmq.common.message.Message;

/**
 * \* @author wcy
 * \* @date: 2020-05-15 17:34
 * \* Description:  类
 * \
 */
@Slf4j
@Component
public class SimplenessProducer {


    private String producerGroup = "test_producer";

    private DefaultMQProducer producer;

    public SimplenessProducer() {
        start();
    }

    /**
     * 对象在使用之前必须要调用一次，只能初始化一次
     */
    public void start() {
        //示例生产者
        producer = new DefaultMQProducer(producerGroup);
        //不开启vip通道 开通口端口会减2
        producer.setVipChannelEnabled(false);
        //绑定name server
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
        try {
            this.producer.start();
        } catch (MQClientException e) {
            log.info(ExceptionUtils.getStackTrace(e));
        }
    }

    /**
     * 生产者生产方法
     * @param topic 主题
     * @param tags 标签，用来给消费者进行过滤的
     * @param keys 作为key
     * @param body 发送的内容
     */
    @SneakyThrows
    public SendResult producerSendMes(String topic, String tags, String keys, String body) {
        Message message = new Message(topic, tags, keys, body.getBytes(RemotingHelper.DEFAULT_CHARSET));
        //发送
        SendResult sendResult = this.producer.send(message);
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
        SendResult sendResult = this.producer.send(message);
        return sendResult;
    }

    public DefaultMQProducer getProducer(){
        return this.producer;
    }

    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    public void shutdown() {
        this.producer.shutdown();
    }
}
