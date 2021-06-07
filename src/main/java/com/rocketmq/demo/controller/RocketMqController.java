package com.rocketmq.demo.controller;

import com.rocketmq.demo.config.JmsConfig;
import com.rocketmq.demo.service.simpleness.SimplenessProducer;
import com.rocketmq.demo.service.transaction.TransactionProducer;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * \* @author wcy
 * \* @date: 2020-05-15 17:30
 * \* Description:  类
 * \
 */
@Slf4j
@RestController
@RequestMapping(value = "/text")
public class RocketMqController {

    @Autowired
    private SimplenessProducer producer;
    @Autowired
    private TransactionProducer transactionProducer;

    private List<String> mesList;

    /**
     * 初始化消息
     */
    public RocketMqController() {
        mesList = new ArrayList<>();
        mesList.add("小小");
        mesList.add("爸爸");
        mesList.add("妈妈");
        mesList.add("爷爷");
        mesList.add("奶奶");
        mesList.add("外公");
        mesList.add("外婆");

    }


    /**
     * 事务消息MQ
     * @return
     */
    @RequestMapping("/rocketmq/tran")
    public Object callbackTran(){
        for (String s : mesList) {
            //创建生产信息
            SendResult sendResult = transactionProducer.producerSendMes(JmsConfig.TOPIC, "testtag", ("事务消息--小小一家人的称谓:" + s));
            log.info("事务消息输出生产者信息={}",sendResult);
        }
        return "成功";
    }

    /**
     * 测试是否能访问
     * @param test
     * @return
     * @throws Exception
     */
    @RequestMapping("/test")
    public Object test(String test) {
        return "success "+test;
    }
}
