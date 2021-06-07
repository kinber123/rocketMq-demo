package com.rocketmq.demo.service.transaction;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;

/**
 * \* @author wcy
 * \* @date: 2020-05-21 9:23
 * \* Description:  实现事务会查监听器类
 * \
 */
@Slf4j
public class TransactionListenerImpl implements TransactionListener {
    private ConcurrentHashMap<String, Integer> localTran = new ConcurrentHashMap<String, Integer>();

    /**
     * 进行事务开始
     */
    private static final Integer BEGIN_LOCAL = 0;
    /**
     * 事务处理成功
     */
    private static final Integer SUCCESS_LOCAL = 1;
    /**
     * 事实处理异常
     */
    private static  final Integer EXCEPTION_LOCAL = 2;

    /**
     * - 发送prepare消息成功此方法被回调，该方法用于执行本地事务
     * - @param message 回传的消息，o利用transactionId即可获取到该消息的唯一Id
     * - @param  调用send方法时传递的参数，当send时候若有额外的参数可以传递到send方法中，这里能获取到
     * - @return 返回事务状态，COMMIT：提交 ROLLBACK：回滚 UNKNOW：回调
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message message, Object o) {
        /// 获取事务Id
        String transactionId = message.getTransactionId();
//        log.info("获取到事务消息Id：{}", transactionId);
        // 业务处理，执行本地事务处理
        try {
            localTran.put(transactionId, BEGIN_LOCAL);
//            log.info("正在执行行业事务处理------开始:{}",transactionId);
            Thread.sleep(1 * 1 * 1000);
//            log.info("正在执行行业事务处理------成功:{}",transactionId);
            localTran.put(transactionId, SUCCESS_LOCAL);
        } catch (InterruptedException e) {
            localTran.put(transactionId, EXCEPTION_LOCAL);
            log.error(ExceptionUtils.getStackTrace(e));
            // 事务重新执行
            return LocalTransactionState.ROLLBACK_MESSAGE;
        }
        return LocalTransactionState.COMMIT_MESSAGE;
    }

    /**
     * - @param messageExt 通过获取transactionId来判断这条消息的本地事务执行状态
     * - @return 返回事务状态，COMMIT：提交 ROLLBACK：回滚 UNKNOW：回调
     */
    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt messageExt) {
        String transactionId = messageExt.getTransactionId();
//        log.info("获取回调事务ID=={}",transactionId);
        Integer state = localTran.get(transactionId);
//        log.info("获取回调事务状态=={}",state);
        switch (state) {
            case 0:
                return LocalTransactionState.UNKNOW;
            case 1:
                return LocalTransactionState.COMMIT_MESSAGE;
            case 2:
                return LocalTransactionState.ROLLBACK_MESSAGE;

        }
        return LocalTransactionState.UNKNOW;
    }
}
