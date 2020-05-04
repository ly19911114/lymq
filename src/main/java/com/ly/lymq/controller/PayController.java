package com.ly.lymq.controller;

import com.ly.lymq.jms.JmsConfig;
import com.ly.lymq.jms.PayProducer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;

@RestController
public class PayController {

    @Autowired
    private PayProducer payProducer;

//    private static final String topic="xdclass_pay_test_topic2";

    @RequestMapping("api/v1/pay_cb")
    public Object callback(String text) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        //设定消息message的key做唯一标识，例如:可以把订单作为唯一标识的key
        Message message = new Message(JmsConfig.TOPIC, "taga", "6688",("hello ly rocketmq=" + text).getBytes());

        //发送消息
        SendResult sendResult = payProducer.getProducer().send(message);
        System.out.println(sendResult);
        return new HashMap<>();
    }
}
