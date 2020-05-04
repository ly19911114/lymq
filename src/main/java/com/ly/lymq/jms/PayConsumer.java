package com.ly.lymq.jms;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.springframework.stereotype.Component;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * @author 46993
 */
@Component
public class PayConsumer {


    private DefaultMQPushConsumer consumer;

    private String consumerGroup = "pay_consumer_group";

    public PayConsumer() throws MQClientException {
        // 用于把多个Consumer组织到一起，提高并发处理能力
        consumer = new DefaultMQPushConsumer(consumerGroup);
        // 设置nameServer地址
        consumer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        /**
         * 1. CONSUME_FROM_LAST_OFFSET：第一次启动从队列最后位置消费，后续再启动接着上次消费的进度开始消费
           2. CONSUME_FROM_FIRST_OFFSET：第一次启动从队列初始位置消费，后续再启动接着上次消费的进度开始消费
           3. CONSUME_FROM_TIMESTAMP：第一次启动从指定时间点位置消费，后续再启动接着上次消费的进度开始消费
                以上所说的第一次启动是指从来没有消费过的消费者，如果该消费者消费过，那么会在broker端记录该消费者的消费位置，
         如果该消费者挂了再启动，那么自动从上次消费的进度开始

         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        // 订阅主题以使用订阅;订阅topic，可以对指定消息进行过滤，仅支持或操作“||”,例如："TopicTest","tagl||tag2||tag3",*或null表示topic所有消息
        consumer.subscribe(JmsConfig.TOPIC, "*");

        //消费模式默认是CLUSTERING集群模式，把消费模式改为广播模式，测试消息重试只针对集群消费方式有效，广播方式不提供失败重试特性
//        consumer.setMessageModel(MessageModel.BROADCASTING);
        //设置消费模式为集群模式，如果不设置，默认也是集群模式
        consumer.setMessageModel(MessageModel.CLUSTERING);

//        consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
//            try {
//                Message msg = msgs.get(0);
//                System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));
//                String topic = msg.getTopic();
//                String body = new String(msg.getBody(), "utf-8");
//                String tags = msg.getTags();
//                String keys = msg.getKeys();
//                System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);
//                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
//            } catch (UnsupportedEncodingException e) {
//                e.printStackTrace();
//                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
//            }
//        });
        //创建监听
        //MessageListenerConcurrently对象用于同时接收异步传递的消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {

                MessageExt msg = msgs.get(0);
                Integer times=msg.getReconsumeTimes();
                System.out.println("重试次数:"+times);
                try {

                    System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), new String(msgs.get(0).getBody()));

                    String topic = msg.getTopic();
                    String body = new String(msg.getBody(), "utf-8");
                    String tags = msg.getTags();
                    String keys = msg.getKeys();

                    if (keys.equalsIgnoreCase("6688")){
                        throw new Exception();
                    }
                    System.out.println("topic=" + topic + ", tags=" + tags + ", keys=" + keys + ", msg=" + body);

                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                } catch (Exception e) {
                    System.out.println("消费异常");
                    //如果重试两次不成功，则记录，人工介入
                    if (times>=2){
                        System.out.println("重试次数大于2，记录数据库，发短信通知开发人员或者运营人员");
                        //TODO  记录数据库，发短信通知开发人员或者运营人员
                        //告诉broker，消息成功
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    e.printStackTrace();
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                }
            }
        });

        consumer.start();
        System.out.println("consumer start ...");
    }

}
