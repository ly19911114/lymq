package com.ly.lymq.jms;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.stereotype.Component;

@Component
public class PayProducer {
    //生产者所在的组
    private String producerGroup = "pay_group";
//    private String nameServerAddr="203.195.237.107:9876";

    //此类是打算发送消息的应用程序的入口点。
    private DefaultMQProducer producer;

    public DefaultMQProducer getProducer() {
        return producer;
    }


    public PayProducer() throws MQClientException {
        //DefaultMQProducer的构造函数会指定生产者组
        producer = new DefaultMQProducer(producerGroup);
        //指定NameserAddr地址，多个地址以;隔开
        producer.setNamesrvAddr(JmsConfig.NAME_SERVER);
        //构造函数中进行调用
        start();
    }

    /**
     *
     * @throws MQClientException
     */
    public void start() throws MQClientException {
        //启动此生产者实例。
        //源码中的注释：需要执行许多内部初始化过程来准备此实例，因此，必须*在发送或查询消息之前调用此方法。
        // 对象在使用之前必须调用一次，只能初始化一次
        this.producer.start();
    }

    /**
     * 一般在应用上下文，使用上下文监听器，进行关闭
     */
    public void shutdown() {
        this.producer.shutdown();
    }
}
