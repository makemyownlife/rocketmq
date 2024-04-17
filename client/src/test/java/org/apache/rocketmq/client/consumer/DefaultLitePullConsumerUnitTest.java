package org.apache.rocketmq.client.consumer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.junit.Test;

import java.util.List;

public class DefaultLitePullConsumerUnitTest {

    @Test
    public void testAutoCommit() throws Exception {
        boolean running = true;
        // 定义消费者组 mygroup
        DefaultLitePullConsumer litePullConsumer = new
                DefaultLitePullConsumer("mygroup");
        // 设置名字服务地址
        litePullConsumer.setNamesrvAddr("127.0.0.1:9876");
        // 从最新的进度偏移量
        litePullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        // 订阅主题 TopicTest
        litePullConsumer.subscribe("TopicTest", "*");
        // 自动提交消费偏移量的选项设置为 true
        litePullConsumer.setAutoCommit(true);
        litePullConsumer.start();
        try {
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                System.out.printf("%s%n", messageExts);
            }
        } finally {
            litePullConsumer.shutdown();
        }
    }

    @Test
    public void testNoAutoCommit() throws MQClientException {
        boolean running = true;
        // 定义消费者组 mygroup
        DefaultLitePullConsumer litePullConsumer = new
                DefaultLitePullConsumer("mygroup");
        // 设置名字服务地址
        litePullConsumer.setNamesrvAddr("127.0.0.1:9876");
        // 从最新的进度偏移量
        litePullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        // 订阅主题 TopicTest
        litePullConsumer.subscribe("TopicTest", "*");
        // 自动提交消费偏移量的选项设置为 false
        litePullConsumer.setAutoCommit(false);
        litePullConsumer.start();
        try {
            while (running) {
                List<MessageExt> messageExts = litePullConsumer.poll();
                if (CollectionUtils.isNotEmpty(messageExts)) {
                    System.out.println("消息大小：" + messageExts.size());
                    // 取出第一条消息数据
                    MessageExt first = messageExts.get(0);
                    MessageQueue messageQueue = new MessageQueue(first.getTopic(), first.getBrokerName(), first.getQueueId());
                    for (MessageExt messageExt : messageExts) {
                        // 打印消息内存
                        System.out.println(new String(first.getBody()));
                        // 业务处理代码
                    }
                   litePullConsumer.commitSync();
                    // 回滚到第一条消息的点位
                 //   litePullConsumer.seek(messageQueue, first.getQueueOffset());
                }
                Thread.sleep(5000);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            litePullConsumer.shutdown();
        }


    }

}
