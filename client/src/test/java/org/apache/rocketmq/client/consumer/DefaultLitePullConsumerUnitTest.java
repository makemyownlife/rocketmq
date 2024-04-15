package org.apache.rocketmq.client.consumer;

import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
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

}
