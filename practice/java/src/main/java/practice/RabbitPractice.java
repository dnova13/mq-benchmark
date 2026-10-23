package practice;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.MessageProperties;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * RabbitMQ 최소 예제 (amqp-client)
 * <pre>
 * ./gradlew run -Pex=RabbitPractice --args="produce 5"   practice-queue에 persistent 메시지 발행
 * ./gradlew run -Pex=RabbitPractice --args="consume"     prefetch 10으로 받아 처리 후 ack (Ctrl+C로 종료)
 * ./gradlew run -Pex=RabbitPractice --args="fanout"      fanout exchange에 1건 → 묶인 큐 2개가 모두 받음
 * ./gradlew run -Pex=RabbitPractice --args="info"        큐의 메시지 수, 컨슈머 수
 * </pre>
 */
public class RabbitPractice {
    static final String QUEUE = "practice-queue";

    public static void main(String[] args) throws Exception {
        String cmd = args.length > 0 ? args[0] : "info";
        ConnectionFactory f = new ConnectionFactory();
        f.setUri(System.getenv().getOrDefault("RABBITMQ_URL", "amqp://guest:guest@localhost:35672"));
        try (Connection conn = f.newConnection(); Channel ch = conn.createChannel()) {
            ch.queueDeclare(QUEUE, true, false, false, null); // durable: 브로커가 재시작해도 큐가 남는다
            switch (cmd) {
                case "produce" -> produce(ch, args.length > 1 ? Integer.parseInt(args[1]) : 5);
                case "consume" -> consume(ch);
                case "fanout" -> fanout(ch);
                default -> info(ch);
            }
        }
    }

    static void produce(Channel ch, int count) throws Exception {
        for (int i = 1; i <= count; i++) {
            String body = "{\"orderId\":" + i + ",\"createdAt\":" + System.currentTimeMillis() + "}";
            // 기본 exchange("")로 보내면 routing key = 큐 이름. PERSISTENT_TEXT_PLAIN: 디스크에 저장
            ch.basicPublish("", QUEUE, MessageProperties.PERSISTENT_TEXT_PLAIN, body.getBytes(StandardCharsets.UTF_8));
            System.out.println("발행 → " + QUEUE + " " + body);
        }
    }

    static void consume(Channel ch) throws Exception {
        ch.basicQos(10); // ack하지 않은 메시지를 최대 10건까지만 받는다
        DeliverCallback onMessage = (tag, d) -> {
            System.out.println("받음 " + new String(d.getBody(), StandardCharsets.UTF_8) + " redelivered=" + d.getEnvelope().isRedeliver());
            ch.basicAck(d.getEnvelope().getDeliveryTag(), false); // ack해야 큐에서 사라진다
        };
        ch.basicConsume(QUEUE, false, onMessage, tag -> { }); // autoAck=false: 직접 ack
        System.out.println(QUEUE + " 대기 중... (Ctrl+C로 종료)");
        Thread.currentThread().join();
    }

    static void fanout(Channel ch) throws Exception {
        String ex = "practice.fanout";
        ch.exchangeDeclare(ex, BuiltinExchangeType.FANOUT, true);
        for (String q : List.of("practice-email", "practice-sms")) {
            ch.queueDeclare(q, true, false, false, null);
            ch.queueBind(q, ex, ""); // fanout은 routing key를 무시한다
        }
        ch.basicPublish(ex, "", MessageProperties.PERSISTENT_TEXT_PLAIN, "user-signed-up".getBytes(StandardCharsets.UTF_8));
        Thread.sleep(200);
        for (String q : List.of("practice-email", "practice-sms")) {
            System.out.println(q + ": " + ch.queueDeclarePassive(q).getMessageCount() + "건");
        }
    }

    static void info(Channel ch) throws Exception {
        AMQP.Queue.DeclareOk ok = ch.queueDeclarePassive(QUEUE);
        System.out.println(QUEUE + ": messages=" + ok.getMessageCount() + " consumers=" + ok.getConsumerCount());
    }
}
