package producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;


public class ExchangeSenderApp {
    private static final String EXCHANGE_NAME = "directExchanger";

    public static void main(String[] argv) throws Exception {
        boolean b = true;
        Scanner scanner = new Scanner(System.in);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
            while (b) {
                String title = scanner.next();
                if (title.equals("stop")) {
                    b = false;
                } else {
                    String message = scanner.nextLine().substring(1);
                    channel.basicPublish(EXCHANGE_NAME, title, null, message.getBytes("UTF-8"));
                    System.out.println(" [x] Sent '" + message + "'");
                }
            }
            scanner.close();
        }
    }
}