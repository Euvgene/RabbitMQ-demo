package consumer;

import com.rabbitmq.client.*;

import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;


public class ExchangeReceiverApp {
    private static final String EXCHANGE_NAME = "directExchanger";

    public static void main(String[] argv) throws Exception {
        boolean b = true;
        Set<String> stringSet = new HashSet<>();
        Scanner scanner = new Scanner(System.in);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String queueName = channel.queueDeclare().getQueue();

        showCommands();

        while (b) {
            String command = scanner.next();
            if (command.equals("stop")) {
                b = false;
                connection.close();
            } else {
                String title = scanner.next();
                switch (command) {
                    case "delete":
                        channel.queueUnbind(queueName, EXCHANGE_NAME, title);
                        stringSet.remove(title);
                        if (stringSet.isEmpty()) {
                            System.out.println("Nothing to listen. Set topic.");
                        } else {
                            System.out.println("Waiting for messages from");
                            stringSet.forEach(value -> System.out.println("-" + value));
                        }
                        break;
                    case "set_topic":
                        stringSet.add(title);
                        channel.queueBind(queueName, EXCHANGE_NAME, title);
                        System.out.println("Waiting for messages from:");
                        stringSet.forEach(value -> System.out.println("-" + value));

                        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                            String message = new String(delivery.getBody(), "UTF-8");
                            System.out.println("Message from " + title + " - '" + message + "'");
                        };
                        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
                        });
                        break;
                    default:
                        System.out.println("Wrong command!");
                        showCommands();
                }
            }
        }
        scanner.close();
    }

    private static void showCommands() {
        System.out.println("Commands:\n"
                + "'set_topic' - set topic to listen\n"
                + "'delete' - delete topic\n"
                + "'stop' - stop the app");
    }
}
