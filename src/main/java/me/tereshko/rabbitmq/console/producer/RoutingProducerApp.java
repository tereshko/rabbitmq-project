package me.tereshko.rabbitmq.console.producer;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Scanner;

public class RoutingProducerApp {
    private static final String EXCHANGE_NAME = "topic_exchange";

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");

        try (Connection connection = factory.newConnection(); Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

            boolean unlimitedMessages = false;

            while (!unlimitedMessages) {
                String adminMessage = getMessage();

                if (adminMessage.equals(":q")) {
                    unlimitedMessages = true;
                } else {
                    String[] splitMessageText = adminMessage.split("\\s+");

                    int splitSize = splitMessageText.length;

                    if (splitSize <= 1) {
                        unlimitedMessages = true;
                    } else {
                        String routingKey = splitMessageText[0];

                        String message = "";

                        for (int i = 1; i <= splitSize - 1; i++) {
                            message = message + " " + splitMessageText[i];
                        }

                        message = message.substring(1); // remove first space

                        channel.basicPublish(EXCHANGE_NAME, routingKey, null, message.getBytes("UTF-8"));
                        System.out.println(" [x] Sent '" + routingKey + "':'" + message + "'");
                    }
                }
            }
        }
    }

    private static String getMessage() {
        System.out.println("For exit send :q");
        System.out.println("Write the message. Format: <channel> <message>");
        Scanner scanner = new Scanner(System.in);
        String line = scanner.nextLine();
        return line;
    }
}
