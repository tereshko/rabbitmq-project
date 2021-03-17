package me.tereshko.rabbitmq.console.consumer;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class RoutingConsumerApp {
    private static final String EXCHANGE_NAME = "topic_exchange";
    private static List<String> routingKeys = new ArrayList<String>();

    public static void main(String[] argv) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.TOPIC);

        String queueName = channel.queueDeclare().getQueue();

        binding(channel, queueName);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };
        channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
        });

        while (true) {
            Scanner scanner = new Scanner(System.in);
            String commandFromUser = scanner.nextLine();
            if (commandFromUser.contains("set_topic")) {
                String routingKey = getRoutingKey(commandFromUser);
                routingKeys.add(routingKey);
                binding(channel, queueName, routingKey);
            }
            if (commandFromUser.contains("my_topics")) {
                showMyTopics();
            }
            if (commandFromUser.contains("remove_topic")) {
                String routingKey = getRoutingKey(commandFromUser);
                routingKeys.remove(routingKey);
                unBinding(channel,queueName,routingKey);
            }
        }

    }

    public static void binding(Channel channel,String queueName) {
        String routingKey = getRoutingKey();
        try {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(" [*] Waiting for messages with routing key (" + routingKey + "):");
    }

    public static void binding(Channel channel,String queueName, String routingKey) {
        try {
            channel.queueBind(queueName, EXCHANGE_NAME, routingKey);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(" [*] Waiting for messages with routing key (" + routingKey + "):");
    }

    public static String getRoutingKey() {
        String routingKey = "";
        System.out.println("Please set key: set_topic <key name>");
        Scanner scanner = new Scanner(System.in);
        String commandFromUser = scanner.nextLine();

        String[] splitMessageText = commandFromUser.split("\\s+");
        if (splitMessageText[0].equals("set_topic")) {
            routingKey = splitMessageText[1];
            System.out.println("routing: " + routingKey);
            routingKeys.add(routingKey);
        } else {
            System.out.println("command is not recognized. please try again");
        }
        return routingKey;
    }

    public static String getRoutingKey(String commandFromUser) {
        String routingKey = "";
        String[] splitMessageText = commandFromUser.split("\\s+");
        if (splitMessageText[0].equals("set_topic") || splitMessageText[0].equals("remove_topic")) {
            routingKey = splitMessageText[1];
        } else {
            System.out.println("command is not recognized. please try again");
        }
        return routingKey;
    }

    public static void showMyTopics() {
        routingKeys.forEach((temp) -> System.out.println(temp));
    }

    public static void unBinding(Channel channel,String queueName, String routingKey) {
        try {
            channel.queueUnbind(queueName, EXCHANGE_NAME, routingKey);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
