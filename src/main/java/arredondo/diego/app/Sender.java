package arredondo.diego.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import entitites.Person;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.TimeoutException;

public class Sender {

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        try(Connection connection = factory.newConnection()){
            Channel channel = connection.createChannel();
            channel.queueDeclare("registerQueue", false, false, false, null);

            Scanner tec = new Scanner(System.in);
            Person person;

            String r = "";
            while(!r.equalsIgnoreCase("N")){
                person =  new Person();

                System.out.print("\nIntroduce the person ID:      ");
                person.setId(tec.nextInt());
                tec.nextLine(); // for flushing
                System.out.print("Introduce the person name:    ");
                person.setName(tec.nextLine());
                System.out.print("Introduce the person address: ");
                person.setAddress(tec.nextLine());


                //Create mapper
                ObjectMapper mapper = new ObjectMapper();
                //Object to JSON in String
                String jsonInString = mapper.writeValueAsString(person);

                channel.basicPublish("", "registerQueue", false, null, jsonInString.getBytes());

                System.out.println(person.getName() + " sent to be saved in database");

                System.out.print("Do you want to register another person? (type 'N' for NO): ");
                r = tec.nextLine();
            }
        }
    }
}
