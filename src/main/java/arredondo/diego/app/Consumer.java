package arredondo.diego.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import entitites.Person;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeoutException;

import static java.lang.Class.forName;
import static java.sql.DriverManager.getConnection;

public class Consumer {

    static String server = "LAPTOP-7911NSO5\\SQLEXPRESS"; //Nombre del servidor
    static String port = "1433"; //IP
    static String user = "sa"; //usuario loggin SQL Server
    static String password = "admin"; //Contraseña
    static String dataBase = "DistribuitedSystems"; //Nombre de la base de datos
    static String driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    static java.sql.Connection con = null;

    public static void main(String[] args) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();

        Channel channel = connection.createChannel();
        channel.queueDeclare("registerQueue", false, false, false, null);

        System.out.println("Listening for messages...");

        channel.basicConsume("registerQueue", true, (consumerTag, message) -> {
            String msg = new String(message.getBody(), "UTF-8");
            System.out.println("I just received a person: " + msg);

            // Here is where i receive my data

            //Create mapper
            ObjectMapper mapper = new ObjectMapper();
            //JSON from String to Object
            Person person = mapper.readValue(msg, Person.class);

            try {
                forName(driver).newInstance();
                String url = "jdbc:sqlserver://"+server+":"+port+";"+"databaseName="+dataBase+
                        ";user="+user+";password="+password+";";
                con = getConnection(url);

                /////////////////////////////////////////////////////////////////////

                String query = "INSERT INTO People VALUES ("+person.getId()+",'"+person.getName()+"','"+person.getAddress()+"');";

                Statement sentencia = con.createStatement();
                sentencia.executeQuery(query);

                System.out.println("The person '" + person.getName() + "' was registered succesfully");

            } catch (SQLException | ClassNotFoundException e) {
                System.out.println(e.getMessage());
            } catch (IllegalAccessException e) {
                System.out.println(e.getMessage());
            } catch (InstantiationException e) {
                System.out.println(e.getMessage());
            }


        }, consumerTag -> {});

    }

    private static void registerPerson(Person person){
        System.out.println("in method");
        try {
            forName(driver).newInstance();
            System.out.println("created instance");
            String url = "jdbc:sqlserver://"+server+":"+port+";"+"databaseName="+dataBase+
                    ";user="+user+";password="+password+";";
            con = getConnection(url);

            System.out.println("conected");

            /////////////////////////////////////////////////////////////////////

            String query = "INSERT INTO People VALUES ("+person.getId()+","+person.getName()+","+person.getAddress()+");";

            System.out.println("made query   " + query);

            Statement sentencia = con.createStatement();
            sentencia.executeQuery(query);

            System.out.println("The person '" + person.getName() + "' was registered succesfully");

        } catch (SQLException | ClassNotFoundException e) {
            System.out.println(e.getMessage());
        } catch (IllegalAccessException e) {
            System.out.println(e.getMessage());
        } catch (InstantiationException e) {
            System.out.println(e.getMessage());
        }

    }
}
