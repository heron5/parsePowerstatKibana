package com.ronny;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import org.elasticsearch.client.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;



import org.apache.http.HttpHost;

public class Main {

    public void updateDb(String source, float v1, float v2, float v3, float p1, float p2, float p3,
                         float i1, float i2, float i3, String timeOffset, String host, String port, int loggLevel) {

        SensorProperties sensor = new SensorProperties();
        if (!sensor.getSensorProperties(source))
            return;

        long sourceId = sensor.sourceId;
        String description = sensor.shortDescription;
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss" + timeOffset.trim());
        LocalDateTime now = LocalDateTime.now();

        JSONObject jsonBody = new JSONObject();
        JSONObject jsonTags = new JSONObject();
        JSONObject jsonFields = new JSONObject();

        jsonTags.put("source", sourceId);
        jsonTags.put("source_desc", description);
        jsonFields.put("v1", v1);
        jsonFields.put("v2", v2);
        jsonFields.put("v3", v3);
        jsonFields.put("p1", p1);
        jsonFields.put("p2", p2);
        jsonFields.put("p3", p3);
        jsonFields.put("i1", i1);
        jsonFields.put("i2", i2);
        jsonFields.put("i3", i3);

        jsonBody.put("measurement", "PowerStat");
        jsonBody.put("date", dtf.format(now));
        jsonBody.put("tags", jsonTags);
        jsonBody.put("fields", jsonFields);

        if (loggLevel > 1)
            System.out.println(jsonBody);

        HttpHost esHost = new HttpHost(host, Integer.parseInt(port));
        RestClient restClient = RestClient.builder(esHost).build();

        Request request = new Request(
                "POST",
                "/powerstat_index/powerstat");

        request.setJsonEntity(jsonBody.toString());

        Response response = null;
        try {
            response = restClient.performRequest(request);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                restClient.close();
            } catch (IOException closeEx){
                closeEx.printStackTrace();
            }

            if (loggLevel > 0)
                System.out.println(response);

        }
    }


    public void run() {
        System.out.println("TopicSubscriber initializing...");
        System.out.println("Get properties from file...");

        String host = "";
        String username = "";
        String password = "";
        String topic = "";
        String timeOffset = "";
        String kibanaHost = "";
        String kibanaPort = "";
        int loggLevel = 0;


        try (InputStream input = new FileInputStream("parsePowerstatKibana.properties")) {

            Properties prop = new Properties();

            // load a properties file
            prop.load(input);

            // get the property value and print it out
            host = prop.getProperty("mqttHost");
            username = prop.getProperty("mqttUsername");
            password = prop.getProperty("mqttPassword");
            topic = prop.getProperty("mqttTopic");
            timeOffset = prop.getProperty("timeOffset");
            kibanaHost = prop.getProperty("kibanaHost");
            kibanaPort = prop.getProperty("kibanaPort");
            loggLevel = Integer.parseInt( prop.getProperty("loggLevel"));
            System.out.println("Logglevel: "+ loggLevel);

        } catch (IOException ex) {
            ex.printStackTrace();
        }



        try {
            // Create an Mqtt client
            Random rand = new Random();
            MqttClient mqttClient = new MqttClient(host, "parsePowerstat" + rand.toString());
            MqttConnectOptions connOpts = new MqttConnectOptions();
            connOpts.setCleanSession(true);
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());
            connOpts.setAutomaticReconnect(true);

            // Connect the client
            System.out.println("Connecting to Kjuladata messaging at " + host);
            mqttClient.connect(connOpts);
            System.out.println("Connected");

            // Topic filter the client will subscribe to
            final String subTopic = topic;

            // Callback - Anonymous inner-class for receiving messages
            String finalTimeOffset = timeOffset;
            String finalKibanaHost = kibanaHost;
            String finalKibanaPort = kibanaPort;
            int finalLoggLevel = loggLevel;
            mqttClient.setCallback(new MqttCallback() {
                public void messageArrived(String topic, MqttMessage message) throws Exception {
                    // Called when a message arrives from the server that
                    // matches any subscription made by the client
                    String source = "0";
                    if (topic.equals("logger/powerstat/heron5"))
                        source = "5";
                    if (topic.equals("logger/powerstat/hagtorn"))
                        source = "1";

                    JSONParser parser = new JSONParser();
                    String payLoad = new String(message.getPayload());

                    if (finalLoggLevel > 1)
                        System.out.println(topic);
                        System.out.println(payLoad);

                    try {
                        JSONObject json = (JSONObject) parser.parse(payLoad);
                        float v1 = Float.parseFloat((String) json.get("v1"));
                        float v2 = Float.parseFloat((String) json.get("v2"));
                        float v3 = Float.parseFloat((String) json.get("v3"));
                        float p1 = Float.parseFloat((String) json.get("p1"));
                        float p2 = Float.parseFloat((String) json.get("p2"));
                        float p3 = Float.parseFloat((String) json.get("p3"));
                        float i1 = Float.parseFloat((String) json.get("i1"));
                        float i2 = Float.parseFloat((String) json.get("i2"));
                        float i3 = Float.parseFloat((String) json.get("i3"));

                        updateDb(source, v1, v2, v3, p1, p2, p3, i1,i2, i3,
                                finalTimeOffset, finalKibanaHost, finalKibanaPort, finalLoggLevel);

                    } catch (Exception pe) {
                        //  System.out.println("position: " + pe.getPosition());
                        System.out.println(pe);
                    }
                }

                public void connectionLost(Throwable cause) {
                    System.out.println("Connection to KjulaData messaging lost!" + cause.getMessage());
                }

                public void deliveryComplete(IMqttDeliveryToken token) {
                }

            });

            // Subscribe client to the topic filter and a QoS level of 0
            System.out.println("Subscribing client to topic: " + subTopic);
            mqttClient.subscribe(subTopic, 0);
            System.out.println("Subscribed");

        } catch (MqttException me) {
            System.out.println("reason " + me.getReasonCode());
            System.out.println("msg " + me.getMessage());
            System.out.println("loc " + me.getLocalizedMessage());
            System.out.println("cause " + me.getCause());
            System.out.println("excep " + me);
            me.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new Main().run();

    }
}
