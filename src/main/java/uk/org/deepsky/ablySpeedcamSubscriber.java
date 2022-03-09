package uk.org.deepsky;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import java.util.Base64;
import java.util.Properties;

import io.ably.lib.realtime.AblyRealtime;
import io.ably.lib.realtime.Channel;
import io.ably.lib.types.AblyException;
import io.ably.lib.types.Message;

public class ablySpeedcamSubscriber {
    static Properties props = new Properties();
    public static void main(String[] args) {

        try(InputStream configStream = new FileInputStream("config.properties"))
        {
            props.load(configStream);

        } catch (IOException ex)
        {
            ex.printStackTrace();
        }

        try {
            initAbly();
        } catch (AblyException e) {
            e.printStackTrace();
        }


    }

    private static void initAbly() throws AblyException {
        AblyRealtime ablyRealtime = new AblyRealtime(props.getProperty("ably.api_key"));
        Channel channel = ablyRealtime.channels.get(props.getProperty("ably.channel"));
        channel.subscribe(new Channel.MessageListener() {
            @Override
            public void onMessage(Message messages) {
                System.out.println("Message received: " + messages.data);
                try {
                    sendToElasticsearch(messages.data.toString());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        });
    }

    private static void sendToElasticsearch(String message) throws Exception {

        URL url = new URL("http://" + props.getProperty("es.host") + ":" + props.getProperty("es.port") + "/" + props.getProperty("channel") + "/_doc/");
        HttpURLConnection myURLConnection = (HttpURLConnection) url.openConnection();

        String userCredentials = props.getProperty("es.user") + ":" + props.getProperty("es.password");
        String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userCredentials.getBytes()));

        myURLConnection.setRequestProperty("Authorization", basicAuth);

        myURLConnection.setRequestMethod("POST");
        myURLConnection.setDoOutput(true);
        myURLConnection.setRequestProperty("Accept", "application/json");
        myURLConnection.setRequestProperty("Content-Type", "application/json");

        byte[] out = message.getBytes(StandardCharsets.UTF_8);

        OutputStream stream = myURLConnection.getOutputStream();
        stream.write(out);

        System.out.println(myURLConnection.getResponseCode() + " " + myURLConnection.getResponseMessage());
        myURLConnection.disconnect();

    }

}

