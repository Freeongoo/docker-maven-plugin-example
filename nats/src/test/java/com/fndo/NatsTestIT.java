package com.fndo;

import io.nats.client.*;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class NatsTestIT {

    @Test
    public void test() throws Exception {
        Connection natsConnection = initConnection();

        SyncSubscription fooSubscription = natsConnection.subscribe("foo.bar");
        SyncSubscription barSubscription = natsConnection.subscribe("bar.foo");
        natsConnection.publish("foo.bar", "bar.foo", "hello there".getBytes());

        Message message = fooSubscription.nextMessage();
        assertNotNull("No message!", message);
        assertEquals("hello there", new String(message.getData()));

        natsConnection
                .publish(message.getReplyTo(), message.getSubject(), "hello back".getBytes());

        message = barSubscription.nextMessage();
        assertNotNull("No message!", message);
        assertEquals("hello back", new String(message.getData()));
    }

    private Connection initConnection() throws IOException {
        Options options = new Options.Builder()
                .errorCb(ex -> System.out.println("Connection Exception: " + ex))
                .disconnectedCb(event -> System.out.println("Channel disconnected: {}" + event.getConnection()))
                .reconnectedCb(event -> System.out.println("Reconnected to server: {}" + event.getConnection()))
                .build();

        return Nats.connect("nats://localhost:4222", options);
    }
}
