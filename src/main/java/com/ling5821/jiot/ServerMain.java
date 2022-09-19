package com.ling5821.jiot;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttServerOptions;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;

/**
 * @author lsj
 * @date 2022-06-21 15:06
 */
@Slf4j
public class ServerMain {
    public static final Map<String, MqttEndpoint> connections = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        int port = 1883;
        Vertx vertx = Vertx.vertx();
        MqttServerOptions options = new MqttServerOptions();
        MqttServer mqttServer = MqttServer.create(vertx, options);
        mqttServer.endpointHandler(endpoint -> {
            /*
               如果清除会话每次都覆盖缓存
             */
            if (endpoint.isCleanSession()) {

                endpoint.accept();
                MqttEndpoint previous = connections.put(endpoint.clientIdentifier(), endpoint);
                if (previous != null) {
                    //如果存在上一个连接则关闭
                    previous.close();
                }
            } else {
                /*
                  如果不清除会话判断是否已经存在会话
                 */
                MqttEndpoint existsEndpoint = connections.get(endpoint.clientIdentifier());
                /*
                  不存在会话则缓存该会话
                 */
                if (existsEndpoint == null) {
                    endpoint.accept();
                    connections.put(endpoint.clientIdentifier(), endpoint);
                } else {
                    existsEndpoint.close();
                    //提取该会话的相关数据到新的会话 TODO
                    existsEndpoint.accept(true);
                }

            }
            endpoint.pingHandler(ignore -> {
                log.info("pingHandler");
            }).publishAcknowledgeHandler(event -> {
                log.info("publishAcknowledgeHandler {}", event);
            }).publishAcknowledgeMessageHandler(message -> {
                log.info("publishAcknowledgeMessageHandler {}", message);
            }).publishCompletionHandler(event -> {
                log.info("publishCompletionHandler {}", event);
            }).publishCompletionMessageHandler(message -> {
                log.info("publishCompletionMessageHandler {}", message);
            }).publishHandler(message -> {
                log.info("publishHandler {}", message);
            }).publishReceivedHandler(event -> {
                log.info("publishReceivedHandler {}", event);
            }).publishReceivedMessageHandler(message -> {
                log.info("publishReceivedMessageHandler {}", message);
            }).publishReleaseHandler(event -> {
                log.info("publishReleaseHandler {}", event);
            }).publishReleaseMessageHandler(message -> {
                log.info("publishReleaseMessageHandler {}", message);
            }).subscribeHandler(message -> {
                log.info("subscribeHandler {}", message);
            }).unsubscribeHandler(message -> {
                log.info("unsubscribeHandler {}", message);
            }).disconnectHandler(ignore -> {
                log.info("disconnectHandler");
            }).disconnectMessageHandler(ignore -> {
                log.info("disconnectMessageHandler");
            }).closeHandler(ignore -> {
                log.info("closeHandler");
                if (endpoint.isCleanSession()) {
                    connections.remove(endpoint.clientIdentifier());
                }
            }).exceptionHandler(error -> {
                log.info("exceptionHandler");
            });

        }).exceptionHandler(error -> log.error(error.getMessage(), error));
        mqttServer.listen(port, result -> {
            if (result.succeeded()) {
                log.info("listener port {}", port);
            } else {
                log.error("listener error", result.cause());
            }
        });
    }

}
