package com.ker.springboot.kafka.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

@Service
public class Producer {

    private static final Logger logger = LoggerFactory.getLogger(Producer.class);
    private static final String TOPIC = "users";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message) {

        logger.info("Producing message: [{}]", message);

        ListenableFuture<SendResult<String, String>>  result = this.kafkaTemplate.send(TOPIC, message);


        // Result handler
        result.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onFailure(Throwable throwable) {
                logger.error("Unable to send message = [ {} ] due to : {}", message, throwable.getMessage());
            }

            @Override
            public void onSuccess(SendResult<String, String> res) {

                logger.info("Sent message = [ {} ] with offset : {}", message, res.getRecordMetadata().offset());

            }
        });
    }
}