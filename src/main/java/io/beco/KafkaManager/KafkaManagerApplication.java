package io.beco.KafkaManager;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KafkaManagerApplication
{
    public static void main(String[] args)
    {
	    SpringApplication.run(KafkaManagerApplication.class, args);
	}
}
