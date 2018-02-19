/**
 * @file TestConfiguration.java
 * <p>
 * Copyright (C) 2018 by Beco Inc. All Rights Reserved.
 * See included LICENSE file with this for terms of use.
 * <p>
 * Any use, compilation, or distribution of this source code constitutes consent to the
 * terms and conditions in the license file.
 * @date 2/18/18 9:20 PM
 * @author jzampieron
 */

package io.beco.KafkaManager;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Arrays;

/**
 * TestConfiguration is a class that does...
 */
public class TestConfiguration
{
    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private KafkaEmbedded kafkaEmbedded;

    @Bean
    public KafkaAdmin kafkaAdmin()
    {
        kafkaProperties.setBootstrapServers( Arrays.asList( kafkaEmbedded.getBrokersAsString() ) );
        return new KafkaAdmin( kafkaProperties.buildAdminProperties() );
    }
}
