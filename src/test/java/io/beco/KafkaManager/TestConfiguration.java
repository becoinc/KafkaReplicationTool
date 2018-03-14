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

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.rule.KafkaEmbedded;

import java.util.Arrays;

/**
 * TestConfiguration is a class that does...
 */
@Slf4j
public class TestConfiguration implements InitializingBean
{
    @Autowired
    private KafkaProperties kafkaProperties;

    @Autowired
    private KafkaEmbedded kafkaEmbedded;

    @Autowired
    private ConfigurableApplicationContext context;

    @Bean
    public KafkaAdmin kafkaAdmin()
    {
        kafkaProperties.setBootstrapServers( Arrays.asList( kafkaEmbedded.getBrokersAsString() ) );
        return new KafkaAdmin( kafkaProperties.buildAdminProperties() );
    }

    @Override
    public void afterPropertiesSet() throws Exception
    {
        TestPropertyValues.of( "zookeeper.url:" + kafkaEmbedded.getZookeeperConnectionString() )
                          .applyTo( context );
    }
}
