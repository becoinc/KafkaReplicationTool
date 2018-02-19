/**
 * @file KafkaTopicController.java
 * <p>
 * Copyright (C) 2018 by Beco Inc. All Rights Reserved.
 * See included LICENSE file with this for terms of use.
 * <p>
 * Any use, compilation, or distribution of this source code constitutes consent to the
 * terms and conditions in the license file.
 * @date 2/18/18 8:29 PM
 * @author jzampieron
 */

package io.beco.KafkaManager;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * KafkaTopicController is a class that does...
 */
@Controller
@Slf4j
public class KafkaTopicController
{

    private final KafkaAdmin kafkaAdmin;

    private final AdminClient adminClient;

    @Autowired
    public KafkaTopicController( final KafkaAdmin kafkaAdmin )
    {
        this.kafkaAdmin  = kafkaAdmin;
        this.adminClient = AdminClient.create( kafkaAdmin.getConfig() );
        log.info( "Kafka Client Properties: {}", kafkaAdmin.getConfig() );
    }

    @GetMapping( "/" )
    public String index( Model m ) throws InterruptedException, ExecutionException, TimeoutException
    {
        final DescribeClusterResult dcr    = this.adminClient.describeCluster();
        final ListTopicsResult      topics = this.adminClient.listTopics();

        final KafkaFuture< Void > clusterInfo
            = KafkaFuture.allOf( dcr.clusterId()
                                    .thenApply( new KafkaFuture.Function< String, Object >()
                                    {
                                        @Override
                                        public Object apply( String clusterId )
                                        {
                                            m.addAttribute( "clusterId", clusterId );
                                            return clusterId;
                                        }
                                    } ),
                                 dcr.controller()
                                    .thenApply( new KafkaFuture.Function< Node, Object >()
                                 {
                                     @Override
                                     public Object apply( Node node )
                                     {
                                         m.addAttribute( "controller", node );
                                         return node;
                                     }
                                 } ),
                                 dcr.nodes()
                                    .thenApply( new KafkaFuture.Function< Collection<Node>, Object >()
                                 {
                                     @Override
                                     public Object apply( Collection< Node > nodes )
                                     {
                                         m.addAttribute( "nodes", nodes );
                                         return nodes;
                                     }
                                 } ),
                                 topics.namesToListings()
                                        .thenApply( new KafkaFuture.Function< Map<String,TopicListing>, Object >()
                                    {
                                        @Override
                                        public Object apply( Map< String, TopicListing > map )
                                        {
                                            m.addAttribute( "topicListings", map.values() );
                                            m.addAttribute( "topicNames", map.keySet() );
                                            return null;
                                        }
                                    } )
                                 );

        clusterInfo.get( 5, TimeUnit.SECONDS );

        log.debug( "Model Attributes: {}", m.asMap() );

        return "index";
    }
}
