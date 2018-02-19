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

import kafka.admin.AdminUtils;
import kafka.admin.AdminUtils$;
import kafka.admin.ReassignPartitionsCommand;
import kafka.common.TopicAndPartition;
import kafka.utils.ZkUtils;
import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.security.JaasUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.util.Assert;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import scala.Int;
import scala.Tuple2;
import scala.collection.Seq;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * KafkaTopicController is a class that does...
 */
@Controller
@Slf4j
public class KafkaTopicController
{

    private final KafkaAdmin kafkaAdmin;

    private final AdminClient adminClient;

    private final ZkUtils zkUtils;

    private final String zkUrl;

    private Map< TopicAndPartition, List< Integer > > assignmentPlan;

    @Autowired
    public KafkaTopicController( @Value( "${zookeeper.url}" ) final String zkUrl,
                                 final KafkaAdmin kafkaAdmin )
    {
        this.kafkaAdmin  = kafkaAdmin;
        this.adminClient = AdminClient.create( kafkaAdmin.getConfig() );
        this.zkUrl       = zkUrl;

        final Tuple2< ZkClient, ZkConnection > zkClient
            = ZkUtils.createZkClientAndConnection( zkUrl,
                                                  30000,
                                                  30000 );
        this.zkUtils = new ZkUtils( zkClient._1, zkClient._2, JaasUtils.isZkSecurityEnabled() );

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

    @GetMapping( "/topic/{topicName}/describe" )
    public String describeTopic( @PathVariable String topicName,
                                 Model m ) throws InterruptedException, ExecutionException, TimeoutException
    {
        log.debug( "Describing Topic: {}", topicName );

        m.addAttribute( "topicName", topicName );

        final DescribeClusterResult dcr    = this.adminClient.describeCluster();
        final DescribeTopicsResult dtr = this.adminClient.describeTopics( Collections.singleton( topicName ) );

        final KafkaFuture< Void > topicData
            = KafkaFuture.allOf(
            dtr.all()
               .thenApply( new KafkaFuture.Function< Map< String, TopicDescription >, Map< String, TopicDescription > >()
            {
                @Override
                public Map< String, TopicDescription > apply( Map< String, TopicDescription > topicDescriptionMap )
                {
                    Assert.isTrue( topicDescriptionMap.size() == 1, "Only Single Topic Supported." );

                    final TopicDescription description = topicDescriptionMap.get( topicName );
                    m.addAttribute( "topicInfo", description );

                    final Map< Integer, Set< Integer > > partitionsToSetOfNodeIds =
                    description.partitions()
                               .stream()
                               .collect( Collectors.toMap( TopicPartitionInfo::partition,
                                                           tpi -> tpi.replicas()
                                                                     .stream()
                                                                     .map( Node::id )
                                                                     .collect( Collectors.toSet() ) ) );
                    m.addAttribute( "partitionsToSetOfNodeIds", partitionsToSetOfNodeIds );
                    return topicDescriptionMap;
                }
            } ),
            dcr.nodes()
               .thenApply( new KafkaFuture.Function< Collection<Node>, Object >()
           {
               @Override
               public Object apply( Collection< Node > nodes )
               {
                   final Set< Node > sortedNodes = new TreeSet<>( Comparator.comparingInt( Node::id ) );
                   sortedNodes.addAll( nodes );
                   m.addAttribute( "nodes", sortedNodes );
                   return null;
               }
           } )
        );

        topicData.get( 5, TimeUnit.SECONDS );

        log.debug( "Topic Desc: {}", m.asMap() );

        return "topicView";
    }

    @GetMapping( "/topic/{topicName}" )
    public String describeTopicBase( @PathVariable String topicName,
                                     Model m )
    throws InterruptedException, ExecutionException, TimeoutException
    {
        return this.describeTopic( topicName, m );
    }

    @GetMapping( "/topic/{topicName}/rebalance" )
    public String describeTopicFromRebalance( @PathVariable String topicName,
                                              Model m )
    throws InterruptedException, ExecutionException, TimeoutException
    {
        return this.describeTopic( topicName, m );
    }

    @PostMapping( "/topic/{topicName}/rebalance" )
    public String rebalanceTopic( @PathVariable String topicName,
                                  @RequestBody MultiValueMap< String, String > formData,
                                  Model m )
    throws InterruptedException, ExecutionException, TimeoutException
    {
        log.debug( "Selected Part to Brokers: {}", formData );

        final String operation = formData.getFirst( "operation" );
        final ReassignPartitionsCommand.Throttle throttle
            = new ReassignPartitionsCommand.Throttle( Long.parseLong( formData.getFirst( "throttle" ) ),
                                                      ReassignPartitionsCommand.Throttle.$lessinit$greater$default$2() );

        switch ( operation )
        {
            case "Execute":
                if ( zkUtils.pathExists( ZkUtils.ReassignPartitionsPath() ) )
                {
                    log.error("There is an existing assignment running.");
                }
                else
                {
                    this.assignmentPlan = buildAssignmentPlan( formData );
                }
                break;
            case "Verify":
            default:
                break;
        }

        m.addAttribute( "assignmentPlan", this.assignmentPlan );

        return this.describeTopic( topicName, m );
    }

    private Map< TopicAndPartition, List< Integer > > buildAssignmentPlan( MultiValueMap< String, String > formData )
    {
        final Map< TopicAndPartition, List< Integer > > retVal = new HashMap<>();

        for( Map.Entry< String, List< String > > item  : formData.entrySet() )
        {

        }

        return retVal;
    }

    /**
     *
     * @param zkUtils
     * @param partitionsToBeReassigned This is really the scala type: Map[TopicAndPartition, Seq[Int]]
     * @param throttle
     */
    private void executeAssignment( ZkUtils zkUtils,
                                    scala.collection.Map< TopicAndPartition, Seq< Object > > partitionsToBeReassigned,
                                    ReassignPartitionsCommand.Throttle throttle )
    {
        // This is really a Seq< scala.Int > but for some reason the type system doesn't
        // understand that.
        final ReassignPartitionsCommand reassignPartitionsCommand
            = new ReassignPartitionsCommand( zkUtils, partitionsToBeReassigned, AdminUtils$.MODULE$ );

        // If there is an existing rebalance running, attempt to change its throttle
        if ( zkUtils.pathExists( ZkUtils.ReassignPartitionsPath() ) )
        {
            log.error("There is an existing assignment running.");
            reassignPartitionsCommand.maybeLimit(throttle);
        }
        else
        {
            if ( throttle.value() >= 0 )
            {
                log.warn( "Warning: You must run Verify periodically, until the reassignment completes, "
                          + " to ensure the throttle is removed. "
                          + "You can also alter the throttle by rerunning the Execute command passing a new value." );
            }

            if (reassignPartitionsCommand.reassignPartitions(throttle))
            {
                log.info( "Successfully started reassignment of partitions." );
            }
            else
            {
                log.error( "Failed to reassign partitions {}", partitionsToBeReassigned );
            }
        }
    }
}
