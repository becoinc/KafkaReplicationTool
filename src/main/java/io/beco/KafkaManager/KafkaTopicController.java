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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import kafka.admin.ReassignPartitionsCommand;
import kafka.admin.ReassignPartitionsCommand$;
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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import scala.Option;
import scala.Tuple2;

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

    private final AdminClient adminClient;

    private final Option< AdminClient > adminClientOption;

    private final ZkUtils zkUtils;

    private TopicPartitionAssignment assignmentPlan;

    private ObjectMapper om;

    @Autowired
    public KafkaTopicController( @Value( "${zookeeper.url}" ) final String zkUrl,
                                 final KafkaAdmin kafkaAdmin,
                                 final ObjectMapper objectMapper )
    {
        this.adminClient       = AdminClient.create( kafkaAdmin.getConfig() );
        this.adminClientOption = Option.apply( this.adminClient );
        this.om                = objectMapper;

        this.om.enable( SerializationFeature.INDENT_OUTPUT );

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

        clusterInfo.get( 25, TimeUnit.SECONDS );

        log.debug( "Model Attributes: {}", m.asMap() );

        return "index";
    }

    @GetMapping( "/topic/{topicName}/describe" )
    public String describeTopic( @PathVariable String topicName,
                                 Model m ) throws InterruptedException, ExecutionException, TimeoutException
    {
        log.debug( "Describing Topic: {}", topicName );

        m.addAttribute( "topicName", topicName );

        final DescribeClusterResult dcr = this.adminClient.describeCluster();
        final DescribeTopicsResult  dtr = this.adminClient.describeTopics( Collections.singleton( topicName ) );

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

        topicData.get( 25, TimeUnit.SECONDS );

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
    throws InterruptedException, ExecutionException, TimeoutException, JsonProcessingException
    {
        log.debug( "Selected Part to Brokers: {}", formData );

        final String operation = formData.getFirst( "operation" );

        long throttleVal = 0;
        try
        {
            throttleVal = Long.parseLong( formData.getFirst( "throttle" ) );
            throttleVal *= 1024; // we ask for input in KiBps
        }
        catch( NumberFormatException nfe )
        {
            log.error( "Invalid Number Format for throttle... using default of 0." );
        }

        final ReassignPartitionsCommand.Throttle throttle
            = new ReassignPartitionsCommand.Throttle( throttleVal,
                                                      ReassignPartitionsCommand$.MODULE$.NoThrottle().postUpdateAction() );

        String assignmentPlanJson = "";

        switch ( operation )
        {
            case "Execute":
                if ( zkUtils.pathExists( ZkUtils.ReassignPartitionsPath() ) )
                {
                    log.error("There is an existing assignment running.");
                }
                else
                {
                    final TopicPartitionAssignment requested
                        = convertToTopicPartitionAssignment( buildAssignmentPlan( topicName, formData ) );
                    final TopicPartitionAssignment current
                        = this.getCurrentTopicConfiguration( topicName );

                    this.assignmentPlan = TopicPartitionAssignment.findAssignmentChanges( requested, current );

                    assignmentPlanJson  = om.writeValueAsString( this.assignmentPlan );
                    log.debug( "Assignment Plan: {}", assignmentPlanJson );
                    ReassignPartitionsCommand$.MODULE$.executeAssignment( this.zkUtils,
                                                                          this.adminClientOption,
                                                                          assignmentPlanJson,
                                                                          throttle,
                                                                          10000L );
                }
                break;
            case "Verify":
            default:
                if( this.assignmentPlan != null )
                {
                    assignmentPlanJson = om.writeValueAsString( this.assignmentPlan );
                    ReassignPartitionsCommand$.MODULE$.verifyAssignment( this.zkUtils,
                                                                         this.adminClientOption,
                                                                         assignmentPlanJson );
                }
                else
                {
                    // Nothing to verify.
                }
                break;
        }

        m.addAttribute( "assignmentPlan", this.assignmentPlan );
        m.addAttribute( "assignmentPlanJson", assignmentPlanJson );

        return this.describeTopic( topicName, m );
    }

    private TopicPartitionAssignment getCurrentTopicConfiguration( final String topicName )
    throws InterruptedException, ExecutionException, TimeoutException
    {
        final TopicPartitionAssignment tpa = new TopicPartitionAssignment();

        final DescribeTopicsResult  dtr = this.adminClient.describeTopics( Collections.singleton( topicName ) );

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

                       final Map< Integer, Set< Integer > > partitionsToSetOfNodeIds =
                           description.partitions()
                                      .stream()
                                      .collect( Collectors.toMap( TopicPartitionInfo::partition,
                                                                  tpi -> tpi.replicas()
                                                                            .stream()
                                                                            .map( Node::id )
                                                                            .collect( Collectors.toSet() ) ) );

                       partitionsToSetOfNodeIds.forEach( ( partition, nodeIdSet ) -> tpa.add( topicName, partition, nodeIdSet ) );

                       return topicDescriptionMap;
                   }
               } )
        );

        topicData.get( 25, TimeUnit.SECONDS );

        return tpa;
    }

    private MultiValueMap< TopicAndPartition, Integer >
        buildAssignmentPlan( final String topicName,
                             final MultiValueMap< String, String > formData )
    {
        final MultiValueMap< TopicAndPartition, Integer > retVal = new LinkedMultiValueMap<>();

        for( Map.Entry< String, List< String > > item  : formData.entrySet() )
        {
            final String key = item.getKey();
            if( key.startsWith( "partitionAssignment-" ) )
            {
                final List< String > partitionBrokerIdStr = item.getValue();
                // These should all be 1 element lists.
                Assert.isTrue( partitionBrokerIdStr.size() == 1, "List of Broker Ids must be 1." );
                final String[] partitionBrokerIdCsv = partitionBrokerIdStr.get( 0 ).split( "," );

                final int partitionNum = Integer.parseInt( partitionBrokerIdCsv[ 0 ] );
                final int brokerId     = Integer.parseInt( partitionBrokerIdCsv[ 1 ] );

                final TopicAndPartition tp = new TopicAndPartition( topicName, partitionNum );
                retVal.add( tp, brokerId );
            }
        }

        return retVal;
    }

    private TopicPartitionAssignment
    convertToTopicPartitionAssignment( final MultiValueMap< TopicAndPartition, Integer > map )
    {
        final TopicPartitionAssignment tpa = new TopicPartitionAssignment();

        map.forEach( ( topicAndPartition, brokerList ) ->
                         tpa.add( topicAndPartition.topic(), topicAndPartition.partition(), brokerList ) );

        return tpa;
    }

}
