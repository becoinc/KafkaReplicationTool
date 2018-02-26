/**
 * @file TopicPartitionAssignment.java
 * <p>
 * Copyright (C) 2018 by Beco Inc. All Rights Reserved.
 * See included LICENSE file with this for terms of use.
 * <p>
 * Any use, compilation, or distribution of this source code constitutes consent to the
 * terms and conditions in the license file.
 * @date 2/19/18 7:05 PM
 * @author jzampieron
 */

package io.beco.KafkaManager;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import org.springframework.util.Assert;

import java.util.*;

/**
 * TopicPartitionAssignment is a class used to create
 * a json version of the data used by the Scala
 * {@link kafka.admin.ReassignPartitionsCommand} tool.
 *
 * You can also refer to https://kafka.apache.org/documentation/#basic_ops_partitionassignment
 */
@Getter
@ToString
public class TopicPartitionAssignment
{
    @AllArgsConstructor
    @Data
    public static class TopicPartReplSet
    {
        private String         topic;
        private int            partition;
        private Set< Integer > replicas;
    }

    private final int version = 1;

    /**
     * A multi-dimensional map of topics -> partitions -> [ replica id set ]
     */
    @JsonIgnore
    private final Map< String, Map< Integer, Set< Integer > > > topicPartAssignments = new HashMap<>();

    /**
     * Adds a set of broker ids to the set of brokers.
     * @param topic
     * @param partition
     * @param brokerIds
     */
    public void add( final String topic, int partition, Collection< Integer > brokerIds )
    {
        Assert.notNull( topic, "Topic may not be null" );
        Assert.notNull( topic, "Broker Ids may not be null." );

        Map< Integer, Set< Integer > > partAssignments = this.topicPartAssignments.get( topic );
        if( partAssignments == null )
        {
            partAssignments = new HashMap<>();
            this.topicPartAssignments.put( topic, partAssignments );
        }

        Set< Integer > replicas = partAssignments.get( partition );
        if ( replicas == null )
        {
            replicas = new HashSet<>();
            partAssignments.put( partition, replicas );
        }
        replicas.addAll( brokerIds );
    }

    /**
     * Adds a broker to the set of brokers for the
     * @param topic
     * @param partition
     * @param brokerId
     */
    public void add( final String topic, int partition, int brokerId )
    {
        Assert.notNull( topic, "Topic may not be null" );

        Map< Integer, Set< Integer > > partAssignments = this.topicPartAssignments.get( topic );
        if( partAssignments == null )
        {
            partAssignments = new HashMap<>();
            this.topicPartAssignments.put( topic, partAssignments );
        }

        Set< Integer > replicas = partAssignments.get( partition );
        if ( replicas == null )
        {
            replicas = new HashSet<>();
            partAssignments.put( partition, replicas );
        }
        replicas.add( brokerId );
    }

    @JsonProperty( "partitions" )
    public List< TopicPartReplSet > generateTopicAssignments()
    {
        final List< TopicPartReplSet > partitions = new LinkedList<>();

        this.topicPartAssignments.forEach( ( topic, topicPartAssignments ) -> {
            topicPartAssignments.forEach( ( partition, replicas ) -> {
                partitions.add( new TopicPartReplSet( topic, partition, replicas ) );
            });
        });

        return partitions;
    }

    /**
     * Finds only the changed broker assignments for all the partitions on the topic.
     *
     * For simplicity we only work with a <b>single</b> topic.
     *
     * @param requested
     * @param current
     * @return
     */
    public static TopicPartitionAssignment findAssignmentChanges( final TopicPartitionAssignment requested,
                                                                  final TopicPartitionAssignment current )
    {
        final TopicPartitionAssignment changed = new TopicPartitionAssignment();

        final Map< String, Map< Integer, Set< Integer > > > topicPartMap = current.getTopicPartAssignments();

        requested.getTopicPartAssignments().forEach( ( topic, partAssignments ) -> {
            final Map< Integer, Set< Integer > > partMap = topicPartMap.get( topic );
            if( partMap != null )
            {
                changed.set( topic, partMap );
            }
        } );

        return changed;
    }

    /**
     * Set the partition assignments for a topic, <b>overwriting</b> whatever is there already.
     * @param topic
     * @param partMap
     */
    public void set( final String topic,
                     final Map< Integer, Set< Integer > > partMap )
    {
        this.topicPartAssignments.put( topic, partMap );
    }
}
