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

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

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

    private final List< TopicPartReplSet > partitions = new LinkedList<>();
}
