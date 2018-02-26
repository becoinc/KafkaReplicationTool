/**
 * @file TopicPartitionAssignmentTests.java
 * <p>
 * Copyright (C) 2018 by Beco Inc. All Rights Reserved.
 * See included LICENSE file with this for terms of use.
 * <p>
 * Any use, compilation, or distribution of this source code constitutes consent to the
 * terms and conditions in the license file.
 * @date 2/26/18 2:29 PM
 * @author jzampieron
 */

package io.beco.KafkaManager;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.List;

/**
 * TopicPartitionAssignmentTests is a class that tests the logic in {@link TopicPartitionAssignment}.
 */
@RunWith( JUnit4.class )
public class TopicPartitionAssignmentTests
{
    private static final String topic1 = "testTopic1";
    private static final String topic2 = "testTopic2";

    @Test
    public void testGenerateAssignmentList()
    {

        final TopicPartitionAssignment tpa = testSet1();

        final List< TopicPartitionAssignment.TopicPartReplSet > assignments = tpa.generateTopicAssignments();
        Assert.assertEquals( 4, assignments.size() );
        Assert.assertEquals( 2, tpa.getTopicPartAssignments().get( topic1 ).size() );
        Assert.assertEquals( 2, tpa.getTopicPartAssignments().get( topic2 ).size() );
        // Total number of partition-replicas
        Assert.assertEquals( 8, assignments.stream().mapToInt( partReplSet -> partReplSet.getReplicas().size() ).sum() );
    }

    @Test
    public void testFindPartitionAssignments()
    {
        final TopicPartitionAssignment changes =
            TopicPartitionAssignment.findAssignmentChanges( testSet1(), testSet2() );
        Assert.assertNotNull( changes );
        Assert.assertEquals( 1, changes.getTopicPartAssignments().size() ); // 1 topic changed
        Assert.assertEquals( 2, changes.getTopicPartAssignments().get( topic2 ).size() );
        Assert.assertEquals( 3, changes.getTopicPartAssignments().get( topic2 ).get( 1 ).size() );
        Assert.assertEquals( 3, changes.getTopicPartAssignments().get( topic2 ).get( 2 ).size() );
    }

    private TopicPartitionAssignment testSet1()
    {
        final TopicPartitionAssignment tpa = new TopicPartitionAssignment();
        tpa.add( topic1, 1, 1 );
        tpa.add( topic1, 1, 2 );
        tpa.add( topic1, 2, 1 );
        tpa.add( topic1, 2, 2 );

        tpa.add( topic2, 1, 1 );
        tpa.add( topic2, 1, 2 );
        tpa.add( topic2, 2, 1 );
        tpa.add( topic2, 2, 2 );
        return tpa;
    }

    private TopicPartitionAssignment testSet2()
    {
        final TopicPartitionAssignment tpa = new TopicPartitionAssignment();
        tpa.add( topic2, 1, 1 );
        tpa.add( topic2, 1, 2 );
        tpa.add( topic2, 2, 1 );
        tpa.add( topic2, 2, 2 );
        tpa.add( topic2, 1, 3 );
        tpa.add( topic2, 2, 3 );
        return tpa;
    }
}
