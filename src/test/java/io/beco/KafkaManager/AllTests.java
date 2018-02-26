/**
 * @file AllTests.java
 * <p>
 * Copyright (C) 2018 by Beco Inc. All Rights Reserved.
 * See included LICENSE file with this for terms of use.
 * <p>
 * Any use, compilation, or distribution of this source code constitutes consent to the
 * terms and conditions in the license file.
 * @date 2/20/18 2:46 PM
 * @author jzampieron
 */

package io.beco.KafkaManager;

/**
 * AllTests is a class that does...
 */

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith( Suite.class )
@Suite.SuiteClasses(
    {
        KafkaManagerApplicationTests.class,
        TopicPartitionAssignmentTests.class
    } )
public class AllTests
{
}
