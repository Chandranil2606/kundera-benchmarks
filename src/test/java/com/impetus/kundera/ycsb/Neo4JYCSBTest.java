/**
 * Copyright 2012 Impetus Infotech.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.impetus.kundera.ycsb;
import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.ycsb.runner.Neo4jRunner;


/**
 * @author vivek.mishra
 *
 */
public class Neo4JYCSBTest extends YCSBBaseTest
{

    @Before
    public void setUp() throws Exception
    {
        String propsFileName = System.getProperty("fileName");
        super.setUp(propsFileName);
        runner = new Neo4jRunner(propsFileName, config);
    }

    @After
    public void tearDown() throws Exception
    {
    }

//    @Test
    public void bulkLoadTest() throws IOException
    {
        onBulkLoad();
    }

    @Test
    public void concurrentWorkloadTest() throws IOException
    {
        onConcurrentBulkLoad();
    }

}
