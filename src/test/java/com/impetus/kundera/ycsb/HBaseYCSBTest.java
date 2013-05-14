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

import org.apache.commons.configuration.ConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.impetus.kundera.ycsb.runner.HBaseRunner;

/**
 * Cassandra Kundera YCSB benchmarking.
 * 
 * @author vivek.mishra
 * 
 */
public class HBaseYCSBTest extends YCSBBaseTest
{

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception
    {
        super.setUp("src/main/resources/db-hbase.properties");
    }

    @Test
    public void concurrentWorkloadTest() throws IOException, ConfigurationException
    {
        onChangeRunType("load");
        process();
    }
    
    
    @Test
    public void testRead() throws Exception
    {
    	onChangeRunType("t");
    	onRead();
    }
	
    @Test
    public void testUpdate() throws Exception
    {
    	onChangeRunType("t");
    	onUpdate();
    }
	
    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception
    {
        
    }


    /**
     * @param runType
     * @throws ConfigurationException
     */
    protected void onChangeRunType(final String runType) throws ConfigurationException {
		config.setProperty("run.type",runType);
    	config.save();
        runner = new HBaseRunner(propsFileName, config);
	}

}
