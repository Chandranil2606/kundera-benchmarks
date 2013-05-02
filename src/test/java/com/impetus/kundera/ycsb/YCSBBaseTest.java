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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;

import com.impetus.kundera.ycsb.runner.YCSBRunner;

/**
 * @author vivek.mishra
 *
 */
public abstract class YCSBBaseTest
{
    protected Configuration config;
    protected String workLoadPackage;
    protected YCSBRunner runner;

    /**
     * @throws java.lang.Exception
     */
    protected void setUp(final String propertyFileName) throws Exception
    {
        config = new PropertiesConfiguration(propertyFileName);
        workLoadPackage = config.getString("workload.dir","src/main/resources/workloads");
    }

    protected void onBulkLoad() throws IOException
    {
        int noOfThreads = 1;
        String[] workLoadList = config.getStringArray("bulk.workload.type");
        
        for(String workLoad : workLoadList)
        {
            runner.run(workLoadPackage+"/"+workLoad, noOfThreads);
        }

    }
    
    protected void onConcurrentBulkLoad() throws NumberFormatException, IOException
    {
        // comma seperated list.
        String[] noOfThreads = config.getStringArray("threads");

        String[] workLoadList = config.getStringArray("concurrent.workload.type");
        
        for(int i=0;i<noOfThreads.length;i++)
        {
            runner.run(workLoadPackage+"/"+workLoadList[0], Integer.parseInt(noOfThreads[i]));
        }

    }
}
