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
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import scala.io.BytePickle.Def;

import com.impetus.kundera.ycsb.runner.CassandraRunner;
import com.impetus.kundera.ycsb.runner.YCSBRunner;

/**
 * @author vivek.mishra
 *
 */
public abstract class YCSBBaseTest
{
    protected PropertiesConfiguration config;
    protected String workLoadPackage;
    protected YCSBRunner runner;
    protected String propsFileName=System.getProperty("fileName");

    /**
     * @throws java.lang.Exception
     */
    protected void setUp(final String defaultPath) throws Exception
    {
        config = new PropertiesConfiguration(!StringUtils.isBlank(propsFileName)? propsFileName:defaultPath);
        workLoadPackage = config.getString("workload.dir","src/main/resources/workloads");
    }

    /**
     * @throws IOException
     */
    protected void onBulkLoad() throws IOException
    {
        int noOfThreads = 1;
        String[] workLoadList = config.getStringArray("bulk.workload.type");
        
        for(String workLoad : workLoadList)
        {
            runner.run(workLoadPackage+"/"+workLoad, noOfThreads);
        }

    }
    
    /**
     * @throws NumberFormatException
     * @throws IOException
     */
    protected void process() throws NumberFormatException, IOException
    {
        // comma seperated list.
        String[] noOfThreads = config.getStringArray("threads");

        String[] workLoadList = config.getStringArray("workload.file");
        
        for(int i=0;i<noOfThreads.length;i++)
        {
            runner.run(workLoadPackage+"/"+workLoadList[0], Integer.parseInt(noOfThreads[i]));
        }

    }
    
    /**
     * @throws ConfigurationException
     * @throws IOException 
     * @throws NumberFormatException 
     */
    protected void onRead() throws ConfigurationException, NumberFormatException, IOException
    {
        String[] workLoadList = config.getStringArray("workload.file");
        for(String workLoad : workLoadList)
        {
            PropertiesConfiguration workLoadConfig = new PropertiesConfiguration(workLoadPackage+"/"+workLoad);
            workLoadConfig.setProperty("readproportion", "1");
            workLoadConfig.setProperty("updateproportion", "0");
            workLoadConfig.setProperty("scanproportion", "0");
            workLoadConfig.setProperty("insertproportion", "0");
            workLoadConfig.save();
            process();
        }
    	
    }


    /**
     * @throws ConfigurationException
     * @throws IOException 
     * @throws NumberFormatException 
     */
    protected void onUpdate() throws ConfigurationException, NumberFormatException, IOException
    {
        String[] workLoadList = config.getStringArray("workload.file");
        for(String workLoad : workLoadList)
        {
            PropertiesConfiguration workLoadConfig = new PropertiesConfiguration(workLoadPackage+"/"+workLoad);
            workLoadConfig.setProperty("readproportion", "0");
            workLoadConfig.setProperty("updateproportion", "1");
            workLoadConfig.setProperty("scanproportion", "0");
            workLoadConfig.setProperty("insertproportion", "0");
            workLoadConfig.save();
            process();
        }
    }
	
    protected abstract void onChangeRunType(final String runType) throws ConfigurationException; 
}
