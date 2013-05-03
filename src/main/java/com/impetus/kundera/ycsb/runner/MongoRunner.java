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
package com.impetus.kundera.ycsb.runner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.impetus.kundera.ycsb.utils.CassandraOperationUtils;
import com.impetus.kundera.ycsb.utils.HibernateCRUDUtils;
import com.impetus.kundera.ycsb.utils.MailUtils;
import com.impetus.kundera.ycsb.utils.MongoDBOperationUtils;

import common.Logger;

/**
 * @author vivek mishra
 * 
 */
public class MongoRunner extends YCSBRunner
{
    private static MongoDBOperationUtils operationUtils;

    private String mongoServerLocation;
    
    private String url;

    private static Logger logger = Logger.getLogger(CassandraRunner.class);

    public MongoRunner(final String propertyFile, final Configuration config)
    {
        super(propertyFile,config);
        this.mongoServerLocation = config.getString("server.location");
        operationUtils = new MongoDBOperationUtils();
        crudUtils = new HibernateCRUDUtils();
        url = "mongodb://"+host+":"+port;
    }

   
    @Override
    protected void startServer(boolean performCleanUp, Runtime runTime)
 {
		if (performCleanUp) {
			try {
				operationUtils.cleanDatabase(url, schema);
			} catch (IOException e) {
				logger.error(e);
				throw new RuntimeException(e);
			} catch (InterruptedException e) {
				logger.error(e);
				throw new RuntimeException(e);
			}
		}
	}

    @Override
    protected void stopServer(Runtime runTime)
    {
        // Do nothing.
    }

    protected void sendMail()
    {
        Map<String, Double> delta = new HashMap<String, Double>();

        double kunderaMongoToNativeDelta = ((timeTakenByClient.get(clients[0]) - timeTakenByClient.get(clients[1]))
                / timeTakenByClient.get(clients[0]) * 100);
        delta.put("kunderaMongoToNativeDelta", kunderaMongoToNativeDelta);

        if (kunderaMongoToNativeDelta > 8.00)
        {
            MailUtils.sendMail(delta, runType, "mongoDb");
        }

    }
}
