/**
 * 
 */
package com.impetus.kundera.ycsb.utils;

import java.io.IOException;

import com.mongodb.DB;
import com.mongodb.DBAddress;
import com.mongodb.Mongo;
import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class MongoDBOperationUtils
{
    private static Logger logger = Logger.getLogger(MongoDBOperationUtils.class);

    /**
     * Stop mongo server.
     * 
     * @param port
     *            the runtime
     * @param br
     *            the br
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     * @throws InterruptedException
     */
    public void cleanDatabase(String url, String dbname) throws IOException, InterruptedException
    {
        logger.info("flushing db ..........");

        if (url.startsWith("mongodb://"))
        {
            url = url.substring(10);
        }

        // need to append db to url.
        url += "/" + dbname;

        Mongo mongo = new Mongo(new DBAddress(url));

        DB db = mongo.getDB(dbname);
        db.dropDatabase();
        mongo.close();
    }
}
