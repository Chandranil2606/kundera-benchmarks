/**
 * 
 */
package com.impetus.kundera.ycsb.utils;

import java.io.IOException;

import redis.clients.jedis.Jedis;

import common.Logger;

/**
 * @author Kuldeep Mishra
 * 
 */
public class RedisOperationUtils
{
    private static Logger logger = Logger.getLogger(RedisOperationUtils.class);

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
    public void cleanRedisDatabase(String host, int port,String password) throws IOException, InterruptedException
    {
        logger.info("flushing db ..........");
        Jedis jedis = new Jedis(host, port);
        jedis.connect();
        if(password != null)
       {
        jedis.auth(password);
       }
        jedis.flushDB();
        jedis.disconnect();
    }
}
