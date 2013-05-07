/**
 * 
 */
package com.impetus.kundera.ycsb.runner;

import java.io.IOException;

import org.apache.commons.configuration.Configuration;

import com.impetus.kundera.ycsb.utils.HBaseOperationUtils;

/**
 * @author vivek.mishra
 * 
 */
public class HBaseRunner extends YCSBRunner
{

    public HBaseRunner(String propertyFile, Configuration config)
    {
        super(propertyFile, config);
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.ycsb.runner.YCSBRunner#startServer(boolean,
     * java.lang.Runtime)
     */
    @Override
    protected void startServer(boolean performDelete, Runtime runTime)
    {
        if(performDelete)
        {/*
            HBaseOperationUtils utils = new HBaseOperationUtils();
            try
            {
                utils.deleteTable(columnFamilyOrTable);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error while deleting data,Caused by:" , e);
            }
            
        */}

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.ycsb.runner.YCSBRunner#stopServer(java.lang.Runtime)
     */
    @Override
    protected void stopServer(Runtime runTime)
    {

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.ycsb.runner.YCSBRunner#sendMail()
     */
    @Override
    protected void sendMail()
    {
        // TODO Auto-generated method stub

    }

}
