/**
 * 
 */
package com.impetus.kundera.ycsb.runner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.configuration.Configuration;

import com.impetus.kundera.ycsb.utils.HBaseOperationUtils;
import com.impetus.kundera.ycsb.utils.HibernateCRUDUtils;
import com.impetus.kundera.ycsb.utils.MailUtils;

/**
 * @author vivek.mishra
 * 
 */
public class HBaseRunner extends YCSBRunner
{

    private HBaseOperationUtils utils = new HBaseOperationUtils();

    private String startHBaseServerCommand;

    private String stopHBaseServerCommand;

    public HBaseRunner(String propertyFile, Configuration config)
    {
        super(propertyFile, config);
        String server = config.getString("server.location");
        this.startHBaseServerCommand = server+"/"+"start-hbase.sh";
        this.stopHBaseServerCommand = server+"/"+"stop-hbase.sh";
        crudUtils = new HibernateCRUDUtils();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.ycsb.runner.YCSBRunner#startServer(boolean,
     * java.lang.Runtime)
     */
    @Override
    public void startServer(boolean performDelete, Runtime runTime)
    {
        if (performDelete)
        {/*
          * HBaseOperationUtils utils = new HBaseOperationUtils(); try {
          * utils.deleteTable(columnFamilyOrTable); } catch (IOException e) {
          * throw new RuntimeException("Error while deleting data,Caused by:" ,
          * e); }
          */
            try
            {
                utils.startHBaseServer(runTime, startHBaseServerCommand);
            }
            catch (IOException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch (InterruptedException e)
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.impetus.kundera.ycsb.runner.YCSBRunner#stopServer(java.lang.Runtime)
     */
    @Override
    public void stopServer(Runtime runTime)
    {
        try
        {
            utils.stopHBaseServer(stopHBaseServerCommand, runTime);
        }
        catch (IOException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.kundera.ycsb.runner.YCSBRunner#sendMail()
     */
    @Override
    protected void sendMail()
    {
        Map<String, Double> delta = new HashMap<String, Double>();

        double kunderaHBaseToNativeDelta = ((timeTakenByClient.get(clients[1]).doubleValue() - timeTakenByClient.get(
                clients[0]).doubleValue())
                / timeTakenByClient.get(clients[1]).doubleValue() * 100);
        delta.put("kunderaHBaseToNativeDelta", kunderaHBaseToNativeDelta);

        if (kunderaHBaseToNativeDelta > 8.00)
        {
            MailUtils.sendMail(delta, isUpdate ? "update" : runType, "hbase");
        }
    }

}
