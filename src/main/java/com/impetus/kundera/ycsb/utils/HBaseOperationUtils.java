package com.impetus.kundera.ycsb.utils;

import java.io.IOException;

import javax.persistence.PersistenceException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public final class HBaseOperationUtils
{
    
    private final Configuration config = HBaseConfiguration.create(); //new HBaseConfiguration();

    private HBaseAdmin admin;
    
    public HBaseOperationUtils()
    {
        try
        {
            admin = new HBaseAdmin(config);
        }
        catch (Exception e)
        {
            throw new PersistenceException(e);
        }
    }

    
    public void deleteTable(String name) throws IOException
    {
//        TableDescriptors desc = admin.getTableDescriptor(name.getBytes());
//        desc.remove(arg0)
        admin.deleteTable(name);
    }
    
  }
