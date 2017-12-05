package com.you.nosql.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;

public class FirstDemo {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
       Metadata  metadata= cluster.getMetadata();
       for(Host host:metadata.getAllHosts()){
           System.out.println("==>" + host.getAddress());
       }
       System.out.println("==================");
       
       for(KeyspaceMetadata keyspaceMetadata : metadata.getKeyspaces()){
           System.out.println("==>" + keyspaceMetadata.getName());
       }
       cluster.close();
        
        
        
        
        
    }

}
