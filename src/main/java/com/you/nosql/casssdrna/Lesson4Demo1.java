package com.you.nosql.casssdrna;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class Lesson4Demo1 {

    public static void main(String[] args) {
        // TODO Auto-generated method stub
        Cluster cluster = null;
        Session session = null;

        try {
            QueryOptions options = new QueryOptions();
            options.setConsistencyLevel(ConsistencyLevel.QUORUM);

            cluster = Cluster.builder().addContactPoint("127.0.0.1").withQueryOptions(options).build();
            session = cluster.connect();

            PreparedStatement statement = session.prepare("INSERT INTO testkeyspace1.student(name,age) values(?,?");
            statement.setConsistencyLevel(ConsistencyLevel.ONE);
            session.execute(statement.bind("zhangsan", 20));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            session.close();
            cluster.close();
        }

    }
}
