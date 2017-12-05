package com.you.nosql.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.datastax.driver.core.querybuilder.Update;

public class ThirdDemo {

    public static void main(String[] args) {
        Cluster cluster = null;
        Session session = null;
        try{
            // 定义一个cluster类
            cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
            // 需要获取Session对象
            session = cluster.connect();

            // 新增数据
            Insert insert = QueryBuilder.insertInto("testkeyspace1", "student").value("name", "lisi").value("age", 40);
            session.execute(insert);

            // 查询数据
            Where select = QueryBuilder.select().all().from("testkeyspace1", "student")
                    .where(QueryBuilder.eq("name", "lisi"));
            ResultSet rs = session.execute(select);
            for (Row row : rs.all()) {
                System.out.println("=>name: " + row.getString("name"));
                System.out.println("=>age: " + row.getInt("age"));
            }

            // 更新数据
            com.datastax.driver.core.querybuilder.Update.Where update = QueryBuilder.update("testkeyspace1", "student")
                    .with(QueryBuilder.set("age", 45)).where(QueryBuilder.eq("name", "lisi"));
            System.out.println(update);
            session.execute(update);
            rs = session.execute(select);
            for (Row row : rs.all()) {
                System.out.println("=>name: " + row.getString("name"));
                System.out.println("=>age : " + row.getInt("age"));
            }

            // 删除数据
            com.datastax.driver.core.querybuilder.Delete.Where delete = QueryBuilder.delete().from("testkeyspace1", "student")
                  .where(QueryBuilder.eq("name", "lisi"));
            System.out.println(delete);
            session.execute(delete);
            rs = session.execute(select);
            for (Row row : rs.all()) {
                System.out.println("=>name: " + row.getString("name"));
                System.out.println("=>age : " + row.getInt("age"));
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
        }
        session.close();
        cluster.close();

    }
}
