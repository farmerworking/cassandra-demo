package com.farmerworking.cassandra.demo;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.TypeCodec;
import com.google.gson.Gson;

public class FullScan {
  public static void main(String[] args) {
    Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(9042).build();
    cluster.init();
    Session session = cluster.connect();

    int count = 0;
    long actualToken = Long.MIN_VALUE;
    System.out.println("full scan start");

    while(true) {
      if (maxTokenReached(actualToken)) {
        System.out.println("Max Token Reached!");
        break;
      }
      SimpleStatement stmt = new SimpleStatement(queryBuilder(actualToken));
      stmt.setConsistencyLevel(ConsistencyLevel.ALL);
      ResultSet resultSet = session.execute(stmt);

      Iterator<Row> rows = resultSet.iterator();

      if ( !rows.hasNext()){
        break;
      }

      while (rows.hasNext()) {
        Row row = rows.next();
        count ++;
        if (count % 100000 == 0) {
          Map<String, String> map = new HashMap<String, String>();
          for (Definition columnDefinition : row.getColumnDefinitions()) {
            if (columnDefinition.getType().equals(DataType.bigint())) {
              Long value = row.get(columnDefinition.getName(),
                  TypeCodec.bigint());
              map.put(columnDefinition.getName(), String.valueOf(value));
            } else {
              ByteBuffer buffer = row.get(columnDefinition.getName(), TypeCodec.blob());
              map.put(columnDefinition.getName(), buffer.toString());
            }
          }
          System.out.println(new Gson().toJson(map));
        }

        if (!rows.hasNext()) {
          Long token = (Long)row.getPartitionKeyToken().getValue();
          actualToken = nextToken(token);
        }
      }
    }
    System.out.println("full scan finish: " + count);
    session.close();
    cluster.close();
  }

  public static boolean maxTokenReached(Long actualToken){
    return actualToken >= Long.MAX_VALUE;
  }

  public static String queryBuilder(Long nextRange) {
    return String.format("select token(key), key from keyspace1.standard1 where token(key) >= %s limit 10000;", nextRange.toString());
  }

  public static Long nextToken(Long token) {
    return token + 1;
  }
}
