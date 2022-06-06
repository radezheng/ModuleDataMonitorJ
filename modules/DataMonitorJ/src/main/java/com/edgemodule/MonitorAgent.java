package com.edgemodule;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MonitorAgent {

    // Connect to your database.
    // Replace server name, username, and password with your credentials

    private static final String edgeHost = System.getenv("IOTEDGE_GATEWAYHOSTNAME");
    
   private static final String connectionUrl = System.getenv( "SQL_CONNECTION_URL");
    
    private String lastV = "0";

    public MonitorAgent() {
        
    }


    private void setLastV(String lastV) {
        this.lastV = lastV;
    }

    private String getLastV() {
        return lastV;
    }

    public  List<HashMap<String, String>> getDataChanged(String lastv) {

        ResultSet resultSet = null;
        if(lastv == null) {
            lastv = getLastV();
        }
        List<HashMap<String,String>> data = new ArrayList<HashMap<String, String>>();
        
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            System.out.println("Connecting to database..." + connectionUrl);
            Connection connection = DriverManager.getConnection(connectionUrl);
                Statement statement = connection.createStatement();
            // Create and execute a SELECT SQL statement.
            String selectSql = "exec dbo.sp_getInventoryChanged " + lastv;
            resultSet = statement.executeQuery(selectSql);
            
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();

            int h = 0;
            // Print results from select statement
            
            while (resultSet.next()) {
                HashMap<String,String> row = new HashMap<String, String>();   
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    String columnValue = resultSet.getString(i);
                    row.put(columnName, columnValue);
                    if(columnName.equals("curr_v")) {
                        setLastV(columnValue);
                    }
                }
                data.add(row);
                System.out.println(row.toString());
                
            }

            
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        return data;
    }
}