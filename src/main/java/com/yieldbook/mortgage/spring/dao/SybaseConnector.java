package com.yieldbook.mortgage.spring.dao;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class SybaseConnector {

        public static void main(String[] args)  {
                try{
                Class.forName("com.sybase.jdbc4.jdbc.SybDriver");
                String url = "jdbc:sybase:Tds:icgmrt390db1u.nam.nsroot.net:20021?charset=iso_1&JCE_PROVIDER_CLASS=org.bouncycastle.jce.provider.BouncyCastleProvider&ENCRYPT_PASSWORD=true&user=bpa&password=bpa123";
                Connection con = DriverManager.getConnection(url);
                Statement stmt = con.createStatement();
                stmt.setMaxRows(100);
                ResultSet rs = stmt.executeQuery("select * from  prd.dbo.pool" );
                while (rs.next()){
                    String firstCol  = rs.getString(1);
                    System.out.println(firstCol);
                    String secondCol  = rs.getString(2);
                    System.out.println(secondCol);
                    String thirdCol  = rs.getString(3);
                    System.out.println(thirdCol);
                }
                con.close();}
                catch (Exception ex){
                    ex.printStackTrace();
                    ex.getCause();
                    ex.toString();
                }
            }
}
