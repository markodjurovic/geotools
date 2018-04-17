/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.orientdb;

import java.lang.reflect.Field;
import java.sql.SQLException;
import org.geotools.jdbc.JDBCDataStore;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

/**
 *
 * @author mdjurovi
 */
public class OrientDBJDBCDataStore extends JDBCDataStore{
  
  public OrientDBJDBCDataStore(JDBCDataStore dataStore){    
    super();
    LOGGER.info("OPAAAAAAAAAAAAAAAAAAAAAAAAAA");
    Field[] fields = JDBCDataStore.class.getDeclaredFields();
    for (Field field : fields){
      try{
        LOGGER.info("Constructor field: " + field.getName());
        field.setAccessible(true);
        field.set(this, field.get(dataStore));
      }
      catch (IllegalAccessException exc){
        LOGGER.info("Error constructor: " + exc.getLocalizedMessage());
        int a = 0;
        ++a;
        //should never happend
      }
    }
  }
  
  
  @Override
  protected String createTableSQL(String tableName, String[] columnNames, String[] sqlTypeNames,
      boolean[] nillable, String pkeyColumn, SimpleFeatureType featureType ) throws SQLException {
      //build the create table sql
      StringBuffer sql = new StringBuffer();
      dialect.encodeCreateTable(sql);

      encodeTableName(tableName, sql, null);      

      //encode anything post create table
      dialect.encodePostCreateTable(tableName, sql);

      return sql.toString();
  }
}
