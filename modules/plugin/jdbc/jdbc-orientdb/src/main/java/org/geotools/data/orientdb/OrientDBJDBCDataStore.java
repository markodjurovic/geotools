/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.orientdb;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.SQLException;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.jdbc.JDBCDataStore;
import static org.geotools.jdbc.JDBCDataStore.JDBC_READ_ONLY;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 *
 * @author mdjurovi
 */
public class OrientDBJDBCDataStore extends JDBCDataStore{
  
  private String[] sqlTypeNames;
  private String[] columnNames;
  private SimpleFeatureType simpleFeatureType;
  
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

      this.sqlTypeNames = sqlTypeNames;
      this.columnNames = columnNames;
      this.simpleFeatureType = featureType;
      
      return sql.toString();
  }
  
  @Override
  protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
      // grab the schema, it carries a flag telling us if the feature type is read only
      SimpleFeatureType schema = entry.getState(Transaction.AUTO_COMMIT).getFeatureType();
      if (schema == null) {
          // if the schema still haven't been computed, force its computation so
          // that we can decide if the feature type is read only
          schema = new OrientDBJDBCFeatureSource(entry, null).buildFeatureType();
//          schema = simpleFeatureType;          
          entry.getState(Transaction.AUTO_COMMIT).setFeatureType(schema);
      }

      Object readOnlyMarker = schema.getUserData().get(JDBC_READ_ONLY);
      if (Boolean.TRUE.equals(readOnlyMarker)) {
          return new OrientDBJDBCFeatureSource(entry, null);
      }
      return new OrientDBJDBCFeatureSource(entry, null);
  }
  
}
