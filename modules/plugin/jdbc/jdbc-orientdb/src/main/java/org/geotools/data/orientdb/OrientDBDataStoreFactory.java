/*
 *    GeoTools - The Open Source Java GIS Toolkit
 *    http://geotools.org
 *
 *    (C) 2002-2008, Open Source Geospatial Foundation (OSGeo)
 *
 *    This library is free software; you can redistribute it and/or
 *    modify it under the terms of the GNU Lesser General Public
 *    License as published by the Free Software Foundation;
 *    version 2.1 of the License.
 *
 *    This library is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *    Lesser General Public License for more details.
 */
package org.geotools.data.orientdb;

import org.geotools.data.Parameter;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import static org.geotools.jdbc.JDBCDataStoreFactory.SCHEMA;
import org.geotools.jdbc.OrientDBJDBCDataStore;
import org.geotools.jdbc.SQLDialect;


/**
 * DataStoreFactory for OrientDB
 **/

public class OrientDBDataStoreFactory extends JDBCDataStoreFactory {
    /** parameter for database type */
    public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true,"orient",
            Collections.singletonMap(Parameter.LEVEL, "program"));    
    
    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {        
      return new OrientDBSQLDialectBasic(dataStore);
    }

    @Override
    public String getDisplayName() {
        return "OrientDB";
    }
    
    @Override
    protected String getDriverClassName() {
        return "com.orientechnologies.orient.jdbc.OrientJdbcDriver";
    }

    @Override
    protected String getDatabaseID() {
        return (String) DBTYPE.sample;
    }

    @Override
    public String getDescription() {
        return "OrientDB store";
    }

    @Override
    protected String getValidationQuery() {
        return "select 1";
    }
    
    @Override
    protected void setupParameters(Map parameters) {
        super.setupParameters(parameters);
        parameters.put(DBTYPE.key, DBTYPE);
        
        parameters.remove(SCHEMA.key);
    }
    
    @Override
    protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map params)
            throws IOException {                
        
      OrientDBJDBCDataStore ds = new OrientDBJDBCDataStore(dataStore);
      return ds;
    }    
    
    @Override
    protected String getJDBCUrl(Map params) throws IOException {
        // jdbc url
        String host = (String) HOST.lookUp(params);
//        Integer port = (Integer) PORT.lookUp(params);
        String db = (String) DATABASE.lookUp(params);
        
        String url = "jdbc:" + getDatabaseID() + ":remote:" + host;        
        
        if ( db != null ) {
            url += "/" + db; 
        }
        return url;
    }
    
    @Override
    public JDBCDataStore createDataStore(Map params)
        throws IOException {
        JDBCDataStore dataStore = super.createDataStore(params);
        dataStore.setCallbackFactory(new OrientDBCallBackFactory());
        return dataStore;
    }
}
