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
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;


/**
 * DataStoreFactory for OrientDB
 **/

public class OrientDBDataStoreFactory extends JDBCDataStoreFactory {
    /** parameter for database type */
    public static final Param DBTYPE = new Param("dbtype", String.class, "Type", true,"mysql",
            Collections.singletonMap(Parameter.LEVEL, "program"));
    /** Default port number for MYSQL */
    public static final Param PORT = new Param("port", Integer.class, "Port", true, 3306);
    /** Storage engine to use when creating tables */    
    
    @Override
    protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
        //return new MySQLDialectPrepared(dataStore);
        return new OrientDBSQLDialectBasic(dataStore, enhancedSpatialSupport);
    }

    @Override
    public String getDisplayName() {
        return "MySQL";
    }
    
    @Override
    protected String getDriverClassName() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    protected String getDatabaseID() {
        return (String) DBTYPE.sample;
    }

    @Override
    public String getDescription() {
        return "MySQL Database";
    }

    @Override
    protected String getValidationQuery() {
        return "select version()";
    }
    
    @Override
    protected void setupParameters(Map parameters) {
        super.setupParameters(parameters);
        parameters.put(DBTYPE.key, DBTYPE);
        parameters.put(PORT.key, PORT);        
        
        parameters.remove(SCHEMA.key);
    }
    
    @Override
    protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map params)
            throws IOException {                
        
        SQLDialect dialect = dataStore.getSQLDialect();
        if (dialect instanceof OrientDBSQLDialectBasic) {
            ((OrientDBSQLDialectBasic)dialect).setStorageEngine(storageEngine);
            ((OrientDBSQLDialectBasic)dialect).setUsePreciseSpatialOps(enhancedSpatialSupport);
        }
        else {
            ((OrientDBSQLDialectPrepared)dialect).setStorageEngine(storageEngine);
            ((OrientDBSQLDialectPrepared)dialect).setUsePreciseSpatialOps(enhancedSpatialSupport);
        }
        
        return dataStore;
    }    
}
