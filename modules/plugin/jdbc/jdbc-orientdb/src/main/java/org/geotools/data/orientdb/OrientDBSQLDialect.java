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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Map;
import java.util.logging.Level;

import org.geotools.factory.Hints;
import org.geotools.geometry.jts.Geometries;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.SQLDialect;
import org.geotools.referencing.CRS;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import java.util.HashMap;

public class OrientDBSQLDialect extends SQLDialect {

    protected Integer POINT = 2001;
    protected Integer LINESTRING = 2002;
    protected Integer POLYGON = 2003;
    protected Integer MULTIPOINT = 2004;
    protected Integer MULTILINESTRING = 2005;
    protected Integer MULTIPOLYGON = 2006;
    protected Integer GEOMETRY = 2007;
    protected Integer GEOMETRY_COLLECTION = 2008;

    private Map<Class<?>, String> classesToSqlTypeNameMappings = null;
    private Map<Integer, Class<?>> sqlTypesToClasses = null;

    protected Connection currentConnection = null;
    
    public OrientDBSQLDialect(JDBCDataStore dataStore) {
        super(dataStore);
    }
    
    public void setCurrentConnection(Connection cx){
      currentConnection = cx;      
    }

//    public void setStorageEngine(String storageEngine) {
//        this.storageEngine = storageEngine;
//    }
//    public String getStorageEngine() {
//        return storageEngine;
//    }
//    public void setUsePreciseSpatialOps(boolean usePreciseSpatialOps) {
//        this.usePreciseSpatialOps = usePreciseSpatialOps;
//    }
//    public boolean getUsePreciseSpatialOps() {
//        return usePreciseSpatialOps;
//    }
    @Override
    public void encodeCreateTable(StringBuffer sql) {
        sql.append("CREATE CLASS ");
    }

    @Override
    public boolean includeTable(String schemaName, String tableName, Connection cx)
            throws SQLException {
        if ("geometry_columns".equalsIgnoreCase(tableName)) {
            return false;
        }
        return super.includeTable(schemaName, tableName, cx);
    }

    @Override
    public String getNameEscape() {
        return "";
    }

    @Override
    public String getGeometryTypeName(Integer type) {
        if (sqlTypesToClasses == null) {
            registerSqlTypeToClassMappings(new HashMap<>());
        }

        Class<?> classType = sqlTypesToClasses.get(type);
        if (classType != null) {
            String sqlTypeName = getSqlTypeNameForClass(classType);
            if (sqlTypeName != null) {
                return sqlTypeName;
            }
        }

        return super.getGeometryTypeName(type);
    }

    @Override
    public Integer getGeometrySRID(String schemaName, String tableName, String columnName,
            Connection cx) throws SQLException {
        if (cx == null){
          return null;
        }
      
        //first check the geometry_columns table
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT ");
        encodeColumnName(null, "srid", sql);
        sql.append(" FROM ");
        encodeTableName("geometry_columns", sql);
        sql.append(" WHERE ");

        encodeColumnName(null, "f_table_schema", sql);

        if (schemaName != null) {
            sql.append(" = '").append(schemaName).append("'");
        } else {
            sql.append(" IS NULL");
        }
        sql.append(" AND ");

        encodeColumnName(null, "f_table_name", sql);
        sql.append(" = '").append(tableName).append("' AND ");

        encodeColumnName(null, "f_geometry_column", sql);
        sql.append(" = '").append(columnName).append("'");

        dataStore.getLogger().fine(sql.toString());

        Statement st = cx.createStatement();
        try {
            ResultSet rs = st.executeQuery(sql.toString());
            try {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            } finally {
                dataStore.closeSafe(rs);
            }
        } catch (SQLException e) {
            //geometry_columns does not exist
        } finally {
            dataStore.closeSafe(st);
        }

        return null;
    }

    @Override
    public void encodeGeometryColumn(GeometryDescriptor gatt, String prefix,
            int srid, Hints hints, StringBuffer sql) {
        sql.append("ST_AsBinary(");
        encodeColumnName(prefix, gatt.getLocalName(), sql);
        sql.append(")");
    }

    @Override
    public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
        sql.append("ST_AsBinary(");
        sql.append("ST_Envelope(");
        encodeColumnName(null, geometryColumn, sql);
        sql.append(")");
        sql.append(")");
    }

    @Override
    public Envelope decodeGeometryEnvelope(ResultSet rs, int column,
            Connection cx) throws SQLException, IOException {        
        byte[] wkb = rs.getBytes(column);

        try {
            //TODO: srid
            Polygon polygon = (Polygon) new WKBReader().read(wkb);

            return polygon.getEnvelopeInternal();
        } catch (ParseException e) {
            String msg = "Error decoding wkb for envelope";
            throw (IOException) new IOException(msg).initCause(e);
        }
    }

    @Override
    public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String name,
            GeometryFactory factory, Connection cx, Hints hints) throws IOException, SQLException {
        byte[] bytes = rs.getBytes(name);
        if (bytes == null) {
            return null;
        }
        try {
            return new WKBReader(factory).read(bytes);
        } catch (ParseException e) {
            String msg = "Error decoding wkb";
            throw (IOException) new IOException(msg).initCause(e);
        }
    }

    @Override
    public void registerClassToSqlMappings(Map<Class<?>, Integer> mappings) {
        super.registerClassToSqlMappings(mappings);

        mappings.put(Point.class, POINT);
        mappings.put(LineString.class, LINESTRING);
        mappings.put(Polygon.class, POLYGON);
        mappings.put(MultiPoint.class, MULTIPOINT);
        mappings.put(MultiLineString.class, MULTILINESTRING);
        mappings.put(MultiPolygon.class, MULTIPOLYGON);
        mappings.put(Geometry.class, GEOMETRY);
        mappings.put(GeometryCollection.class, GEOMETRY_COLLECTION);
    }

    @Override
    public void registerSqlTypeToClassMappings(Map<Integer, Class<?>> mappings) {
        super.registerSqlTypeToClassMappings(mappings);

        mappings.put(POINT, Point.class);
        mappings.put(LINESTRING, LineString.class);
        mappings.put(POLYGON, Polygon.class);
        mappings.put(MULTIPOINT, MultiPoint.class);
        mappings.put(MULTILINESTRING, MultiLineString.class);
        mappings.put(MULTIPOLYGON, MultiPolygon.class);
        mappings.put(GEOMETRY, Geometry.class);
        mappings.put(GEOMETRY_COLLECTION, GeometryCollection.class);

        sqlTypesToClasses = mappings;
    }

    @Override
    public void registerSqlTypeNameToClassMappings(Map<String, Class<?>> mappings) {
        super.registerSqlTypeNameToClassMappings(mappings);

        mappings.put("OPoint", Point.class);
        mappings.put("OLineString", LineString.class);
        mappings.put("OPolygon", Polygon.class);
        mappings.put("OMultiPoint", MultiPoint.class);
        mappings.put("OMultiLineString", MultiLineString.class);
        mappings.put("OMultiPolygon", MultiPolygon.class);
        mappings.put("OGeometry", Geometry.class);
        mappings.put("OGeometryCollection", GeometryCollection.class);
    }

    @Override
    public void registerSqlTypeToSqlTypeNameOverrides(
            Map<Integer, String> overrides) {
        overrides.put(Types.BOOLEAN, "BOOL");
    }

    //reverse of registerSqlTypeNameToClassMappings
    private void registerClassToSqlTypeName() {
        if (classesToSqlTypeNameMappings == null) {
            classesToSqlTypeNameMappings = new HashMap<>();
        } else {
            return;
        }

        classesToSqlTypeNameMappings.put(Point.class, "OPoint");
        classesToSqlTypeNameMappings.put(LineString.class, "OLineString");
        classesToSqlTypeNameMappings.put(Polygon.class, "OPolygon");
        classesToSqlTypeNameMappings.put(MultiPoint.class, "OMultiPoint");
        classesToSqlTypeNameMappings.put(MultiLineString.class, "OMultiLineString");
        classesToSqlTypeNameMappings.put(MultiPolygon.class, "OMultiPolygon");
        classesToSqlTypeNameMappings.put(Geometry.class, "OGeometry");
        classesToSqlTypeNameMappings.put(GeometryCollection.class, "OGeometryCollection");
    }

    public String getSqlTypeNameForClass(Class<?> classType) {
        if (classesToSqlTypeNameMappings == null) {
            registerClassToSqlTypeName();
        }
        return classesToSqlTypeNameMappings.get(classType);
    }

    @Override
    public void encodePostCreateTable(String tableName, StringBuffer sql) {
    }

    @Override
    public void encodePostColumnCreateTable(AttributeDescriptor att, StringBuffer sql) {
        //make geometry columns non null in order to be able to index them
        if (att instanceof GeometryDescriptor && !att.isNillable()) {
            if (!sql.toString().trim().endsWith(" NOT NULL")) {
                sql.append(" NOT NULL");
            }
        }
    }

    @Override
    public void postCreateTable(String schemaName, SimpleFeatureType featureType, Connection cx)
            throws SQLException, IOException {

        //create teh geometry_columns table if necessary
        DatabaseMetaData md = cx.getMetaData();
        ResultSet rs = md.getTables(null, dataStore.escapeNamePattern(md, schemaName),
                dataStore.escapeNamePattern(md, "geometry_columns"), new String[]{"TABLE"});

        try {
            if (!rs.next()) {
                //create it
                Statement st = cx.createStatement();
                try {
                    StringBuffer sql = new StringBuffer("CREATE CLASS ");
                    encodeTableName("geometry_columns", sql);
                    if (LOGGER.isLoggable(Level.FINE)) {
                        LOGGER.fine(sql.toString());
                    }
                    st.execute(sql.toString());
                } finally {
                    dataStore.closeSafe(st);
                }
            }
        } finally {
            dataStore.closeSafe(rs);
        }

        //create spatial index for all geometry columns
        for (AttributeDescriptor ad : featureType.getAttributeDescriptors()) {
            if (!(ad instanceof GeometryDescriptor)) {
                continue;
            }
            GeometryDescriptor gd = (GeometryDescriptor) ad;
            Class type = gd.getType().getBinding();
            String typeStr = getSqlTypeNameForClass(type);
            String className = featureType.getTypeName();

            //create properties
            {
                Statement statement = cx.createStatement();
                StringBuffer sql = new StringBuffer("CREATE PROPERTY ");
                encodeTableName(className, sql);
                sql.append(".");
                encodeColumnName(null, gd.getLocalName(), sql);
                sql.append(" EMBEDDED ");
                sql.append(typeStr);
                try {
                    statement.execute(sql.toString());
                } finally {
                    dataStore.closeSafe(statement);
                }
            }

            if (true) {//(!ad.isNillable()) {
                //can only index non null columns
                StringBuffer sql = new StringBuffer("CREATE INDEX ");
                encodeTableName(featureType.getTypeName(), sql);
                sql.append(".");
                encodeColumnName(null, gd.getLocalName(), sql);
                sql.append("_index ON ");
                encodeTableName(featureType.getTypeName(), sql);
                sql.append(" (");
                encodeColumnName(null, gd.getLocalName(), sql);
                sql.append(") SPATIAL ENGINE LUCENE");

                LOGGER.fine(sql.toString());
                Statement st = cx.createStatement();
                try {
                    st.execute(sql.toString());
                } finally {
                    dataStore.closeSafe(st);
                }
            }

            CoordinateReferenceSystem crs = gd.getCoordinateReferenceSystem();
            int srid = -1;
            if (crs != null) {
                Integer i = null;
                try {
                    i = CRS.lookupEpsgCode(crs, true);
                } catch (FactoryException e) {
                    LOGGER.log(Level.FINER, "Could not determine epsg code", e);
                }
                srid = i != null ? i : srid;
            }

            StringBuffer sql = new StringBuffer("INSERT INTO ");
            encodeTableName("geometry_columns", sql);
            sql.append(" (");
            encodeColumnName(null, "f_table_schema", sql);
            sql.append(", ");
            encodeColumnName(null, "f_table_name", sql);
            sql.append(", ");
            encodeColumnName(null, "f_geometry_column", sql);
            sql.append(", ");
            encodeColumnName(null, "coord_dimension", sql);
            sql.append(", ");
            encodeColumnName(null, "srid", sql);
            sql.append(", ");
            encodeColumnName(null, "type", sql);
            sql.append(") ");
            sql.append(" VALUES (");
            sql.append(schemaName != null ? "'" + schemaName + "'" : "NULL").append(", ");
            sql.append("'").append(featureType.getTypeName()).append("', ");
            sql.append("'").append(ad.getLocalName()).append("', ");
            sql.append("2, ");
            sql.append(srid).append(", ");

            Geometries g = Geometries.getForBinding((Class<? extends Geometry>) gd.getType().getBinding());
            sql.append("'").append(g != null ? g.getName().toUpperCase() : "GEOMETRY").append("')");

            LOGGER.fine(sql.toString());
            Statement st = cx.createStatement();
            try {
                st.execute(sql.toString());
            } finally {
                dataStore.closeSafe(st);
            }
        }

    }

    @Override
    public boolean lookupGeneratedValuesPostInsert() {
        return true;
    }

    @Override
    public Object getLastAutoGeneratedValue(String schemaName, String tableName, String columnName,
            Connection cx) throws SQLException {
        Statement st = cx.createStatement();
        try {
            String sql = "SELECT last_insert_id()";
            dataStore.getLogger().fine(sql);

            ResultSet rs = st.executeQuery(sql);
            try {
                if (rs.next()) {
                    return rs.getLong(1);
                }
            } finally {
                dataStore.closeSafe(rs);
            }
        } finally {
            dataStore.closeSafe(st);
        }

        return null;
    }

    @Override
    public boolean isLimitOffsetSupported() {
        return true;
    }

    @Override
    public void applyLimitOffset(StringBuffer sql, int limit, int offset) {
        if (offset > 0) {
            sql.append(" SKIP ").append(offset).append(" LIMIT ").append(limit);
        } else {
            sql.append(" LIMIT ").append(limit);
        }
    }

    @Override
    public void dropIndex(Connection cx, SimpleFeatureType schema, String databaseSchema,
            String indexName) throws SQLException {
        StringBuffer sql = new StringBuffer();
        String escape = getNameEscape();
        sql.append("DROP INDEX ");
        if (databaseSchema != null) {
            encodeSchemaName(databaseSchema, sql);
            sql.append(".");
        }
        // weirdness, index naems are treated as strings...
        sql.append(escape).append(indexName).append(escape);
        sql.append(" on ");
        if (databaseSchema != null) {
            encodeSchemaName(databaseSchema, sql);
            sql.append(".");
        }
        encodeTableName(schema.getTypeName(), sql);

        Statement st = null;
        try {
            st = cx.createStatement();
            st.execute(sql.toString());
            if (!cx.getAutoCommit()) {
                cx.commit();
            }
        } finally {
            dataStore.closeSafe(cx);
        }
    }
}
