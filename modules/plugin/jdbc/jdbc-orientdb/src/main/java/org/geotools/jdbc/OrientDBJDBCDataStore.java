/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.jdbc;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.geotools.data.Query;
import org.geotools.data.Transaction;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import static org.geotools.jdbc.JDBCDataStore.getColumnNames;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;

/**
 *
 * @author mdjurovi
 */
public class OrientDBJDBCDataStore extends JDBCDataStore {

    private Field[] getAllFields() {
        List<Field> retList = new ArrayList<>();
        Class currentClass = JDBCDataStore.class;
        while (currentClass != null && currentClass != Object.class) {
            Field[] fields = currentClass.getDeclaredFields();
            retList.addAll(Arrays.asList(fields));
            currentClass = currentClass.getSuperclass();
        }

        return retList.toArray(new Field[0]);
    }

    public OrientDBJDBCDataStore(JDBCDataStore dataStore) {
        super();
        Field[] fields = getAllFields();
        for (Field field : fields) {
            if (field.isSynthetic() || Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            try {
                field.setAccessible(true);
                field.set(this, field.get(dataStore));
            } catch (IllegalAccessException exc) {
                LOGGER.info("Error constructor: " + exc.getLocalizedMessage());
                //should never happend
            }
        }
    }

    @Override
    protected String createTableSQL(String tableName, String[] columnNames, String[] sqlTypeNames,
            boolean[] nillable, String pkeyColumn, SimpleFeatureType featureType) throws SQLException {
        //build the create table sql
        StringBuffer sql = new StringBuffer();
        dialect.encodeCreateTable(sql);

        encodeTableName(tableName, sql, null);

        //encode anything post create table
        dialect.encodePostCreateTable(tableName, sql);

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

//      Object readOnlyMarker = schema.getUserData().get(JDBC_READ_ONLY);
//      if (Boolean.TRUE.equals(readOnlyMarker)) {
//          return new OrientDBJDBCFeatureSource(entry, null);
//      }
        return new OrientDBJDBCFeatureStore(entry, null);
    }

    @Override
    protected String dropTableSQL(SimpleFeatureType featureType, Connection cx)
            throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("DROP CLASS ");

        encodeTableName(featureType.getTypeName(), sql, null);

        sql.append(" UNSAFE");

        return sql.toString();
    }

    protected String selectSQLClean(SimpleFeatureType featureType, Query query) throws IOException, SQLException {
        StringBuffer sql = new StringBuffer();
        sql.append("SELECT ");

        //column names
        selectColumnsClean(featureType, null, query, sql);
        sql.setLength(sql.length() - 1);
        dialect.encodePostSelect(featureType, sql);

        //from
        sql.append(" FROM ");
        encodeTableName(featureType.getTypeName(), sql, query.getHints());

        //filtering
        Filter filter = query.getFilter();
        if (filter != null && !Filter.INCLUDE.equals(filter)) {
            sql.append(" WHERE ");

            //encode filter
            filter(featureType, filter, sql);
        }

        //sorting
        sort(featureType, query.getSortBy(), null, sql);

        // encode limit/offset, if necessary
        applyLimitOffset(sql, query.getStartIndex(), query.getMaxFeatures());

        // add search hints if the dialect supports them
        applySearchHints(featureType, query, sql);

        return sql.toString();
    }

    private void applySearchHints(SimpleFeatureType featureType, Query query, StringBuffer sql) {
        // we can apply search hints only on real tables
        if (virtualTables.containsKey(featureType.getTypeName())) {
            return;
        }

        dialect.handleSelectHints(sql, featureType, query);
    }

    void selectColumnsClean(SimpleFeatureType featureType, String prefix, Query query, StringBuffer sql)
            throws IOException {

        //primary key
        PrimaryKey key = null;
        try {
            key = getPrimaryKey(featureType);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Set<String> pkColumnNames = getColumnNames(key);

        // we need to add the primary key columns only if they are not already exposed
        for (PrimaryKeyColumn col : key.getColumns()) {
            dialect.encodeColumnName(prefix, col.getName(), sql);
            if (prefix != null) {
                //if a prefix is specified means we are joining so use a prefix to avoid clashing
                // with primary key columsn with the same name from other tables in the join
                dialect.encodeColumnAlias(prefix + "_" + col.getName(), sql);
            }
            sql.append(",");
        }

        //other columns
        for (AttributeDescriptor att : featureType.getAttributeDescriptors()) {
            String columnName = att.getLocalName();
            // skip the eventually exposed pk column values
            if (pkColumnNames.contains(columnName)) {
                continue;
            }

            String alias = null;
            if (att.getUserData().containsKey(JDBC_COLUMN_ALIAS)) {
                alias = (String) att.getUserData().get(JDBC_COLUMN_ALIAS);
            }

            dialect.encodeColumnName(prefix, columnName, sql);

            if (alias != null) {
                dialect.encodeColumnAlias(alias, sql);
            }

            sql.append(",");
        }
    }

    @Override
    protected String insertSQL(SimpleFeatureType featureType, SimpleFeature feature,
            KeysFetcher keysFetcher, Connection cx) throws SQLException, IOException {
        BasicSQLDialect dialect = (BasicSQLDialect) getSQLDialect();

        StringBuffer sql = new StringBuffer();
        sql.append("INSERT INTO ");
        encodeTableName(featureType.getTypeName(), sql, null);

        //column names
        sql.append(" ( ");

        for (int i = 0; i < featureType.getAttributeCount(); i++) {
            String colName = featureType.getDescriptor(i).getLocalName();
            // skip the pk columns in case we have exposed them
            if (keysFetcher.isKey(colName)) {
                continue;
            }
            dialect.encodeColumnName(null, colName, sql);
            sql.append(",");
        }

        //primary key values
        keysFetcher.addKeyColumns(sql);
        sql.setLength(sql.length() - 1);

        //values
        sql.append(" ) VALUES ( ");

        for (int i = 0; i < featureType.getAttributeCount(); i++) {
            AttributeDescriptor att = featureType.getDescriptor(i);
            String colName = att.getLocalName();
            // skip the pk columns in case we have exposed them, we grab the
            // value from the pk itself
            if (keysFetcher.isKey(colName)) {
                continue;
            }

            Class binding = att.getType().getBinding();

            Object value = feature.getAttribute(colName);

            if (value == null) {
                if (!att.isNillable()) {
                    throw new IOException("Cannot set a NULL value on the not null column "
                            + colName);
                }

                sql.append("null");
            } else {
                if (Geometry.class.isAssignableFrom(binding)) {
                    try {
                        Geometry g = (Geometry) value;
                        int srid = getGeometrySRID(g, att);
                        int dimension = getGeometryDimension(g, att);
                        dialect.encodeGeometryValue(g, dimension, srid, sql);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    dialect.encodeValue(value, binding, sql);
                }
            }

            sql.append(",");
        }

        feature.getUserData().put("fid", featureType.getTypeName() + ".");
        sql.setLength(sql.length() - 1);  //remove last comma

        sql.append(")");

        return sql.toString();
    }

}
