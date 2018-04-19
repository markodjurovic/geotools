/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.orientdb;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import org.geotools.data.Query;
import org.geotools.data.store.ContentEntry;
import org.geotools.factory.Hints;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.jdbc.ColumnMetadata;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCFeatureSource;
import org.geotools.jdbc.JDBCState;
import org.geotools.jdbc.NullPrimaryKey;
import org.geotools.jdbc.PreparedStatementSQLDialect;
import org.geotools.jdbc.PrimaryKey;
import org.geotools.jdbc.PrimaryKeyColumn;
import org.geotools.jdbc.SQLDialect;
import org.geotools.jdbc.VirtualTable;
import org.opengis.feature.Association;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 *
 * @author mdjurovi
 */
public class OrientDBJDBCFeatureSource extends JDBCFeatureSource{
  
  class ColumnMetadataEx extends ColumnMetadata{
    
    private boolean embedded;
    private String embeddedType;

    public boolean isEmbedded() {
      return embedded;
    }

    public void setEmbedded(boolean embedded) {
      this.embedded = embedded;
    }

    public String getEmbeddedType() {
      return embeddedType;
    }

    public void setEmbeddedType(String embeddedType) {
      this.embeddedType = embeddedType;
    }        
    
  }
  
  public OrientDBJDBCFeatureSource(ContentEntry entry,Query query) throws IOException {
    super(entry,query);      
  }
    
  /**
   * Copy existing feature source
   * @param featureSource jdbc feature source
   * @throws IOException
   */
  public OrientDBJDBCFeatureSource(JDBCFeatureSource featureSource) throws IOException{
      super(featureSource);
  }
  
  @Override
  protected List<ColumnMetadata> getColumnMetadata(Connection cx, String databaseSchema, String tableName, SQLDialect dialect)
            throws SQLException {
        List<ColumnMetadata> result = new ArrayList<>();

        DatabaseMetaData metaData = cx.getMetaData();

        /*
         * <UL>
         * <LI><B>COLUMN_NAME</B> String => column name
         * <LI><B>DATA_TYPE</B> int => SQL type from java.sql.Types
         * <LI><B>TYPE_NAME</B> String => Data source dependent type name, for a UDT the type name
         * is fully qualified
         * <LI><B>COLUMN_SIZE</B> int => column size. For char or date types this is the maximum
         * number of characters, for numeric or decimal types this is precision.
         * <LI><B>BUFFER_LENGTH</B> is not used.
         * <LI><B>DECIMAL_DIGITS</B> int => the number of fractional digits
         * <LI><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)
         * <LI><B>NULLABLE</B> int => is NULL allowed.
         * <UL>
         * <LI>columnNoNulls - might not allow <code>NULL</code> values
         * <LI>columnNullable - definitely allows <code>NULL</code> values
         * <LI>columnNullableUnknown - nullability unknown
         * </UL>
         * <LI><B>COLUMN_DEF</B> String => default value (may be <code>null</code>)
         * <LI>
         * <B>IS_NULLABLE</B> String => "NO" means column definitely does not allow NULL values;
         * "YES" means the column might allow NULL values. An empty string means nobody knows.
         * </UL>
         */
        ResultSet columns = metaData.getColumns(cx.getCatalog(),
                getDataStore().escapeNamePattern(metaData, databaseSchema),
                getDataStore().escapeNamePattern(metaData, tableName),
                "%");
        if(getDataStore().getFetchSize() > 0) {
            columns.setFetchSize(getDataStore().getFetchSize());
        }

        try {
            while (columns.next()) {
                ColumnMetadataEx column = new ColumnMetadataEx();
                column.setName(columns.getString("COLUMN_NAME"));
                column.setTypeName(columns.getString("TYPE_NAME"));
                column.setSqlType(columns.getInt("DATA_TYPE"));
                column.setNullable("YES".equalsIgnoreCase(columns.getString("IS_NULLABLE")));
                column.setBinding(dialect.getMapping(columns, cx));
                String isEmbeddedString = columns.getString("IS_EMBEDDED");
                boolean isEmbedded = isEmbeddedString.equals("YES") ? true : false;
                String embeddedClass = columns.getString("EMBEDDED_TYPE");
                column.setEmbedded(isEmbedded);
                column.setEmbeddedType(embeddedClass);
                
                //support for user defined types, allow the dialect to handle them
                if (column.getSqlType() == Types.DISTINCT) {
                    dialect.handleUserDefinedType(columns, column, cx);
                }
                
                result.add(column);
            }
        } finally {
            getDataStore().closeSafe(columns);
        }

        return result;
    }
  
  @Override
  public SimpleFeatureType buildFeatureType() throws IOException {
        //grab the primary key
        PrimaryKey pkey = getDataStore().getPrimaryKey(entry);
        VirtualTable virtualTable = getDataStore().getVirtualTables().get(entry.getTypeName());
        
        SimpleFeatureTypeBuilder tb = new SimpleFeatureTypeBuilder();
        AttributeTypeBuilder ab = new AttributeTypeBuilder();
        
        // setup the read only marker if no pk or null pk or it's a view
        boolean readOnly = false;
        if(pkey == null || pkey instanceof NullPrimaryKey || virtualTable != null) {
            readOnly = true;
        }

        //set up the name
        String tableName = entry.getName().getLocalPart();
        tb.setName(tableName);

        //set the namespace, if not null
        if (entry.getName().getNamespaceURI() != null) {
            tb.setNamespaceURI(entry.getName().getNamespaceURI());
        } else {
            //use the data store
            tb.setNamespaceURI(getDataStore().getNamespaceURI());
        }

        //grab the state
        JDBCState state = getState();
        
        //grab the schema
        String databaseSchema = getDataStore().getDatabaseSchema();

        //ensure we have a connection
        Connection cx = getDataStore().getConnection(state);
        
        // grab the dialect
        SQLDialect dialect = getDataStore().getSQLDialect();


        //get metadata about columns from database
        try {
            DatabaseMetaData metaData = cx.getMetaData();
            // get metadata about columns from database
            List<ColumnMetadata> columns;
            if (virtualTable != null) {
                columns = getColumnMetadata(cx, virtualTable, dialect, getDataStore());
            } else {
                columns = getColumnMetadata(cx, databaseSchema, tableName, dialect);
            }

            for (ColumnMetadata column_simple : columns) {
                ColumnMetadataEx column = (ColumnMetadataEx)column_simple;
                String name = column.getName();

                //do not include primary key in the type if not exposing primary key columns
                boolean pkColumn = false;
                for ( PrimaryKeyColumn pkeycol : pkey.getColumns() ) {
                    if ( name.equals( pkeycol.getName() ) ) {
                        if ( !state.isExposePrimaryKeyColumns() ) {
                            name = null;
                            break;
                        } else {
                            pkColumn = true;
                        }
                    }
                    // in views we don't know the pk type, grab it now
                    if(pkeycol.getType() == null) {
                        pkeycol.setType(column.getBinding());
                    }
                }
             
                if (name == null) {
                    continue;
                }

                //check for association
                if (getDataStore().isAssociations()) {
                    getDataStore().ensureAssociationTablesExist(cx);

                    //check for an association
                    Statement st = null;
                    ResultSet relationships = null;
                    if ( getDataStore().getSQLDialect() instanceof PreparedStatementSQLDialect ) {
                        st = getDataStore().selectRelationshipSQLPS(tableName, name, cx);
                        relationships = ((PreparedStatement)st).executeQuery();
                    }
                    else {
                        String sql = getDataStore().selectRelationshipSQL(tableName, name);
                        getDataStore().getLogger().fine(sql);
                        
                        st = cx.createStatement();
                        relationships = st.executeQuery(sql);
                    }

                   try {
                        if (relationships.next()) {
                            //found, create a special mapping 
                            tb.add(name, Association.class);

                            continue;
                        }
                    } finally {
                        getDataStore().closeSafe(relationships);
                        getDataStore().closeSafe(st);
                    }
                    
                }

                //first ask the dialect
                Class binding = column.getBinding();

                if (binding == null) {
                    //determine from type name mappings
                    binding = getDataStore().getMapping(column.getTypeName());
                }

                if (binding == null) {
                    //determine from type mappings
                    binding = getDataStore().getMapping(column.getSqlType());
                }
                
                if (binding == null && column.isEmbedded()){
                    binding = getDataStore().getMapping(column.getEmbeddedType());
                }

                // if still not found, ignore the column we don't know about
                if (binding == null) {
                    getDataStore().getLogger().warning("Could not find mapping for '" + name 
                            + "', ignoring the column and setting the feature type read only");
                	readOnly = true;
                	continue;
                }
                
                // store the native database type in the attribute descriptor user data
                ab.addUserData(JDBCDataStore.JDBC_NATIVE_TYPENAME, column.getTypeName());

                // nullability
                if (!column.isNullable()) {
                    ab.nillable(false);
                    ab.minOccurs(1);
                }
                
                AttributeDescriptor att = null;
                
                //determine if this attribute is a geometry or not
                if (Geometry.class.isAssignableFrom(binding)) {
                    //add the attribute as a geometry, try to figure out 
                    // its srid first
                    Integer srid = null;
                    CoordinateReferenceSystem crs = null;
                    try {
                        if(virtualTable != null) {
                            srid = virtualTable.getNativeSrid(name);
                        } else {
                            srid = dialect.getGeometrySRID(databaseSchema, tableName, name, cx);
                        }
                        if(srid != null)
                            crs = dialect.createCRS(srid, cx);
                    } catch (Exception e) {
                        String msg = "Error occured determing srid for " + tableName + "."
                            + name;
                        getDataStore().getLogger().log(Level.WARNING, msg, e);
                    }
                    
                    // compute the dimension too
                    int dimension = 2;
                    try {
                        if(virtualTable != null) {
                            dimension = virtualTable.getDimension(name);
                        } else {
                            dimension = dialect.getGeometryDimension(databaseSchema, tableName, name, cx);
                        }
                    } catch(Exception e) {
                        String msg = "Error occured determing dimension for " + tableName + "."
                                + name;
                        getDataStore().getLogger().log(Level.WARNING, msg, e);
                    }

                    ab.setBinding(binding);
                    ab.setName(name);
                    ab.setCRS(crs);
                    if(srid != null) {
                        ab.addUserData(JDBCDataStore.JDBC_NATIVE_SRID, srid);
                    }
                    ab.addUserData(Hints.COORDINATE_DIMENSION, dimension);
                    att = ab.buildDescriptor(name, ab.buildGeometryType());
                } else {
                    //add the attribute
                    ab.setName(name);
                    ab.setBinding(binding);
                    att = ab.buildDescriptor(name, ab.buildType());
                }
                // mark primary key columns
                if (pkey.getColumn(att.getLocalName()) != null) {
                    att.getUserData().put(JDBCDataStore.JDBC_PRIMARY_KEY_COLUMN, true);
                }
                
                //call dialect callback
                dialect.postCreateAttribute( att, tableName, databaseSchema, cx);
                tb.add(att);
            }

            //build the final type
            SimpleFeatureType ft = tb.buildFeatureType();
            
            // mark it as read only if necessary 
            // (the builder userData method affects attributes, not the ft itself)
            if(readOnly) {
                ft.getUserData().put(JDBCDataStore.JDBC_READ_ONLY, Boolean.TRUE);
            }

            //call dialect callback
            dialect.postCreateFeatureType(ft, metaData, databaseSchema, cx);
            return ft;
        } catch (SQLException e) {
            String msg = "Error occurred building feature type";
            throw (IOException) new IOException(msg).initCause(e);
        } finally {
            getDataStore().releaseConnection( cx, state );
        }
    }
}
