/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.jdbc;

import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.logging.Level;
import org.geotools.data.Query;
import org.geotools.data.orientdb.OrientDBSQLFilterToSQL;
import org.geotools.feature.IllegalAttributeException;
import static org.geotools.jdbc.JDBCFeatureReader.LOGGER;
import org.geotools.util.Converters;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.referencing.operation.TransformException;

/**
 *
 * @author marko
 */
public class OrientDBFeatureReader extends JDBCFeatureReader{    
  
    public OrientDBFeatureReader( String sql, Connection cx, JDBCFeatureSource featureSource, 
            SimpleFeatureType featureType, Query query) 
        throws SQLException {
        super(sql, cx, featureSource, featureType, query);        
    }
    
    public OrientDBFeatureReader( PreparedStatement st, Connection cx, JDBCFeatureSource featureSource, 
            SimpleFeatureType featureType, Query query) 
        throws SQLException {            
        super(st, cx, featureSource, featureType, query);        
    }
    
    public OrientDBFeatureReader(ResultSet rs, Connection cx, int offset, JDBCFeatureSource featureSource, 
        SimpleFeatureType featureType, Query query) throws SQLException {
        super(rs, cx, offset, featureSource, featureType, query);        
    }
    
    @Override
    protected SimpleFeature readNextFeature() throws IOException {
        //grab the connection
        Connection cx;
        try {
            cx = st.getConnection();
        }
        catch (SQLException e) {
            throw (IOException) new IOException().initCause(e);
        }

        // figure out the fid
        String fid;

        try {
            fid = dataStore.encodeFID(pkey,rs,offset);
            if (fid == null) {
                //fid could be null during an outer join
                return null;
            }
            // wrap the fid in the type name
            fid = featureType.getTypeName() + "." + fid;
        } catch (Exception e) {
            throw new RuntimeException("Could not determine fid from primary key", e);
        }

        // round up attributes
        final int attributeCount = featureType.getAttributeCount();
        int[] attributeRsIndex = buildAttributeRsIndex();
        for(int i = 0; i < attributeCount; i++) {
            AttributeDescriptor type = featureType.getDescriptor(i);
            Integer srid = dataStore.getDescriptorSRID(type);
            if (srid == -1){
              srid = null;
            }
            try {
                Object value = null;

                // is this a geometry?
                if (type instanceof GeometryDescriptor) {
                    GeometryDescriptor gatt = (GeometryDescriptor) type;

                    //read the geometry
                    try {
                        value = dataStore.getSQLDialect()
                                .decodeGeometryValue(gatt, rs, offset+attributeRsIndex[i],
                                        geometryFactory, cx, hints);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }

                    if (value != null) {
                        //check to see if a crs was set
                        Geometry geometry = (Geometry) value;
                        OrientDBSQLFilterToSQL.transformGeometryFromWGS(geometry, srid);
                        if ( geometry.getUserData() == null ) {
                            //if not set, set from descriptor
                            geometry.setUserData( gatt.getCoordinateReferenceSystem() );
                        }

                        try {
                            // is position already busy skip it
                            if (screenMap != null) {
                                if (screenMap.canSimplify(geometry.getEnvelopeInternal())) {
                                    if (screenMap.checkAndSet(geometry.getEnvelopeInternal())) {
                                        return null;
                                    } else {
                                        value = screenMap.getSimplifiedShape(geometry);
                                    }
                                }
                            }
                        } catch (TransformException e) {
                            if(LOGGER.isLoggable(Level.WARNING)) {
                                LOGGER.log(Level.WARNING, "Failed to process screenmap checks, proceeding without", e);
                            }
                        }

                    }
                    
                } else {
                    value = rs.getObject(offset+attributeRsIndex[i]);
                }

                // they value may need conversion. We let converters chew the initial
                // value towards the target type, if the result is not the same as the
                // original, then a conversion happened and we may want to report it to the
                // user (being the feature type reverse engineerd, it's unlikely a true
                // conversion will be needed)
                if(value != null) {
                    Class binding = type.getType().getBinding();
                    Object converted = Converters.convert(value, binding);
                    if(converted != null && converted != value) {
                        value = converted;
                        if (dataStore.getLogger().isLoggable(Level.FINER)) {
                            String msg = value + " is not of type " + binding.getName()
                                    + ", attempting conversion";
                            dataStore.getLogger().finer(msg);
                        }
                    }
                }

                builder.add(value);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        // create the feature
        try {
            return builder.buildFeature(fid);
        } catch (IllegalAttributeException e) {
            throw new RuntimeException(e);
        }
    }
}
