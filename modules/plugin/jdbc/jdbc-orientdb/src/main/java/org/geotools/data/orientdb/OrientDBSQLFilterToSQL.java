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

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequence;
import com.vividsolutions.jts.geom.CoordinateSequenceFilter;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LinearRing;
import org.geotools.data.jdbc.FilterToSQL;
import org.geotools.filter.FilterCapabilities;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.DistanceBufferOperator;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;

import java.io.IOException;
import org.geotools.geometry.GeneralDirectPosition;
import org.geotools.referencing.CRS;
import org.geotools.referencing.operation.DefaultCoordinateOperationFactory;
import org.opengis.geometry.DirectPosition;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.CoordinateOperation;
import org.opengis.referencing.operation.TransformException;

public class OrientDBSQLFilterToSQL extends FilterToSQL {

    public OrientDBSQLFilterToSQL() {
        super();
    }

    @Override
    protected FilterCapabilities createFilterCapabilities() {
        FilterCapabilities caps = super.createFilterCapabilities();
        caps.addType(BBOX.class);
        caps.addType(Contains.class);
        //caps.addType(Crosses.class);
        caps.addType(Disjoint.class);
        caps.addType(Equals.class);
        caps.addType(Intersects.class);
        caps.addType(Overlaps.class);
        caps.addType(Touches.class);
        caps.addType(Within.class);
        caps.addType(Beyond.class);

        return caps;
    }

    private static void clampLongitude(Coordinate coordinate) {
        if (coordinate.x > 180.) {
            coordinate.x = 180.;
        }
        if (coordinate.x < -180.) {
            coordinate.x = -180.;
        }
    }

    private static void clampLattitude(Coordinate coordinate) {
        if (coordinate.y > 90.) {
            coordinate.y = 90.;
        }
        if (coordinate.y < -90.) {
            coordinate.y = -90.;
        }
    }
    
    private static void clampEasting(Coordinate coordinate){
        if (coordinate.x < -84702.61914736108){
            coordinate.x = -84702.61914736108;
        }
        if (coordinate.x > 676223.7241900009){
            coordinate.x = 676223.7241900009;
        }
    }
    
    private static void clampNorthing(Coordinate coordinate){
        if (coordinate.y < -9272.577651805477){
            coordinate.y = -9272.577651805477;
        }
        if (coordinate.y > 1242876.667023777){
            coordinate.y = 1242876.667023777;
        }
    }

    private static void clampCoordinates(Geometry g, Integer currentSRID) {
        if (currentSRID == null){
          return;
        }
        Coordinate[] coordinates = g.getCoordinates();
        for (Coordinate coordinate : coordinates) {
            if (currentSRID == 4326){
                clampLongitude(coordinate);
                clampLattitude(coordinate);
            }
            else if (currentSRID == 27700){
                clampEasting(coordinate);
                clampNorthing(coordinate);
            }
        }        
    }
    
    public static void transformGeometryToWGS(Geometry g, Integer currentSRID){
      if (currentSRID == null || currentSRID == -1){
        return;
      }
      
      g.apply(new CoordinateSequenceFilter() {
        @Override
        public void filter(CoordinateSequence seq, int i) {
          Coordinate coordinate = seq.getCoordinate(i);
          try{
              CoordinateReferenceSystem wgs84crs = CRS.decode("EPSG:4326");//crsFac.createCoordinateReferenceSystem("4326");
              CoordinateReferenceSystem osgbCrs = CRS.decode("EPSG:" + currentSRID);
              CoordinateOperation op = new DefaultCoordinateOperationFactory().createOperation(osgbCrs, wgs84crs);
              DirectPosition eastNorth = new GeneralDirectPosition(coordinate.x, coordinate.y);
              DirectPosition latLng = op.getMathTransform().transform(eastNorth, eastNorth);
              //longitude
              seq.setOrdinate(i, 0, latLng.getOrdinate(0));
              //latitude
              seq.setOrdinate(i, 1, latLng.getOrdinate(1));
          }
          catch (FactoryException | TransformException exc){
              exc.printStackTrace();
          }
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        public boolean isGeometryChanged() {
            return true;
        }
      });
      
//      if (currentSRID == 27700){
//        Coordinate[] coords = g.getCoordinates();
//        for (Coordinate coordinate : coords) {
////          CRSAuthorityFactory crsFac = ReferencingFactoryFinder.getCRSAuthorityFactory("EPSG", null);
//          try{
//            CoordinateReferenceSystem wgs84crs = CRS.decode("EPSG:4326");//crsFac.createCoordinateReferenceSystem("4326");
//            CoordinateReferenceSystem osgbCrs = CRS.decode("EPSG:" + currentSRID);
//            CoordinateOperation op = new DefaultCoordinateOperationFactory().createOperation(osgbCrs, wgs84crs);
//            DirectPosition eastNorth = new GeneralDirectPosition(coordinate.x, coordinate.y);
//            DirectPosition latLng = op.getMathTransform().transform(eastNorth, eastNorth);
//            //longitude
//            coordinate.x = latLng.getOrdinate(0);
//            //latitude
//            coordinate.y = latLng.getOrdinate(1);
//          }
//          catch (FactoryException | TransformException exc){
//              exc.printStackTrace();
//          }
//        }
//      }
        int a = 0;
        ++a;
    }
    
    public static void transformGeometryFromWGS(Geometry g, Integer targetSrid){      
        if (targetSrid == null){
          return;
        }
        
        g.apply(new CoordinateSequenceFilter() {
          @Override
          public void filter(CoordinateSequence seq, int i) {
              Coordinate coordinate = seq.getCoordinate(i);
              try{
                CoordinateReferenceSystem wgs84crs = CRS.decode("EPSG:4326");
                CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:" + targetSrid);
                CoordinateOperation op = new DefaultCoordinateOperationFactory().createOperation(wgs84crs, targetCRS);
                DirectPosition latLng = new GeneralDirectPosition(coordinate.x, coordinate.y);
                DirectPosition otherSystemCoords = op.getMathTransform().transform(latLng, latLng);
                //longitude
                seq.setOrdinate(i, 0, otherSystemCoords.getOrdinate(0));
                //latitude
                seq.setOrdinate(i, 1, otherSystemCoords.getOrdinate(1));
              }
              catch (FactoryException | TransformException exc){
                  exc.printStackTrace();
              }
          }

          @Override
          public boolean isDone() {
            return false;
          }

          @Override
          public boolean isGeometryChanged() {
            return true;
          }
        });
        
//        Coordinate[] coords = g.getCoordinates();
//        for (Coordinate coordinate : coords) {
//            try{
//                CoordinateReferenceSystem wgs84crs = CRS.decode("EPSG:4326");
//                CoordinateReferenceSystem targetCRS = CRS.decode("EPSG:" + targetSrid);
//                CoordinateOperation op = new DefaultCoordinateOperationFactory().createOperation(wgs84crs, targetCRS);
//                DirectPosition latLng = new GeneralDirectPosition(coordinate.x, coordinate.y);
//                DirectPosition otherSystemCoords = op.getMathTransform().transform(latLng, latLng);
//                //longitude
//                coordinate.x = otherSystemCoords.getOrdinate(0);
//                //latitude
//                coordinate.y = otherSystemCoords.getOrdinate(1);
//            }
//            catch (FactoryException | TransformException exc){
//                exc.printStackTrace();
//            }
//        }
        int a = 0;
        ++a;
    }
    
    private void transformGeometryToWGS(Geometry g){
      transformGeometryToWGS(g, currentSRID);
    }

    @Override
    protected void visitLiteralGeometry(Literal expression) throws IOException {
        Geometry g = (Geometry) evaluateLiteral(expression, Geometry.class);
        clampCoordinates(g, currentSRID);
        transformGeometryToWGS(g);
        clampCoordinates(g, 4326);
        if (g instanceof LinearRing) {
            //WKT does not support linear rings
            g = g.getFactory().createLineString(((LinearRing) g).getCoordinateSequence());
        }
        out.write("ST_GeomFromText('" + g.toText() + "')");                
    }

    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            PropertyName property, Literal geometry, boolean swapped, Object extraData) {

        return visitBinarySpatialOperatorEnhanced(filter, (Expression) property, (Expression) geometry,
                swapped, extraData);
    }

    @Override
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter, Expression e1,
            Expression e2, Object extraData) {
        return visitBinarySpatialOperatorEnhanced(filter, e1, e2, false, extraData);
    }

    /**
     *
     * @param filter
     * @param e1
     * @param e2
     * @param swapped
     * @param extraData
     * @return
     */
    protected Object visitBinarySpatialOperatorEnhanced(BinarySpatialOperator filter, Expression e1,
            Expression e2, boolean swapped, Object extraData) {

        try {

            if (filter instanceof DistanceBufferOperator) {
                out.write("ST_Distance(");
                e1.accept(this, extraData);
                out.write(", ");
                e2.accept(this, extraData);
                out.write(")");

                if (filter instanceof DWithin) {
                    out.write("<");
                } else if (filter instanceof Beyond) {
                    out.write(">");
                } else {
                    throw new RuntimeException("Unknown distance operator");
                }
                out.write(Double.toString(((DistanceBufferOperator) filter).getDistance()));
            } else if (filter instanceof BBOX) {
                out.write("ST_Intersects(");
                e1.accept(this, extraData);
                out.write(",");
                e2.accept(this, extraData);
                out.write(") = true");
            } else {
                boolean equalsTrueNecessary = true;
              
                if (filter instanceof Contains) {
                    out.write("ST_Contains(");
//                } else if (filter instanceof Crosses) {
//                    out.write("ST_Crosses(");
                } else if (filter instanceof Disjoint) {
                    out.write("ST_Disjoint(");
                } else if (filter instanceof Equals) {
                    out.write("ST_Equals(");
                } else if (filter instanceof Intersects) {
                    out.write("ST_Intersects(");
                } else if (filter instanceof Overlaps) {
                    out.write("ST_Overlaps(");
//                } else if (filter instanceof Touches) {                  
//                    out.write("ST_Touches(");
                } else if (filter instanceof Within) {
                    out.write("ST_Within(");
                } else {
                    throw new RuntimeException("Unknown operator: " + filter);
                }

                if (swapped) {
                    e2.accept(this, extraData);
                    out.write(", ");
                    e1.accept(this, extraData);
                } else {
                    e1.accept(this, extraData);
                    out.write(", ");
                    e2.accept(this, extraData);
                }

                out.write(")");
                if (equalsTrueNecessary){
                  out.write(" = true");
                }
                                
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return extraData;
    }
}
