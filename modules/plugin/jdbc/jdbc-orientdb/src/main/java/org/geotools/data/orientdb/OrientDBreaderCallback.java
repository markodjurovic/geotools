/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.orientdb;

import java.sql.SQLException;
import org.geotools.jdbc.JDBCFeatureReader;
import org.geotools.jdbc.JDBCReaderCallback;

/**
 *
 * @author marko
 */
public class OrientDBreaderCallback implements JDBCReaderCallback{
  
    @Override
    public void finish(JDBCFeatureReader reader) {
        if (reader.getRs() != null){
            try{
                reader.getRs().close();
            }
            catch (SQLException exc){
                throw new RuntimeException(exc);
            }
        }
    }
  
}
