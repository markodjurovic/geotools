/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.jdbc;

import java.sql.SQLException;

/**
 *
 * @author marko
 */
public class OrientDBreaderCallback implements JDBCReaderCallback {

    @Override
    public void finish(JDBCFeatureReader reader) {
        if (reader.rs != null) {
            try {
                reader.rs.close();
            } catch (SQLException exc) {
                throw new RuntimeException(exc);
            }
        }
    }

}
