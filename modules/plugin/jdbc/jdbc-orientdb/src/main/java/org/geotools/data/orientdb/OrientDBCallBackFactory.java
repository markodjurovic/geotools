/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.orientdb;

import org.geotools.jdbc.JDBCCallbackFactory;
import org.geotools.jdbc.JDBCReaderCallback;

/**
 *
 * @author marko
 */
public class OrientDBCallBackFactory implements JDBCCallbackFactory{

  @Override
  public String getName() {
    return "OrientDB callback factory";
  }

  @Override
  public JDBCReaderCallback createReaderCallback() {
    return new OrientDBreaderCallback();
  }
  
}
