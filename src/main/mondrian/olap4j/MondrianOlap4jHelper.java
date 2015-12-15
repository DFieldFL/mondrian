/*
* This software is subject to the terms of the Eclipse Public License v1.0
* Agreement, available at the following URL:
* http://www.eclipse.org/legal/epl-v10.html.
* You must accept the terms of that agreement to use this software.
*
* Copyright (c) 2002-2015 Pentaho Corporation..  All rights reserved.
*/

package mondrian.olap4j;

import mondrian.olap.MondrianDef;
import mondrian.rolap.RolapBaseCubeMeasure;
import mondrian.rolap.RolapConnection;
import mondrian.rolap.RolapStar;
import org.apache.log4j.Logger;
import org.olap4j.OlapConnection;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

/**
 * Helper for {@link mondrian.olap4j.MondrianOlap4jConnection}.
 *
 * <p>This helper is used to get information of the MondrianOlap4jConnection.</p>
 *
 * @author bgroves
 * @since December 14, 2015
 */
public class MondrianOlap4jHelper {

  private static final Logger LOGGER =
    Logger.getLogger( MondrianOlap4jHelper.class );

  private MondrianOlap4jConnection connection;
  private DatabaseMetaData databaseMetaData;

  /**
   * Constructor used to create a helper for an {@link mondrian.olap4j.MondrianOlap4jConnection}.
   *
   * @param connection MondrianOlap4jConnection
   */
  public MondrianOlap4jHelper( OlapConnection connection ) {
    try {
      this.connection = (MondrianOlap4jConnection) connection;

      RolapConnection rolapConnection = this.connection.unwrap( RolapConnection.class );
      this.databaseMetaData = rolapConnection.getDataSource().getConnection().getMetaData();
    } catch ( ClassCastException e ) {
      LOGGER.warn( "Connection is not a Mondrian Connection." );
    } catch ( SQLException e ) {
      LOGGER.error( "Error retrieving database meta data from Mondrian Connection." );
    }
  }

  /**
   * Returns the column type of a {@link mondrian.rolap.RolapMeasure}.
   *
   * @param rolapMeasure Measure used to look up column type
   * @return {@link java.sql.Types}
   */
  public int getColumnType( RolapBaseCubeMeasure rolapMeasure ) {
    int type = Types.NULL;

    if ( connection == null || !"Column".equals( rolapMeasure.getMondrianDefExpression().getName() ) ) {
      return type;
    }

    try {
      RolapStar.Measure measure = (RolapStar.Measure) rolapMeasure.getStarMeasure();

      String tableName = measure.getTable().getTableName();
      String columnName = ( (MondrianDef.Column) rolapMeasure.getMondrianDefExpression() ).getColumnName();
      ResultSet rs = databaseMetaData.getColumns( null, null, tableName, columnName );
      if ( rs.next() ) {
        type = Integer.valueOf( rs.getString( "DATA_TYPE" ) );
      }
    } catch ( Exception e ) {
      LOGGER.error( "Error retrieving column type from Database meta data." );
    }

    return type;
  }
}
