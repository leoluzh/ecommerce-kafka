package com.lambdasys.ecommerce.database;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class LocalDataBase implements Closeable {

	private final Connection connection;

    public LocalDataBase( String name ) throws Exception {
        String url = "jdbc:sqlite:target/"+name+".db";
        connection = DriverManager.getConnection(url);
    }
    
    public void createIfNotExists( String sql ) {
        try {
            connection.createStatement().execute(sql);
        } catch(SQLException ex) {
            // be careful, the sql could be wrong, be reallllly careful
            ex.printStackTrace();
        }
    }
    
    public boolean update( String statement , Object ...params ) throws SQLException {
        return prepare(statement, params).execute();
    }
    
    public ResultSet query( String statement , Object ...params) throws SQLException {
        return prepare(statement, params).executeQuery();
    }

	private PreparedStatement prepare(String statement, Object... params) throws SQLException {
		var prepareStatement = connection.prepareStatement(statement);
        var index = 1 ;
        for( var param : params ) {
        	prepareStatement.setObject(index+1, param);
        }
		return prepareStatement;
	}

	@Override
	public void close() throws IOException {
		try {
			this.connection.close();
		}catch(Exception ex) {
			throw new IOException(ex);
		}
	}
    
}
