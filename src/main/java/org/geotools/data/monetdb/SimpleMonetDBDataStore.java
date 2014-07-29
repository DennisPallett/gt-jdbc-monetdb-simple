/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.monetdb;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.data.GmlObjectStore;
import org.geotools.data.Query;
import org.geotools.data.store.ContentDataStore;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.factory.Hints;
import org.geotools.feature.NameImpl;
import org.opengis.feature.type.Name;
import org.opengis.filter.identity.GmlObjectId;

/**
 *
 * @author
 * Dennis
 */
public class SimpleMonetDBDataStore extends ContentDataStore {
    private static final Logger LOGGER = Logger.getLogger("org.geotools.data.monetdb.SimpleMonetDBDataStore");
    
    protected String schema;
    protected Connection conn;
    
    public SimpleMonetDBDataStore (String hostname, int port, String schema, String database, String user, String password) {
        this.schema = schema;
        this.conn = this.setupConnection(hostname, port, database, user, password);
        
        LOGGER.fine("SimpleMonetDBDatastore created");
    }
    
    public String getDatabaseSchema() {
        return schema;
    }
    
    public Connection getConnection () {
        return this.conn;
    }
    
    protected Connection setupConnection(String hostname, int port, String database, String user, String password) {
        try {
            Class.forName("nl.cwi.monetdb.jdbc.MonetDriver");
        } catch (ClassNotFoundException e) {
            LOGGER.severe("Where is your JDBC Driver (nl.cwi.monetdb.jdbc.MonetDriver)? Include in your library path!");
            e.printStackTrace();
            return null;
        }
        
        LOGGER.fine("JDBC Driver Registered!");
	Connection connection = null;
	try {
            StringBuilder connUrl = new StringBuilder();
            connUrl.append("jdbc:monetdb://");
            connUrl.append(hostname).append(":").append(port).append("/").append(database); 
                    
            connection = DriverManager.getConnection(connUrl.toString(), user, password);
        } catch(SQLException e) {
            LOGGER.severe("Connection Failed! Check output console");
            e.printStackTrace();
            return null;
	}
	
        if(connection == null) LOGGER.severe("Failed to make connection!");
        
	return connection;
    }

    @Override
    protected List<Name> createTypeNames() throws IOException {
        List<Name> ret = new Vector<Name>();

        try {
            PreparedStatement q = conn.prepareStatement("SELECT * from geometry_columns WHERE LOWER(type) = 'point'");
            ResultSet res = q.executeQuery();
            
            while(res.next()) {
                String name = res.getString("f_table_name");
                ret.add(new NameImpl(name));
            }            
            
            res.close();
            q.close();           
        } catch (SQLException e) {
            throw new IOException(e);
        }        

        return ret;
    }

    @Override
    protected ContentFeatureSource createFeatureSource(ContentEntry entry) throws IOException {
        return new SimpleMonetDBFeatureSource(entry, null);
    }
    
    @Override
    public void dispose () {
        super.dispose();
        
        if (this.conn != null) {
            try { 
                if (this.conn.isClosed() == false) {
                    this.conn.close();
                }
            } catch (SQLException ex) {
                LOGGER.warning("Unable to close connection");
            }
        }
    }


    
}
