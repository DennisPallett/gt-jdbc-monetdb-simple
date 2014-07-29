/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.monetdb;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.CoordinateSequenceFactory;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.data.FeatureReader;
import org.geotools.data.Transaction;
import org.geotools.factory.Hints;
import org.geotools.feature.IllegalAttributeException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.geotools.jdbc.JDBCFeatureSource;
import org.geotools.util.Converters;
import org.geotools.util.logging.Logging;
import org.opengis.feature.FeatureFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;

/**
 *
 * @author
 * Dennis
 */
public class SimpleMonetDBFeatureReader  implements  FeatureReader<SimpleFeatureType, SimpleFeature> {
    protected static final Logger LOGGER = Logging.getLogger(SimpleMonetDBFeatureReader.class);
    
     /**
     * The feature source the reader originated from. 
     */
    protected SimpleMonetDBFeatureSource featureSource;
    /**
     * the datastore
     */
    protected SimpleMonetDBDataStore dataStore;
    /**
     * schema of features
     */
    protected SimpleFeatureType featureType;
    /**
     * geometry factory used to create geometry objects
     */
    protected GeometryFactory geometryFactory;
     /**
     * hints
     */
    protected Hints hints;
    /**
     * current transaction
     */
    protected Transaction tx;
    /**
     * flag indicating if the iterator has another feature
     */
    protected Boolean next;
    /**
     * feature builder
     */
    protected SimpleFeatureBuilder builder;
    
    /**
     * statement,result set that is being worked from.
     */
    protected Statement st;
    protected ResultSet rs;
    protected Connection conn;
    protected String[] columnNames;
    
    protected int row = 0;
    
    /**
     * offset/column index to start reading from result set
     */
    protected int offset = 0;
    
    public SimpleMonetDBFeatureReader(String sql, Connection conn, SimpleMonetDBFeatureSource featureSource, SimpleFeatureType featureType, Hints hints ) 
    throws SQLException {
        init( featureSource, featureType, hints );
        
        //create the result set
        this.conn = conn;
        st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        rs = st.executeQuery(sql);
    }
    
    protected void init( SimpleMonetDBFeatureSource featureSource, SimpleFeatureType featureType, Hints hints ) {        
        // init base fields
        this.featureSource = featureSource;
        this.dataStore = featureSource.getDataStore();
        this.featureType = featureType;
        this.tx = featureSource.getTransaction();
        this.hints = hints;
        
        //grab a geometry factory... check for a special hint
        geometryFactory = (GeometryFactory) hints.get(Hints.JTS_GEOMETRY_FACTORY);
        if (geometryFactory == null) {
            // look for a coordinate sequence factory
            CoordinateSequenceFactory csFactory = 
                (CoordinateSequenceFactory) hints.get(Hints.JTS_COORDINATE_SEQUENCE_FACTORY);

            if (csFactory != null) {
                geometryFactory = new GeometryFactory(csFactory);
            }
        }

        if (geometryFactory == null) {
            geometryFactory = JTSFactoryFinder.getGeometryFactory(null);
        }
        
        builder = new SimpleFeatureBuilder(featureType);
    }

    @Override
    public SimpleFeatureType getFeatureType() {
        return featureType;
    }

    @Override
    public boolean hasNext() throws IOException {
        ensureOpen();
        
        if (next == null) {
            try {
                next = Boolean.valueOf(rs.next());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        return next.booleanValue();
    }
    
    protected void ensureOpen() throws IOException {
        if ( rs == null ) {
            throw new IOException( "reader already closed" );
        }
    }
    
    @Override
    public SimpleFeature next() throws IOException, IllegalArgumentException, NoSuchElementException {        
        try {
            ensureOpen();
            if(!hasNext()) {
                throw new NoSuchElementException("No more features in this reader, you should call " +
                		"hasNext() to check for feature availability");
            }
            
            String fid = featureSource.getName() + "_" + row++;
            
            // round up attributes
            final int attributeCount = featureType.getAttributeCount();
            for(int i = 0; i < attributeCount; i++) {
                AttributeDescriptor type = featureType.getDescriptor(i);
                String columnName = type.getLocalName();
                
                try {
                    Object value = null;

                     // is this a geometry?
                    if (type instanceof GeometryDescriptor) {
                        GeometryDescriptor gatt = (GeometryDescriptor) type;
                        
                        double x = rs.getDouble(columnName + "_x");
                        double y = rs.getDouble(columnName + "_y");
                        
                        Geometry geom = geometryFactory.createPoint(new Coordinate(x, y));
                        geom.setUserData(gatt.getCoordinateReferenceSystem());
                        
                        value = geom;                        
                    } else {
                        value = rs.getObject(columnName);
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
        } finally {
            // reset the next flag. We do this in a finally block to make sure we
            // move to the next record no matter what, if the current one could
            // not be read there is no salvation for it anyways
            next = null;
        }
    }

    @Override
    public void close() throws IOException {
        builder = null;
        geometryFactory = null;
        next = null;
        
        try {
            if (rs != null) {
                rs.close();
                st.close();
            }
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        rs = null;
        st = null;
    }
    
}
