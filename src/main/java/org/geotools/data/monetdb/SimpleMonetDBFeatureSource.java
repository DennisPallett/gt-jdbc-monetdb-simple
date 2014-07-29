/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.monetdb;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.data.FeatureReader;
import org.geotools.data.Query;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.data.store.ContentEntry;
import org.geotools.data.store.ContentFeatureSource;
import org.geotools.factory.Hints;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.geotools.filter.SortOrder;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.jdbc.ColumnMetadata;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.referencing.CRS;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.feature.type.GeometryType;
import org.opengis.filter.Filter;
import org.opengis.filter.sort.SortBy;
import org.opengis.geometry.primitive.Point;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 *
 * @author
 * Dennis
 */
public class SimpleMonetDBFeatureSource extends ContentFeatureSource {
    private static final Logger LOGGER = Logger.getLogger("org.geotools.data.monetdb.SimpleMonetDBFeatureSource");
    
    protected String typeName;
    
    protected String geometryColumn;
    
    protected int srid = 0;
    
    protected int dimension = 2;
    
    public SimpleMonetDBFeatureSource(ContentEntry entry, Query query) {
        super(entry,query);
        
        typeName = entry.getTypeName();
        
        try {
            this.loadTypeInfo();
        } catch (SQLException ex) {
            Logger.getLogger(SimpleMonetDBFeatureSource.class.getName()).log(Level.SEVERE, "Unable to load type info", ex);
        }
    }
    
    protected void loadTypeInfo () throws SQLException {
        PreparedStatement q = getDataStore().conn.prepareStatement("SELECT f_geometry_column, coord_dimension, srid, type FROM geometry_columns " +
                                                                   "WHERE f_table_schema = ? AND f_table_name = ?");
        
        q.setString(1, getDataStore().getDatabaseSchema());
        q.setString(2, typeName);
        
        ResultSet res = q.executeQuery();
        
        if (res.next() == false) {
            LOGGER.severe("Unable to load info of type from geometry_columns table");
            res.close();
            q.close();
            return;
        }
        
        this.geometryColumn = res.getString("f_geometry_column");
        this.srid = res.getInt("srid");
        this.dimension = res.getInt("coord_dimension");
        
        res.close();
        q.close();
    }
    
    
    public SimpleMonetDBDataStore getDataStore(){
            return (SimpleMonetDBDataStore) super.getDataStore();
    }

    @Override
    protected ReferencedEnvelope getBoundsInternal(Query query) throws IOException {
        LOGGER.severe("filter: "+query.getFilter().getClass().getCanonicalName());
        
        // TODO: handle query filter
        
        ReferencedEnvelope bounds = null;
        try {
            Statement q = getDataStore().getConnection().createStatement();
            
            ResultSet res = q.executeQuery(
                                "SELECT " +
                                "MIN(" + geometryColumn + "_x) AS minx, " +
                                "MAX(" + geometryColumn + "_x) AS maxx, " +
                                "MIN(" + geometryColumn + "_y) AS miny, " +
                                "MAX(" + geometryColumn + "_y) AS maxy " +
                                "FROM " + typeName                
                            );  
            
            if (res.next() == false) throw new IOException("unable to fetch bounds");
            
            bounds = new ReferencedEnvelope(
                        res.getDouble("minx"),
                        res.getDouble("maxx"),
                        res.getDouble("miny"),
                        res.getDouble("maxy"),
                        getSchema().getCoordinateReferenceSystem()
                    );
            
            res.close();
            q.close();
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
        
        return bounds;
    }

    @Override
    protected int getCountInternal(Query query) throws IOException {
        // TODO: support query filter
        
        int count = -1;
        try {
            Statement q = getDataStore().getConnection().createStatement();
            ResultSet res = q.executeQuery("SELECT COUNT(*) FROM " + typeName);

            count = res.getInt(1);

            res.close();
            q.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
        
        return count;
    }

    @Override
    protected FeatureReader<SimpleFeatureType, SimpleFeature> getReaderInternal(Query query) throws IOException {
        // build the actual SQL query for selecting features
        String sql = buildSqlQuery(query);
        
        System.out.println("QUERY: " + sql);
        
        // get connection
        Connection conn = getDataStore().getConnection();
        
        // create the reader
        FeatureReader<SimpleFeatureType, SimpleFeature> reader;
        try {
            reader = new SimpleMonetDBFeatureReader(sql, conn, this, getSchema(), query.getHints());
        } catch (Throwable e) {
             if (e instanceof Error) {
                throw (Error) e;
            } else {
                throw (IOException) new IOException().initCause(e);
            }
        }    
        
        return reader;        
    }
    
    protected String buildSqlQuery(Query query) throws IOException {
        StringBuilder sql = new StringBuilder("SELECT ");
        
        //column names
        selectColumns(getSchema(), null, query, sql);
        sql.setLength(sql.length() - 1);
        
        // from
        sql.append(" FROM ");
        sql.append(typeName);
        
        //filtering
        Filter filter = query.getFilter();
        if (filter != null && !Filter.INCLUDE.equals(filter)) {            
            sql.append(" WHERE ");
            
            //encode filter
            filter(getSchema(), filter, sql);
        }

        //sorting
        sort(getSchema(), query.getSortBy(), null, sql);
        
        // finally encode limit/offset, if necessary
        applyLimitOffset(sql, query);
        
        return sql.toString();
    }
    
    void selectColumns(SimpleFeatureType featureType, String prefix, Query query, StringBuilder sql) 
        throws IOException {
                
        //other columns
        for (AttributeDescriptor att : featureType.getAttributeDescriptors()) {
            String columnName = att.getLocalName();
            
            if (att instanceof GeometryDescriptor) {
               sql.append(columnName + "_x,");
               sql.append(columnName + "_y");
            } else {
                sql.append(columnName);
            }

            sql.append(",");
        }
    }
    
    FilterToSQL filter(SimpleFeatureType featureType, Filter filter, StringBuilder sql) throws IOException {
        
        try {
            // grab the full feature type, as we might be encoding a filter
            // that uses attributes that aren't returned in the results
            FilterToSQL toSQL = new FilterToSQL();
            toSQL.setFeatureType(featureType);
                    
            toSQL.setInline(true);
            sql.append(" ").append(toSQL.encodeToString(filter));
            return toSQL;
        } catch (FilterToSQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Encodes the sort-by portion of an sql query
     * @param featureType
     * @param sort
     * @param key
     * @param sql
     * @throws IOException
     */
    protected void sort(SimpleFeatureType featureType, SortBy[] sort, String prefix, StringBuilder sql) throws IOException {
        if ((sort != null) && (sort.length > 0)) {
            sql.append(" ORDER BY ");

            for (int i = 0; i < sort.length; i++) {
                String order;
                if (sort[i].getSortOrder() == SortOrder.DESCENDING) {
                    order = " DESC";
                } else {
                    order = " ASC";
                }
                
                sql.append(sort[i].getPropertyName());
                sql.append(order);
                sql.append(",");
            }

            sql.setLength(sql.length() - 1);
        }
    }
    
    /**
     * Applies the limit/offset elements to the query if they are specified
     * @param sql The sql to be modified
     * @param the query that holds the limit and offset parameters
     */
    protected void applyLimitOffset(StringBuilder sql, Query query) {
        if(checkLimitOffset(query)) {
            final Integer offset = query.getStartIndex();
            final int limit = query.getMaxFeatures();
            
            sql.append(" LIMIT " + limit);
            
            if (offset != null) {
                sql.append(" OFFSET " + offset);
            }
        }
    }
    
    /**
     * Checks if the query needs limit/offset treatment
     * @param query
     * @return true if the query needs limit/offset treatment
     */
    protected boolean checkLimitOffset(Query query) {        
        // the check the query has at least a non default value for limit/offset
        final Integer offset = query.getStartIndex();
        final int limit = query.getMaxFeatures();
        return limit != Integer.MAX_VALUE || (offset != null && offset > 0);
    }

    @Override
    protected SimpleFeatureType buildFeatureType() throws IOException {
        SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
        AttributeTypeBuilder ab = new AttributeTypeBuilder();
       
        //set up the name
        builder.setName(typeName);
        
        //set the namespace, if not null
        if (entry.getName().getNamespaceURI() != null) {
            builder.setNamespaceURI(entry.getName().getNamespaceURI());
        } else {
            //use the data store
            builder.setNamespaceURI(getDataStore().getNamespaceURI());
        }
        
        //grab the schema
        String databaseSchema = getDataStore().getDatabaseSchema();
        
        Connection conn = getDataStore().getConnection();
        
        boolean isNullableX = false;
        boolean isNullableY = false;
        
        try {
            //get metadata about columns from database
            DatabaseMetaData metaData = conn.getMetaData();
            
            // get metadata about columns from database
            ResultSet columns = metaData.getColumns(conn.getCatalog(), databaseSchema, typeName, "%");
            
            while(columns.next()) {
                String name = columns.getString("COLUMN_NAME");
                String typeName = columns.getString("TYPE_NAME");
                
                boolean isNullable = "YES".equalsIgnoreCase(columns.getString("IS_NULLABLE"));
                
                // skip geometry column parts
                if (name.equals(geometryColumn + "_x")) {
                    isNullableX = isNullable;
                    continue;
                }
                if (name.equals(geometryColumn + "_y")) {
                    isNullableY = isNullable;
                    continue;
                }
                
                // nullability
                if (isNullable == false) {
                    ab.nillable(false);
                    ab.minOccurs(1);
                }
                
                Class<?> mapping = getMapping(typeName);
                
                if (mapping == null) {
                    LOGGER.severe("Unable to find mapping for SQL type '" + typeName + "'");
                    continue;
                }
                
                ab.setName(name);
                ab.setBinding(mapping);
                
                AttributeDescriptor att = ab.buildDescriptor(name, ab.buildType());                
                builder.add(att);
            }
            
            columns.close();
            
        } catch (SQLException ex) {
            Logger.getLogger(SimpleMonetDBFeatureSource.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        ab.setName(geometryColumn);
        ab.setBinding(Point.class);
        
        if (isNullableX == false || isNullableY == false) {
            ab.setNillable(false);
            ab.minOccurs(1);
        }
        
        CRSAuthorityFactory factory = CRS.getAuthorityFactory(true);
        CoordinateReferenceSystem crs = DefaultGeographicCRS.WGS84;
        
        try {
            crs = factory.createCoordinateReferenceSystem("EPSG:" + srid);
        } catch (NoSuchAuthorityCodeException ex) {
            LOGGER.severe("Invalid SRID " + srid);
            ex.printStackTrace();            
        } catch (FactoryException ex) {
            ex.printStackTrace();
        }
        
        ab.setCRS(crs);
        ab.addUserData(JDBCDataStore.JDBC_NATIVE_SRID, srid);
        ab.addUserData(Hints.COORDINATE_DIMENSION, dimension);
        
        AttributeDescriptor att = ab.buildDescriptor(geometryColumn, ab.buildGeometryType());
        builder.add(att);        
        
        //build the final type
        SimpleFeatureType ft = builder.buildFeatureType();
         
        return ft;
    }
    
    @Override
    protected boolean canLimit() {
        return true;
    }
    
    @Override
    protected boolean canOffset() {
        return true;
    }
    
    @Override
    protected boolean canSort() {
        return false;
    }
    
    protected Class<?> getMapping(String sqlTypeName) {
        Map<String, Class<?>> mappings = new HashMap<String, Class<?>>();
        
        mappings.put("point", Point.class);
        mappings.put("linestring", LineString.class);
        mappings.put("polygon", Polygon.class);
        mappings.put("multipoint", MultiPoint.class);
        mappings.put("multilinestring", MultiLineString.class);
        mappings.put("multipolygon", MultiPolygon.class);
        mappings.put("geomcollection", GeometryCollection.class);
        mappings.put("geometry", Geometry.class);
        mappings.put("text", String.class);
        mappings.put("int8", Long.class);
        mappings.put("bigint", Long.class);
        mappings.put("int4", Integer.class);
        mappings.put("bool", Boolean.class);
        mappings.put("boolean", Boolean.class);
        mappings.put("character", String.class);
        mappings.put("varchar", String.class);
        mappings.put("clob", String.class);
        mappings.put("float8", Double.class);
        mappings.put("int", Integer.class);
        mappings.put("float4", Float.class);
        mappings.put("int2", Short.class);
        mappings.put("time", Time.class);
        mappings.put("timetz", Time.class);
        mappings.put("timestamp", Timestamp.class);
        mappings.put("timestamptz", Timestamp.class);
        mappings.put("uuid", UUID.class);
        
        return mappings.get(sqlTypeName);
    }
    
}
