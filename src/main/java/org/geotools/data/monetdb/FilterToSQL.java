/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.geotools.data.monetdb;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.geotools.data.jdbc.FilterToSQLException;
import org.geotools.factory.Hints;
import org.geotools.filter.FilterCapabilities;
import org.geotools.filter.function.FilterFunction_strConcat;
import org.geotools.filter.function.FilterFunction_strEndsWith;
import org.geotools.filter.function.FilterFunction_strEqualsIgnoreCase;
import org.geotools.filter.function.FilterFunction_strIndexOf;
import org.geotools.filter.function.FilterFunction_strLength;
import org.geotools.filter.function.FilterFunction_strReplace;
import org.geotools.filter.function.FilterFunction_strStartsWith;
import org.geotools.filter.function.FilterFunction_strSubstring;
import org.geotools.filter.function.FilterFunction_strSubstringStart;
import org.geotools.filter.function.FilterFunction_strToLowerCase;
import org.geotools.filter.function.FilterFunction_strToUpperCase;
import org.geotools.filter.function.FilterFunction_strTrim;
import org.geotools.filter.function.FilterFunction_strTrim2;
import org.geotools.filter.function.math.FilterFunction_abs;
import org.geotools.filter.function.math.FilterFunction_abs_2;
import org.geotools.filter.function.math.FilterFunction_abs_3;
import org.geotools.filter.function.math.FilterFunction_abs_4;
import org.geotools.filter.function.math.FilterFunction_ceil;
import org.geotools.filter.function.math.FilterFunction_floor;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JoinPropertyName;
import org.geotools.jdbc.SQLDialect;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.And;
import org.opengis.filter.BinaryComparisonOperator;
import org.opengis.filter.ExcludeFilter;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterVisitor;
import org.opengis.filter.Id;
import org.opengis.filter.IncludeFilter;
import org.opengis.filter.Not;
import org.opengis.filter.Or;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsGreaterThan;
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo;
import org.opengis.filter.PropertyIsLessThan;
import org.opengis.filter.PropertyIsLessThanOrEqualTo;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNil;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.expression.Add;
import org.opengis.filter.expression.Divide;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.ExpressionVisitor;
import org.opengis.filter.expression.Function;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.Multiply;
import org.opengis.filter.expression.NilExpression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.expression.Subtract;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.BinarySpatialOperator;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.filter.temporal.After;
import org.opengis.filter.temporal.AnyInteracts;
import org.opengis.filter.temporal.Before;
import org.opengis.filter.temporal.Begins;
import org.opengis.filter.temporal.BegunBy;
import org.opengis.filter.temporal.During;
import org.opengis.filter.temporal.EndedBy;
import org.opengis.filter.temporal.Ends;
import org.opengis.filter.temporal.Meets;
import org.opengis.filter.temporal.MetBy;
import org.opengis.filter.temporal.OverlappedBy;
import org.opengis.filter.temporal.TContains;
import org.opengis.filter.temporal.TEquals;
import org.opengis.filter.temporal.TOverlaps;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

/**
 *
 * @author
 * Dennis
 */
public class FilterToSQL  implements FilterVisitor, ExpressionVisitor  {
    private static Logger LOGGER = org.geotools.util.logging.Logging.getLogger("org.geotools.data.monetdb.FilterToSQL");
    
    /** where to write the constructed string from visiting the filters. */
    protected Writer out;
    
    /** the schmema the encoder will be used to be encode sql for */
    protected SimpleFeatureType featureType;
    
    /** the geometry descriptor corresponding to the current binary spatial filter being encoded */
    protected GeometryDescriptor currentGeometry;
    
    /** the srid corresponding to the current binary spatial filter being encoded */
    protected Integer currentSRID;

    /** The dimension corresponding to the current binary spatial filter being encoded */
    protected Integer currentDimension;

    /** inline flag, controlling whether "WHERE" will prefix the SQL encoded filter */
    protected boolean inline = false;
    
    /** Character used to escape database schema, table and column names */
    private String sqlNameEscape = "\"";
    
    /** the CoordinateReferenceSystem of the query */
    private CoordinateReferenceSystem queryCrs;
    
    /**
     * Default constructor
     */
    public FilterToSQL() {
    }
    
    
    public FilterToSQL(Writer out) {
        this.out = out;
    }
    
    /**
     * Sets the writer the encoder will write to.
     */
    public void setWriter(Writer out) {
        this.out = out;
    }

    public void setInline(boolean inline) {
        this.inline = inline;
    }
    
    /**
     * Performs the encoding, sends the encoded sql to the writer passed in.
     *
     * @param filter the Filter to be encoded.
     *
     * @throws OpenGISFilterToOpenGISFilterToSQLEncoderException If filter type not supported, or if there
     *         were io problems.
     */
    public void encode(Filter filter) throws FilterToSQLException {
        if (out == null) throw new FilterToSQLException("Can't encode to a null writer.");
        
        try {
            if (!inline) {
                out.write("WHERE ");
            }

            filter.accept(this, null);

            //out.write(";");
        } catch (java.io.IOException ioe) {
            LOGGER.warning("Unable to export filter" + ioe);
            throw new FilterToSQLException("Problem writing filter: ", ioe);
        }

    }
    
    /**
     * purely a convenience method.
     * 
     * Equivalent to:
     * 
     *  StringWriter out = new StringWriter();
     *  new FilterToSQL(out).encode(filter);
     *  out.getBuffer().toString();
     * 
     * @param filter
     * @return a string representing the filter encoded to SQL.
     * @throws FilterToSQLException
     */
    
    public String encodeToString(Filter filter) throws FilterToSQLException {
        StringWriter out = new StringWriter();
        this.out = out;
        this.encode(filter);
        return out.getBuffer().toString();
    }
    
    /**
     * Performs the encoding, sends the encoded sql to the writer passed in.
     *
     * @param filter the Filter to be encoded.
     *
     * @throws OpenGISFilterToOpenGISFilterToSQLEncoderException If filter type not supported, or if there
     *         were io problems.
     */
    public void encode(Expression expression) throws FilterToSQLException {
        if (out == null) throw new FilterToSQLException("Can't encode to a null writer.");
        expression.accept(this, null);
    }
    
     /**
     * purely a convenience method.
     * 
     * Equivalent to:
     * 
     *  StringWriter out = new StringWriter();
     *  new FilterToSQL(out).encode(filter);
     *  out.getBuffer().toString();
     * 
     * @param filter
     * @return a string representing the filter encoded to SQL.
     * @throws FilterToSQLException
     */
    
    public String encodeToString(Expression expression) throws FilterToSQLException {
        StringWriter out = new StringWriter();
        this.out = out;
        this.encode(expression);
        return out.getBuffer().toString();
    }
    
    /**
     * Sets the CoordinateReferenceSystem of the query
     * 
     * @param queryCrs
     */
    public void setQueryCrs(CoordinateReferenceSystem queryCrs) {
        this.queryCrs = queryCrs;
    }
    
    
    /**
     * Sets the featuretype the encoder is encoding sql for.
     * <p>
     * This is used for context for attribute expressions when encoding to sql. 
     * </p>
     * 
     * @param featureType
     */
    public void setFeatureType(SimpleFeatureType featureType) {
        this.featureType = featureType;
    }
    
    /**
     * Surrounds a name with the SQL escape character.
     *
     * @param name
     *
     * @return Returns a escaped name
     */
    public String escapeName(String name) {
        return sqlNameEscape + name + sqlNameEscape;
    }

    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
            Object extraData) {
        // basic checks
        if (filter == null)
            throw new NullPointerException(
                    "Filter to be encoded cannot be null");
        if (!(filter instanceof BinaryComparisonOperator))
            throw new IllegalArgumentException(
                    "This filter is not a binary comparison, "
                            + "can't do SDO relate against it: "
                            + filter.getClass());

        
        // extract the property name and the geometry literal
        BinaryComparisonOperator op = (BinaryComparisonOperator) filter;
        Expression e1 = op.getExpression1();
        Expression e2 = op.getExpression2();

        if (e1 instanceof Literal && e2 instanceof PropertyName) {
            e1 = (PropertyName) op.getExpression2();
            e2 = (Literal) op.getExpression1();
        }

        if (e1 instanceof PropertyName) {
            // handle native srid
            currentGeometry = null;
            currentSRID = null;
            currentDimension = null;
            if (featureType != null) {
                // going thru evaluate ensures we get the proper result even if the
                // name has
                // not been specified (convention -> the default geometry)
                AttributeDescriptor descriptor = (AttributeDescriptor) e1.evaluate(featureType);
                if (descriptor instanceof GeometryDescriptor) {
                    currentGeometry = (GeometryDescriptor) descriptor;
                    currentSRID = (Integer) descriptor.getUserData().get(
                            JDBCDataStore.JDBC_NATIVE_SRID);
                    currentDimension = (Integer) descriptor.getUserData().get(
                            Hints.COORDINATE_DIMENSION);
                }
            }
        }

        if (e1 instanceof PropertyName && e2 instanceof Literal) {
            //call the "regular" method
            return visitBinarySpatialOperator(filter, (PropertyName)e1, (Literal)e2, filter
                    .getExpression1() instanceof Literal, extraData);
        }
        else {
            throw new UnsupportedOperationException("Join binary spatial operator not supported");
        }
    }
    
    /**
     * Handles the common case of a PropertyName,Literal geometry binary spatial operator.
     */
    protected Object visitBinarySpatialOperator(BinarySpatialOperator filter,
    PropertyName property, Literal geometry, boolean swapped,
    Object extraData) {
        Geometry geom  = (Geometry) geometry.evaluate(extraData, Geometry.class);
        System.out.println(geom.getSRID());
        //geom.setSRID(currentSRID);
        
        if (filter instanceof BBOX) {        
            Coordinate[] coords = geom.getCoordinates();

            double minX = 0;
            double maxX = 0;
            double minY = 0;
            double maxY = 0;

            for(Coordinate coord : coords) {
                if (coord.x < minX) minX = coord.x;
                if (coord.x > maxX) maxX = coord.x;
                if (coord.y < minY) minY = coord.y;
                if (coord.y > maxY) maxY = coord.y;
            }

            try {
                out.write(property.toString());
                out.write("_x >= ");
                out.write(String.valueOf(minX));
                out.write(" AND ");
                out.write(property.toString());
                out.write("_x <= ");
                out.write(String.valueOf(maxX));

                out.write(" AND ");

                out.write(property.toString());
                out.write("_y >= ");
                out.write(String.valueOf(minY));
                out.write(" AND ");
                out.write(property.toString());
                out.write("_y <= ");
                out.write(String.valueOf(maxY));
            } catch (IOException ex) {
                LOGGER.warning(ex.getMessage());
            }
        } else {
            throw new UnsupportedOperationException("Filter " + filter.getClass().getCanonicalName() + " not yet supported");
        }
        
        return extraData;
    }
    
    @Override
    public Object visitNullFilter(Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(ExcludeFilter ef, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(IncludeFilter i, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(And and, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Id id, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Not not, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Or or, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsBetween pib, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsEqualTo piet, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsNotEqualTo pinet, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsGreaterThan pigt, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsGreaterThanOrEqualTo pgt, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsLessThan pilt, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsLessThanOrEqualTo plt, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsLike pil, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsNull pin, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyIsNil pin, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    @Override
    public Object visit(BBOX filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Beyond filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Contains filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Crosses filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Disjoint filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(DWithin filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Equals filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Intersects filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Overlaps filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Touches filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(Within filter, Object extraData) {
        return visitBinarySpatialOperator((BinarySpatialOperator)filter, extraData);
    }

    @Override
    public Object visit(After after, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(AnyInteracts ai, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Before before, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Begins begins, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(BegunBy bb, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(During during, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(EndedBy eb, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Ends ends, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Meets meets, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(MetBy metby, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(OverlappedBy ob, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(TContains tc, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(TEquals te, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(TOverlaps to, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(NilExpression ne, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Add add, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Divide divide, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Function fnctn, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Literal ltrl, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(Multiply mltpl, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Object visit(PropertyName expression, Object extraData) {
        LOGGER.finer("exporting PropertyName");
        
        Class target = null;
        if(extraData instanceof Class) {
            target = (Class) extraData;
        }

        try {
            SimpleFeatureType featureType = this.featureType;

            //check for join
            if (expression instanceof JoinPropertyName) {
                throw new UnsupportedOperationException("JoinPropertyName not yet supported");
            }

            //first evaluate expression against feautre type get the attribute, 
            //  this handles xpath
            AttributeDescriptor attribute = null;
            try {
                attribute = (AttributeDescriptor) expression.evaluate(featureType);
            }
            catch( Exception e ) {
                //just log and fall back on just encoding propertyName straight up
                String msg = "Error occured mapping " + expression + " to feature type";
                LOGGER.log( Level.WARNING, msg, e );
            }
            String encodedField; 
            if ( attribute != null ) {
                encodedField = escapeName(attribute.getLocalName());
            } else {
                // fall back to just encoding the property name
                encodedField = escapeName(expression.getPropertyName());
            }
            
            out.write(encodedField);
    		
        } catch (java.io.IOException ioe) {
            throw new RuntimeException("IO problems writing attribute exp", ioe);
        }
        return extraData;
    }

    @Override
    public Object visit(Subtract sbtrct, Object o) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
    

}
