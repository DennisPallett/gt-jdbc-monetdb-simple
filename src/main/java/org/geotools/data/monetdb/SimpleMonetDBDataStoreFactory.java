/*
 * The MIT License (MIT)
 * 
 * Copyright (c) 2014 Dennis Pallett
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.geotools.data.monetdb;

import java.awt.RenderingHints;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.logging.Logger;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFactorySpi;
import org.geotools.data.AbstractDataStoreFactory;
import org.geotools.util.KVP;

public class SimpleMonetDBDataStoreFactory  extends AbstractDataStoreFactory {
    private static final Logger LOGGER = Logger.getLogger("org.geotools.data.monetdb.SimpleMonetDBDataStoreFactory");
    
    public static final Param HOSTNAME_PARAM = new Param("hostname", String.class, "hostname", true, "localhost");
    public static final Param PORT_PARAM = new Param("port", Integer.class, "port number", true, 50000);
    public static final Param SCHEMA_PARAM = new Param("schema", String.class, "Database schema", true, "sys");
    public static final Param DATABASE_PARAM = new Param("database", String.class, "Database name", true);
    public static final Param USERNAME_PARAM = new Param("username", String.class, "username", true, null);
    public static final Param PASSWORD_PARAM = new Param("password", String.class, "password", true, null, new KVP(new Object[] {
        "isPassword", Boolean.valueOf(true)
    }));

    @Override
    public String getDisplayName() {
        return "Simple MonetDB DataStore";
    }

    @Override
    public String getDescription() {
        return "Simple DataStore for MonetDB, without any GIS extensions";
    }

    @Override
    public Param[] getParametersInfo() {
        return (new Param[] {
            HOSTNAME_PARAM, PORT_PARAM, SCHEMA_PARAM, DATABASE_PARAM, USERNAME_PARAM, PASSWORD_PARAM
        });
    }

    @Override
    public Map<RenderingHints.Key, ?> getImplementationHints() {
        return null;
    }
    
    @Override
    public DataStore createDataStore(Map<String, Serializable> params) throws IOException {
        String hostname = (String)HOSTNAME_PARAM.lookUp(params);
        int port = ((Integer)PORT_PARAM.lookUp(params)).intValue();
        String schema = (String)SCHEMA_PARAM.lookUp(params);
        String database = (String)DATABASE_PARAM.lookUp(params);
        String username = (String)USERNAME_PARAM.lookUp(params);
        String password = (String)PASSWORD_PARAM.lookUp(params);
        
        return new SimpleMonetDBDataStore(hostname, port, schema, database, username, password);
    }

    @Override
    public DataStore createNewDataStore(Map<String, Serializable> map) throws IOException {
         throw new UnsupportedOperationException("SimpleMonetDBDataStore is read-only");
    }
}
