/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2014 The eXist Project
 *  http://exist-db.org
 *
 *  This program is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public License
 *  as published by the Free Software Foundation; either version 2
 *  of the License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
 */
package org.exist.executor;

import java.util.Map;

import org.exist.Database;
import org.exist.security.xacml.AccessContext;
import org.exist.storage.BrokerPool;
import org.exist.storage.DBBroker;
import org.exist.util.Configuration;
import org.exist.xquery.Module;
import org.exist.xquery.XQuery;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.Sequence;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
public class ExecutorTest {
    
    static Database db;
    
    @Test
    public void testName() throws Exception {
        Sequence result = eval("let $a := 'a' return executor:submit(xmldb:store('/db', 'test.xml', <a>{upper-case($a)}</a>))");
        
        assertEquals(1, result.getItemCount());
        
        assertEquals(36, result.itemAt(0).getStringValue().length());
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {

        Configuration cfg = new Configuration();
        
        BrokerPool.configure(1, 10, cfg);

        db = BrokerPool.getInstance();

        cfg.setProperty(XQueryContext.PROPERTY_XQUERY_RAISE_ERROR_ON_FAILED_RETRIEVAL, true);

        final Map<String, Class<? extends Module>> builtInModules = 
                (Map)cfg.getProperty( XQueryContext.PROPERTY_BUILT_IN_MODULES );

        builtInModules.put(
                org.exist.executor.Module.NAMESPACE_URI, 
                org.exist.executor.Module.class);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        db.shutdown();
    }
    
    private Sequence eval(String query) throws Exception {
        DBBroker broker = null;
        XQuery xquery = null;

        try {
            broker = db.get(db.getSecurityManager().getSystemSubject());
            xquery = broker.getXQueryService();

            return xquery.execute(query, null, AccessContext.TEST);

        } finally {
            db.release(broker);
        }
    }
}
