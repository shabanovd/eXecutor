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

import org.exist.Database;
import org.exist.dom.BinaryDocument;
import org.exist.security.Subject;
import org.exist.security.xacml.AccessContext;
import org.exist.source.DBSource;
import org.exist.source.Source;
import org.exist.source.SourceFactory;
import org.exist.source.StringSource;
import org.exist.storage.DBBroker;
import org.exist.storage.XQueryPool;
import org.exist.xquery.*;
import org.exist.xquery.value.AnyURIValue;
import org.exist.xquery.value.FunctionReference;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;

import java.util.concurrent.Callable;

import static org.exist.executor.Module.futures;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
class RunFunction implements Callable<Void> {

    Database db;
    Subject subject;

    XQueryContext context;
    Sequence contextSequence;


    Sequence r;
    //Expression expr;
    Item callback;

    final String id;

    public RunFunction(String id, XQueryContext context, Sequence contextSequence, Expression expr, Item callback) throws XPathException {
        this.id = id;
        context.setShared(true);
        this.context = context.copyContext();
        final DBBroker broker = context.getBroker();
        db = broker.getDatabase();
        subject = broker.getSubject();

        this.contextSequence = contextSequence;
        this.callback = callback;

        //XXX: copy!!! and replace context
        //this.expr = expr;
        this.r = expr.eval(contextSequence, null);
    }

    @Override
    public Void call() throws Exception {
        DBBroker broker = null;
        try {
            broker = db.get(subject);
            //Sequence r = expr.eval(contextSequence, null);
            if (callback != null) {
                if (callback instanceof FunctionReference) {
                    FunctionReference f = (FunctionReference) callback;
                    f.setContext(context);
                    f.evalFunction(contextSequence, null, new Sequence[]{r});
                } else {
                    Source xq;
                    if (callback instanceof AnyURIValue)
                        xq = SourceFactory.getSource(broker, null, callback.getStringValue(), false);
                    else
                        xq = new StringSource(callback.getStringValue());
                    broker.getConfiguration().setProperty(XQueryContext.PROPERTY_XQUERY_RAISE_ERROR_ON_FAILED_RETRIEVAL, true);
                    XQuery xqs = broker.getXQueryService();
                    XQueryContext context = new XQueryContext(broker.getBrokerPool(), AccessContext.XMLDB);
                    CompiledXQuery compiled = xqs.compile(context, xq);
                    xqs.execute(compiled, r);
                }
            }
        } finally {
            remove();
            db.release(broker);
        }
        return null;
    }

    protected void remove() {
        futures.remove(id);
    }
    ;
}
