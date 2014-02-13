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
import org.exist.security.Subject;
import org.exist.storage.DBBroker;
import org.exist.xquery.Expression;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionReference;
import org.exist.xquery.value.Sequence;

import java.util.concurrent.Callable;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
abstract class RunFunction implements Callable<Void> {

    Database db;
    Subject subject;

    XQueryContext context;
    Sequence contextSequence;

    Expression expr;
    FunctionReference callback;

    final String id;

    public RunFunction(String id, XQueryContext context, Sequence contextSequence, Expression expr, FunctionReference callback) {
        this.id = id;
        this.context = context.copyContext();
        final DBBroker broker = context.getBroker();
        db = broker.getDatabase();
        subject = broker.getSubject();

        this.contextSequence = contextSequence;
        this.callback = callback;

        //XXX: copy!!! and replace context
        this.expr = expr;
    }

    @Override
    public Void call() throws Exception {
        DBBroker broker = null;
        try {
            broker = db.get(subject);
            Sequence r = expr.eval(contextSequence, null);
            if (callback != null) {
                callback.setContext(context);
                callback.evalFunction(contextSequence, null, new Sequence[]{r});
            }
        } finally {
            remove();
            db.release(broker);
        }
        return null;
    }

    abstract void remove();
}
