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

import java.util.UUID;
import java.util.concurrent.Callable;

import org.exist.Database;
import org.exist.dom.QName;
import org.exist.security.Subject;
import org.exist.storage.DBBroker;
import org.exist.xquery.*;
import org.exist.xquery.value.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Submit extends Function {
    
    public final static FunctionSignature signatures[] = { 
        new FunctionSignature(
            new QName("submit", Module.NAMESPACE_URI, Module.PREFIX),
            "Submit task. ",
            new SequenceType[] {
                    new FunctionParameterSequenceType("expression", Type.ITEM, Cardinality.EXACTLY_ONE, ""),
                    new FunctionParameterSequenceType("callback", Type.FUNCTION_REFERENCE, Cardinality.ZERO_OR_ONE, "")
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.ZERO_OR_MORE, "the results of the evaluated expression")
        ),
    };

    public Submit(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }
    
    public Sequence eval(Sequence contextSequence, Item contextItem) throws XPathException {

        FunctionReference callback = null;

        Sequence a = getArgument(1).eval(contextSequence, contextItem);
        if (!a.isEmpty()) {
            callback = (FunctionReference) a.itemAt(0);
        }

        String uuid = UUID.randomUUID().toString();
        return new StringValue(
            Module.submit(uuid, new RunFunction(uuid, getContext(), contextSequence, getArgument(0), callback))
        );
    }


    class RunFunction implements Callable<Void> {

        private String uuid;

        Database db;
        Subject subject;

        XQueryContext context;
        Sequence contextSequence;

        Expression expr;
        FunctionReference callback;

        public RunFunction(String uuid, XQueryContext context, Sequence contextSequence, Expression expr, FunctionReference callback) {
            final DBBroker broker = context.getBroker();
            db = broker.getDatabase();
            subject = broker.getSubject();

            this.uuid = uuid;
            this.context = context.copyContext();
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
                    callback.evalFunction(contextSequence, null, new Sequence[]{r});
                }
            } finally {
                Module.futures.remove(uuid);
                db.release(broker);
            }
            return null;
        }
    }
}
