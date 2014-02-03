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

import java.util.concurrent.Callable;

import org.exist.Database;
import org.exist.dom.QName;
import org.exist.security.Subject;
import org.exist.storage.DBBroker;
import org.exist.xquery.Cardinality;
import org.exist.xquery.Expression;
import org.exist.xquery.Function;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Item;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;
import org.exist.xquery.value.StringValue;
import org.exist.xquery.value.Type;

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
                new FunctionParameterSequenceType("expression", Type.ITEM, Cardinality.EXACTLY_ONE, "")
            },
            new FunctionReturnSequenceType(Type.NODE, Cardinality.ZERO_OR_MORE, "the results of the evaluated expression")
        ),
    };

    public Submit(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }
    
    public Sequence eval(Sequence contextSequence, Item contextItem) throws XPathException {
        
        return new StringValue(
            Module.submit(new RunFunction(getContext(), contextSequence, getArgument(0)))
        );
    }
    
    
    class RunFunction implements Callable<Sequence> {
        
        Database db;
        Subject subject;

        XQueryContext context;
        Sequence contextSequence;
        
        Expression expr;
        
        public RunFunction(XQueryContext context, Sequence contextSequence, Expression expr) {
            final DBBroker broker = context.getBroker();
            db = broker.getDatabase();
            subject = broker.getSubject();

            this.context = context.copyContext();
            this.contextSequence = contextSequence;
            
            //XXX: copy!!! and replace context
            this.expr = expr;
        }

        @Override
        public Sequence call() throws Exception {
            
            DBBroker broker = null;
            try {
                broker = db.get(subject);
                
                return expr.eval(contextSequence, null);
            } finally {
                db.release(broker);
            }
        }
    }
}
