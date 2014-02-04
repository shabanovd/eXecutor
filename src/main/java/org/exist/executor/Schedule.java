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

import org.exist.dom.QName;
import org.exist.xquery.*;
import org.exist.xquery.value.*;
import org.exist.xquery.value.StringValue;

import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Schedule extends Function {

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName("schedule", Module.NAMESPACE_URI, Module.PREFIX),
                    "Submit task. ",
                    new SequenceType[] {
                            new FunctionParameterSequenceType("expression", Type.ITEM, Cardinality.EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("time", Type.INTEGER, Cardinality.EXACTLY_ONE, ""),
                    },
                    new FunctionReturnSequenceType(Type.NODE, Cardinality.ZERO_OR_MORE, "the results of the evaluated expression")
            ),
            new FunctionSignature(
                    new QName("schedule", Module.NAMESPACE_URI, Module.PREFIX),
                    "Submit task. ",
                    new SequenceType[] {
                            new FunctionParameterSequenceType("expression", Type.ITEM, Cardinality.EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("time", Type.INTEGER, Cardinality.EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("callback", Type.FUNCTION_REFERENCE, Cardinality.EXACTLY_ONE, ""),
                    },
                    new FunctionReturnSequenceType(Type.NODE, Cardinality.ZERO_OR_MORE, "the results of the evaluated expression")
            ),
    };

    public Schedule(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }
    
    public Sequence eval(Sequence contextSequence, Item contextItem) throws XPathException {
        FunctionReference callback = null;
        if (getArgumentCount()>2) {
            callback = (FunctionReference) getArgument(2).eval(contextSequence, contextItem).itemAt(0);
        }
        RunFunction f = new RunFunction(getContext(), contextSequence, getArgument(0), callback) {
            @Override
            void remove() {
                Module.scheduled.remove(uuid);
            }
        };
        long t = ((IntegerValue) getArgument(1).eval(contextSequence, contextItem).itemAt(0)).getLong();
        return new StringValue(Module.shedule(f, t));
    }

}
