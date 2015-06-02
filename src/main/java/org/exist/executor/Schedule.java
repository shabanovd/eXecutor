/*
 *  eXist Open Source Native XML Database
 *  Copyright (C) 2001-2015 The eXist Project
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
import org.exist.xquery.Function;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.*;

import static org.exist.executor.Module.*;
import static org.exist.xquery.Cardinality.EXACTLY_ONE;
import static org.exist.xquery.Cardinality.ZERO_OR_MORE;
import static org.exist.xquery.value.Type.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 *
 */
public class Schedule extends Function {

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName("schedule", NAMESPACE_URI, PREFIX),
                    "Schedule task. ",
                    new SequenceType[] {
                            new FunctionParameterSequenceType("id", STRING, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("scheduler", STRING, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("expression", ITEM, ZERO_OR_MORE, ""),
                            new FunctionParameterSequenceType("time", INTEGER, EXACTLY_ONE, ""),
                    },
                    new FunctionReturnSequenceType(BOOLEAN, ZERO_OR_MORE, "the results of the evaluated expression")
            ),
            new FunctionSignature(
                    new QName("schedule", NAMESPACE_URI, PREFIX),
                    "Submit task. ",
                    new SequenceType[] {
                            new FunctionParameterSequenceType("id", STRING, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("scheduler", STRING, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("expression", ITEM, ZERO_OR_MORE, ""),
                            new FunctionParameterSequenceType("time", INTEGER, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("callback", ITEM, EXACTLY_ONE, ""),
                    },
                    new FunctionReturnSequenceType(BOOLEAN, EXACTLY_ONE, "Returns true() if task has been scheduled and false() otherwise")
            ),
    };

    public Schedule(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }
    
    public Sequence eval(Sequence contextSequence, Item contextItem) throws XPathException {
        String id = getArgument(0).eval(contextSequence, contextItem).itemAt(0).getStringValue();
        Item callback = null;
        if (getArgumentCount()>4) {
            callback = getArgument(4).eval(contextSequence, contextItem).itemAt(0);
        }
        RunFunction f = new RunFunction(id, getContext(), contextSequence, getArgument(2), callback);
        long t = ((IntegerValue) getArgument(3).eval(contextSequence, contextItem).itemAt(0)).getLong();
        String scheduler = getArgument(1).eval(contextSequence, contextItem).itemAt(0).getStringValue();
        return BooleanValue.valueOf(schedule(scheduler, f, t));
    }
}
