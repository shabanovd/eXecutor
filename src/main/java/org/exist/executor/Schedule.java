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

import static org.exist.executor.Module.NAMESPACE_URI;
import static org.exist.executor.Module.PREFIX;
import static org.exist.executor.Module.scheduled;
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
                            new FunctionParameterSequenceType("scheduler", STRING, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("expression", ITEM, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("time", INTEGER, EXACTLY_ONE, ""),
                    },
                    new FunctionReturnSequenceType(ITEM, ZERO_OR_MORE, "the results of the evaluated expression")
            ),
            new FunctionSignature(
                    new QName("schedule", NAMESPACE_URI, PREFIX),
                    "Submit task. ",
                    new SequenceType[] {
                            new FunctionParameterSequenceType("scheduler", STRING, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("expression", ITEM, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("time", INTEGER, EXACTLY_ONE, ""),
                            new FunctionParameterSequenceType("callback", FUNCTION_REFERENCE, EXACTLY_ONE, ""),
                    },
                    new FunctionReturnSequenceType(ITEM, ZERO_OR_MORE, "the results of the evaluated expression")
            ),
    };

    public Schedule(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }
    
    public Sequence eval(Sequence contextSequence, Item contextItem) throws XPathException {
        FunctionReference callback = null;
        if (getArgumentCount()>3) {
            callback = (FunctionReference) getArgument(3).eval(contextSequence, contextItem).itemAt(0);
        }
        RunFunction f = new RunFunction(getContext(), contextSequence, getArgument(1), callback) {
            @Override
            void remove() {
                scheduled.remove(uuid);
            }
        };
        long t = ((IntegerValue) getArgument(2).eval(contextSequence, contextItem).itemAt(0)).getLong();
        String scheduler = getArgument(0).eval(contextSequence, contextItem).itemAt(0).getStringValue();
        return new StringValue(Module.shedule(scheduler, f, t));
    }

}
