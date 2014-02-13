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
import org.exist.xquery.BasicFunction;
import org.exist.xquery.FunctionSignature;
import org.exist.xquery.XPathException;
import org.exist.xquery.XQueryContext;
import org.exist.xquery.value.FunctionParameterSequenceType;
import org.exist.xquery.value.FunctionReturnSequenceType;
import org.exist.xquery.value.Sequence;
import org.exist.xquery.value.SequenceType;

import java.util.concurrent.Future;

import static org.exist.executor.Module.*;
import static org.exist.xquery.Cardinality.EXACTLY_ONE;
import static org.exist.xquery.value.BooleanValue.valueOf;
import static org.exist.xquery.value.Type.BOOLEAN;
import static org.exist.xquery.value.Type.STRING;

/**
 * @author <a href="mailto:gazdovsky@gmail.com">Evgeny Gazdovsky</a>
 *
 */
public class IsDone extends BasicFunction {

    public final static FunctionSignature signatures[] = {
            new FunctionSignature(
                    new QName("is-done", NAMESPACE_URI, PREFIX),
                    "Check is task done.",
                    new SequenceType[] {
                            new FunctionParameterSequenceType("id", STRING, EXACTLY_ONE, ""),
                    },
                    new FunctionReturnSequenceType(BOOLEAN, EXACTLY_ONE, "Returns true() if task has been canceled and false() otherwise")
            ),
    };

    public IsDone(XQueryContext context, FunctionSignature signature) {
        super(context, signature);
    }
    
    @Override
    public Sequence eval(Sequence[] args, Sequence contextSequence) throws XPathException {
        String id = args[0].itemAt(0).getStringValue();
        Future future = futures.get(id);
        if (future == null) throw new XPathException("Unknown task" + id);
        return valueOf(future.isDone());
    }

}
