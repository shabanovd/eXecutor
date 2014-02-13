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

import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.FunctionDef;
import org.exist.xquery.XPathException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * @author <a href="mailto:shabanovd@gmail.com">Dmitriy Shabanov</a>
 * 
 */
public class Module extends AbstractInternalModule {

    public final static String NAMESPACE_URI = "http://exist-db.org/executor";
    public final static String PREFIX = "executor";
    private final static String RELEASED_IN_VERSION = "eXist-2.!";
    private final static String DESCRIPTION = "Module provides a way of decoupling task submission from the mechanics of how each task will be run, including details of thread use, scheduling, etc..";

    private final static FunctionDef[] functions = {
            new FunctionDef(Exec.signatures[0], Exec.class),
            new FunctionDef(Exec.signatures[1], Exec.class),
            new FunctionDef(Exec.signatures[2], Exec.class),
            new FunctionDef(Exec.signatures[3], Exec.class),
            new FunctionDef(Exec.signatures[4], Exec.class),
            new FunctionDef(Submit.signatures[0], Submit.class),
            new FunctionDef(Submit.signatures[1], Submit.class),
            new FunctionDef(Schedule.signatures[0], Schedule.class),
            new FunctionDef(Schedule.signatures[1], Schedule.class),
            new FunctionDef(GetDelay.signatures[0], GetDelay.class),
            new FunctionDef(IsCanceled.signatures[0], IsCanceled.class),
            new FunctionDef(IsDone.signatures[0], IsDone.class),
            new FunctionDef(Cancel.signatures[0], Cancel.class),
    };
    
    public Module(Map<String, List<? extends Object>> parameters) {
        super(functions, parameters);
    }

    public String getDefaultPrefix() {
        return PREFIX;
    }

    public String getDescription() {
        return DESCRIPTION;
    }

    public String getNamespaceURI() {
        return NAMESPACE_URI;
    }

    public String getReleaseVersion() {
        return RELEASED_IN_VERSION;
    }

    protected final static Map <String, ExecutorService> executors = new HashMap<String, ExecutorService>();

    protected final static Map<String, Future> futures = new HashMap<String, Future>();

    protected static boolean submit(String name, RunFunction task) throws XPathException {
        if (executors.containsKey(task.id)) return false;
        ExecutorService executor = executors.get(name);
        if (executor == null) throw new XPathException("Unknown executor name: " + name);
        Future future = executor.submit(task);
        futures.put(task.id, future);
        return true;
    }

    protected static boolean schedule(String name, RunFunction task, long t) throws XPathException {
        if (executors.containsKey(task.id)) return false;
        ExecutorService executor = executors.get(name);
        if (!(executor instanceof ScheduledExecutorService)) return false;
        ScheduledExecutorService scheduler = (ScheduledExecutorService) executor;
        if (scheduler == null) throw new XPathException("Unknown scheduler name: " + name);
        ScheduledFuture future = scheduler.schedule(task, t, TimeUnit.MILLISECONDS);
        futures.put(task.id, future);
        return true;
    }

}
