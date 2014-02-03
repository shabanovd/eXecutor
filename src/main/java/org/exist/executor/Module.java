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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.exist.xquery.AbstractInternalModule;
import org.exist.xquery.FunctionDef;

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
        new FunctionDef(Submit.signatures[0], Submit.class)
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
    
    protected final static ExecutorService service = Executors.newCachedThreadPool();
    
    protected final static Map<String, Future> futures = new HashMap<String, Future>();
    
    protected static <T> String submit(String uuid, Callable<T> task) {
        Future<T> future = service.submit(task);
        futures.put(uuid, future);
        return uuid;
    }
}
