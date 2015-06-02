eXecutor
========

Module namespace is 'http://exist-db.org/executor'.

The executor create by one of function:
```
executor:create-single-thread-scheduled-executor("uuid1")
executor:create-single-thread-executor("uuid2")
executor:create-fixed-thread-pool("uuid3", 1)
executor:create-scheduled-thread-pool("uuid4", 10)
```

The task scheduled by function call
```
executor:schedule($task-id, $queue-id, $param, $time, $xq-uri)
```
where 
 - `$task-id` id to manipulate/request infromation for task;
 - `$queue-id` executor's id;
 - `$param` sequence passed as context to xquery script (warning: values must be context independent because of eXist limitations);
 - `$time` when to run (in milliseconds);
 - `$xq-uri` script's AnyURI or script itself as string.

The task placed to executor pool by `executor:submit($task-id, $queue-id, $param, $xq-uri)`.

Functions `executor:is-done($task-id)`, `executor:is-canceled($task-id)` and `executor:get-delay($task-id)` can be used to query task status.

Task can be cancel by `cancel($task-id, $mayInterrupt)` function call.
