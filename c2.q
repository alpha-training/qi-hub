/ Command & Control

.qi.use`log
.qi.use`ipc

.ipc.init[];

\d .c2

TAIL_ROWS:10

procs:update handle:0Ni,pid:0Ni,status:`down,lastheartbeat:0Np,used:0N,heap:0N,log:` from .ipc.conns

notfound:{[pname] string[pname]," process not found"}
entry:{[pname] $[null(e:procs pname)`kind;();e]}

heartbeat:{[pname;info]
  if[()~entry pname;:.log.warn".c2.heartbeat - unrecognized process name: ",string pname];
  procs[pname],:(`handle`pid`lastheartbeat`status!(.z.w;.z.i;.qi.now`;`up)),select used,heap from info;
 }

up:{[pname]

 }

down:{[pname]
  if[()~e:entry pname;:notfound pname];sname:string pname;
  if[null h:e`handle;:".c2.down ",sname," handle is null"];
  if[not first r:.qi.try[neg h;(`.c3.down;`host`port`.z.x!(.z.h;system"p";" "sv .z.x));::];
    .log.error".c2.down ",spname," ",r 2];
 }

killx:{[pname;n]
  if[()~e:entry pname;:notfound pname];
  if[null e`pid;".c2.killx ",string[pname]," pid is null"];
  system"kill -",string[n]," ",string e`pid
 }

interrupt:killx[;2]
kill:killx[;9]

tailx:{[pname;n]
  if[()~e:entry pname;:notfound pname];
  $[.qi.isfile lf:e`log;system"tail -n ",string[n]," ",lf;"Log file not found ",lf]
 }

tail:{[pname] tailx[pname;TAIL_ROWS]}

c3.init:{[pname;hostport]
  if[`c3 in key`;:()];
  .qi.use`c3;
  .c3.init[pname;hostport];
  }