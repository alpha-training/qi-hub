/ Hub

.qi.import`ipc
.qi.import`cron

\d .hub

ws.push:{[h;x] neg[(),h]@\:.j.j`callback`result!x;}
ws.pushall:{if[count h:where"w"=k!exec p from -38!k:key .z.W;ws.push[h;x]]}
.z.ws:{a:.j.k x;r:@[get;a`cmd;{"kdb error: ",x}];if[not"none"~cb:a`callback;ws.push[.z.w;(cb;r)]]}    / cb=callback
pub:{[t;x] ws.pushall("upd";(t;x))}

init:{  
  conns::1!select name,proc,port,status:`down,pid:0Ni,lastheartbeat:0Np,attempts:0,goal:`,lastattempt:0Np,lastattempt:0Np,used:0N,heap:0N from .ipc.conns where proc<>`hub;
  .cron.add[`.hub.check;0Np;.conf.HUB_CHECK_PERIOD];
  .ipc.ping[;".proc.reporthealth[]"]each exec name from .ipc.conns;
  }

getprocess:{[pname] $[null(x:conns pname)`proc;();x]}
getlog:{[name] .qi.spath(.conf.processlogs;` sv name,`log)}

/ process control functions
up:{[x]
  if[any x~/:(`;`all;::;st:.proc.ACTIVE_STACK);.z.s each .proc.stackprocs st;:(::)];
  if[null(e:conns x)`status;'"invalid process name ",string x];
  conns[x],:select attempts:1+0^attempts,lastattempt:.z.p,goal:`up from e;
  .proc.up x;
 }

down:{[x]
 if[any x~/:(`;`all;::;st:.proc.ACTIVE_STACK);.z.s each .proc.stackprocs st;:(::)];
 if[null st:(e:conns x)`status;'"invalid process name ",string x];
 if[st=`down;:()];
 if[0=0^e`attempts;
  conns[x],:`goal`attempts!(`down;1);
  :.proc.down x];
 }

bounce:{down x;up x;}

heartbeat:{[pname;info]
  if[null st:(e:conns pname)`status;:.qi.error"invalid process name",string[pname]," ",.Q.s1 info];
  .hub.conns[pname],:select used,heap,status:`up,pid,lastheartbeat:time,attempts:0N from info;
  }

upall:{up each exec name from .hub.conns;}
downall:{down each exec name from .hub.conns;}

updAPI:{pub[`processes;0!.hub.conns];}

check:{
  update status:`down`up .proc.isup each name from`.hub.conns where status=`up;
  update pid:0Ni,heap:0N,used:0N from `.hub.conns where status=`down;
  update status:`busy from`.hub.conns where status=`up,lastheartbeat<.z.p-.conf.HUB_BUSY_PERIOD;
  if[count tostart:select from .hub.conns where goal=`up,status=`down,attempts<.conf.MAX_START_ATTEMPTS;
    if[count tostart:delete from tostart where not null lastattempt,.conf.HUB_ATTEMPT_PERIOD>.z.p-lastattempt;
      stilldown:exec name from .hub.conns where status=`down;
      tostart:tostart lj 1!select name,waiting_on:stilldown inter/:depends_on from .proc.mystack;
      .hub.up each exec name from tostart where 0=count each waiting_on]];
  updAPI[];
  }