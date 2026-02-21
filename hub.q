/ Hub

.qi.import`ipc
.qi.import`cron

ws.push:{[h;x] neg[(),h]@\:.j.j`callback`result!x;}
ws.pushall:{if[count h:where"w"=k!exec p from -38!k:key .z.W;ws.push[h;x]]}
.z.ws:{a:.j.k x;r:@[get;a`cmd;{"kdb error: ",x}];if[not"none"~cb:a`callback;ws.push[.z.w;(cb;r)]]}    / cb=callback
pub:{[t;x] ws.pushall("upd";(t;x))}

.hub.init:{
  .proc.self:``name`stackname`pkg`port!(::;`hub;`hub;`hub;.conf.HUB_PORT);
  .proc.loadstacks[];
  .proc.ipc.upd select name,proc:pkg,stackname,port from .proc.getstacks[];
  procs::1!select name,proc,stackname,port,status:`down,pid:0Ni,lastheartbeat:0Np,attempts:0N,lastattempt:0Np,lastattempt:0Np,used:0N,heap:0N,goal:` from .ipc.conns where proc<>`hub;
  .cron.add[`check;0Np;.conf.HUB_CHECK_PERIOD];
  .event.delhandler[`.z.pc;`.ipc.pc];
  system"p ",.qi.tostr .conf.HUB_PORT;
  .cron.start[];
  }

getprocess:{[pname] $[null(x:procs pname)`proc;();x]}
getlog:{[name] .qi.spath(.conf.processlogs;` sv name,`log)}
resolvename:{$[x like"*.*";x;` sv x,.conf.DEFAULT_STACK]}

/ process control functions

updown:{[cmd;x]
  if[11<>abs t:type x;'"Require symbol name(s) of process/stack"];
  if[11=t;.z.s each x;:(::)];
  if[x in`all,as:1_key .stacks;{x each` sv'.proc.stackprocs[y],'y}[cmd]each $[x=`all;as;x];:(::)];
  if[null status:(e:procs nm:resolvename x)`status;'"invalid process name ",string nm];
  if[status=cmd;:(::)];
  procs[nm],:select attempts:1+0^attempts,lastattempt:.z.p,goal:cmd from e;
  .proc[cmd]nm;
  }

up:updown`up
down:updown`down

heartbeat:{[pname;info]
  if[null st:(e:procs pname)`status;:.qi.error"invalid process name",string[pname]," ",.Q.s1 info];
  procs[pname],:select used,heap,status:`up,pid,lastheartbeat:time,attempts:0N from info;
  }

upall:{up each exec name from procs;}
downall:{down each exec name from procs;}
isup:{[fullname] .proc.isup . ` vs fullname}

updAPI:{pub[`processes;0!procs];}

check:{
  update status:`down`up isup each name from`procs;
  update pid:0Ni,heap:0N,used:0N from `procs where status=`down;
  update status:`busy from`procs where status=`up,lastheartbeat<.z.p-.conf.HUB_BUSY_PERIOD;
  if[count tostart:select from procs where goal=`up,status=`down,attempts<.conf.MAX_START_ATTEMPTS;
    if[count tostart:delete from tostart where not null lastattempt,.conf.HUB_ATTEMPT_PERIOD>.z.p-lastattempt;
      stilldown:exec name from procs where status=`down;
      tostart:tostart lj 1!select name,waiting_on:stilldown inter/:depends_on from .proc.mystack;
      up each exec name from tostart where 0=count each waiting_on]];
  update attempts:0N from`procs where status=goal;
  updAPI[];
  }