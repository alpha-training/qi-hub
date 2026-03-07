/ Hub

.qi.import`ipc
.qi.import`cron

ws.push:{[h;x] neg[(),h]@\:.j.j`callback`result!x;}
ws.pushall:{if[count h:where"w"=k!exec p from -38!k:key .z.W;ws.push[h;x]]}
.z.ws:{a:.j.k x;r:@[get;a`cmd;{"kdb error: ",x}];if[not"none"~cb:a`callback;ws.push[.z.w;(cb;r)]]}    / cb=callback
pub:{[t;x] ws.pushall("upd";(t;x))}

findstack:{[st] $[not null p:.proc.stackpaths st;p;null p:first .qi.paths[.conf.STACKS;.qi.ext[st;".json"]]; '"Could not find a ",.qi.tostr[st],".json in ",.qi.spath .conf.STACKS;p]}
cpmvstack:{[copy;st;nst]
  $[.qi.exists dest:.qi.path(first` vs a:findstack st;` sv nst,`json);
    '.qi.spath[dest]," already exists";
  $[copy;.qi.cp;.qi.os.mv][a;dest]]
  }

/ ---- Start Public API Functions ----
writestack:{[st;x]
  .qi.info(`writestack;st);
  if[not first r:.qi.try[.j.k;raze x;0];
    '"stack json is badly formed: ",r 2];
  if[null p:.proc.stackpaths st;
    p:.qi.path(.conf.STACKS;`ui;.qi.ext[st;".json"])];
  p 0:x;
  refresh[];
  p
  }

readstack:{[st] 
  .qi.info(`readstack;st);
  read0 findstack st
  }

deletestack:{[st]
  .qi.info(`deletestack;st);
  if[count a:select from procs where stackname=st,status<>`down;
    show a;
    '"Cannot delete a stack with running processes"];
  hdel findstack st;
  refresh[]
  }

clonestack:{[st;nst] 
 .qi.info(`clonestack;st;nst);
  cpmvstack[1;st;nst];refresh[];
  }

renamestack:{[st;nst]
  .qi.info(`renamestack;st;nst);
  if[count a:select from procs where stackname=st,status=`up;
    show a;
    '"Cannot rename a stack with running processes"];
  r:cpmvstack[0;st;nst];
  refresh[];
  r
  }
/ ------ End Public API functions

.hub.init:{
  .proc.self,:`name`stackname`fullname!3#`hub;
  updprocs[];
  .cron.add[`check;0Np;.conf.HUB_CHECK_PERIOD];
  .event.delhandler[`.z.pc;`.ipc.pc];
  system"p ",.qi.tostr .conf.HUB_PORT;
  .proc.reporthealth[];
  monprocs[];
  .cron.start[];
  .ipc.ping[exec name from procs where name<>`hub,.proc.isup'[name;stackname];".proc.reporthealth[]"] ;
  }

refresh:{
  .qi.info"refresh[]";
  .proc.loadstacks[];
  updprocs[];
  }

updprocs:{
   pr:1!select name,proc,stackname,port,status:`down,pid:0Ni,lastheartbeat:0Np,attempts:0N,lastattempt:0Np,lastattempt:0Np,used:0N,heap:0N,goal:`,logfile:.proc.getlog each name from .ipc.conns where proc<>`hub;
  `procs set $[`procs in tables`.;pr upsert select from procs where status<>`down;pr];
  logmap::1!select sym:logfile,name from procs;
  }

/ monitor processes
monprocs:{
  .qi.import`mon;
  .mon.follow each exec logfile from procs;
  if[null .cron.jobs[f:`.mon.monitor]`period;.cron.add[f;0Np;.conf.MON_PERIOD]];
  }

getprocess:{[pname] $[null(x:procs pname)`proc;();x]}

/ process control functions
updown:{[cmd;x]
  .qi.info -3!(cmd;x);
  if[11<>abs t:type x;'"Require symbol name(s) of process/stack"];
  if[11=t;.z.s each x;:(::)];
  if[x in`all,as:1_key .proc.stacks;.z.s[cmd]each $[x=`all;as;.proc.stackprocs x];:(::)];
  if[null status:(e:procs nm:.proc.tofullname x)`status;'"invalid process name ",string nm];
  if[status=cmd;:(::)];
  procs[nm],:select attempts:1+0^attempts,lastattempt:.z.p,goal:cmd from e;
  .proc[cmd]nm;
  }

up:updown`up
down:updown`down

heartbeat:{[pname;info]
  .qi.info(`heartbeat;pname;info);
  if[null st:(e:procs pname)`status;:.qi.error"invalid process name ",string[pname]," ",.Q.s1 info];
  procs[pname],:select used,heap,status:`up,pid,lastheartbeat:time,attempts:0N from info;
  }

upall:{up each exec name from procs;}
downall:{down each exec name from procs;}
isup:{[fullname] .proc.isup . ` vs fullname}

updAPI:{
  if[sub:count .z.W;pub[`processes;0!procs]];
  if[not count MonText;:()];
  if[sub;pub[`Logs;MonText lj logmap]];
  delete from`MonText;
  }

check:{
  update status:`down`up isup each name from`procs;
  update pid:0Ni,heap:0N,used:0N from `procs where status=`down;
  update status:`busy from`procs where status=`up,lastheartbeat<.z.p-.conf.HUB_BUSY_PERIOD;
  if[count tostart:select from procs where goal=`up,status=`down,attempts<.conf.MAX_START_ATTEMPTS;
    if[count tostart:delete from tostart where not null lastattempt,.conf.HUB_ATTEMPT_PERIOD>.z.p-lastattempt;
      stilldown:exec name from procs where status=`down;
      tostart:tostart lj 1!select name,waiting_on:stilldown inter/:publish_to from .proc.mystack;
      up each exec name from tostart where 0=count each waiting_on]];
  update attempts:0N from`procs where status=goal;
  updAPI[];
  }