Doozer erlang client
--------------------

To build :
$ git clone git://github.com/JacobVorreuter/erlang_doozer.git
$ cd erlang_doozer
$ rebar get-deps
$ rebar compile

To Run :
$ cp msg.proto /tmp/
$ erl -pa ebin/ -pa deps/protobuffs/ebin/
Erlang R14B (erts-5.8.1) [source] [64-bit] [smp:2:2] [rq:2] [async-threads:0] [hipe] [kernel-poll:false]

Eshell V5.8.1  (abort with ^G)
1> 
....
..

NOTE : for some reason the doozer instances wernt giving any response to the client request..
So.. wrote a "dummy doozer" server to test networking code and ensure protobuf marshalling is ok..

Test run:

Erlang R14B (erts-5.8.1) [source] [64-bit] [smp:2:2] [rq:2] [async-threads:0] [hipe] [kernel-poll:false]

Eshell V5.8.1  (abort with ^G)
1> dummy_doozer:start(1, 8041).
8041
2> application:start(erlang_doozer).
ok

NOTE : simple request such as "get" and "set" will wait for server to return "done" flag

3> doozer:set("0", "/foo", <<"bar">>).
[dummy_dozer] Req : {request,1,2,"0","/foo",<<"bar">>,undefined,undefined,undefined}
{ok,"0"}

NOTE : for walk/watch/monitor etc.. user has to provide a callback Pid that is notified of
responses from the server.. these are asynch calls which do not block..

4> doozer:walk(self(), "/foo").
{sent,2}
[dummy_dozer] Req : {request,2,9,undefined,"/foo",undefined,undefined,undefined,undefined}
5> flush().
Shell got {walk,valid,2,{"0","/foo",<<"Walk1">>}}
Shell got {walk,valid,2,{"0","/foo",<<"Walk2">>}}
Shell got {walk,valid,2,{"0","/foo",<<"Walk3">>}}
Shell got {walk,done,2,{"0",undefined,undefined}}
ok

 

