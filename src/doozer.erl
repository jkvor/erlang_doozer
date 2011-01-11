%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2011, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2011 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(doozer).
-include("doozer.hrl").
%% API
-export([
         %% sync
         checkin/2,
         get/2,
         set/3,
         del/2,
         eset/2,
         snap/0,
         delsnap/1,
         noop/0,
         cancel/1,
         syncpath/1,

         %% async : pass in a pid to get responces..
         getdir/2,
         walk/2,
         watch/2,
         monitor/2         
        ]).

%%%===================================================================
%%% API
%%%===================================================================
checkin(Cas, Id) when is_list(Cas), is_integer(Id) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 0, cas = Cas, id = Id}),
  reply(Tag, fun(Resp) -> Resp#response.cas end, init).

get(Path, Id) when is_list(Path), is_integer(Id) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 1, path = Path, id = Id}),
  reply(Tag, fun(Resp) -> {Resp#response.cas, Resp#response.value} end, init).

set(Cas, Path, Value) when is_list(Cas), is_list(Path), is_binary(Value) ->
  {sent, Tag} = 
    doozer_conn:send(#request{verb = 2, cas = Cas, path = Path, value = Value}),
  reply(Tag, fun(Resp) -> Resp#response.cas end, init).

del(Cas, Path) when is_list(Cas), is_list(Path) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 3, cas = Cas, path = Path}),
  reply(Tag, fun(_Resp) -> void end, init).

eset(Cas, Path) when is_list(Cas), is_list(Path) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 4, cas = Cas, path = Path}),
  reply(Tag, fun(_Resp) -> void end, init).
  
snap() ->
  {sent, Tag} = doozer_conn:send(#request{verb = 5}),
  reply(Tag, fun(Resp) -> {Resp#response.seqn, Resp#response.id} end, init).

delsnap(Id) when is_integer(Id) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 6, id = Id}),
  reply(Tag, fun(_Resp) -> void end, init).

noop() ->
  {sent, Tag} = doozer_conn:send(#request{verb = 7}),
  reply(Tag, fun(_Resp) -> void end, init).

cancel(Id) when is_integer(Id) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 10, id = Id}),
  reply(Tag, fun(_Resp) -> void end, init).

syncpath(Path) when is_list(Path) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 12, path = Path}),
  reply(Tag, fun(Resp) -> {Resp#response.cas, Resp#response.value} end, init).


watch(Pid, Path) when is_list(Path) ->
  async_fun(Pid, watch, #request{verb = 8, path = Path}, 
            fun(Resp) -> 
                {Resp#response.cas, Resp#response.path, Resp#response.value}
            end).    

walk(Pid, Path) when is_list(Path) ->
  async_fun(Pid, walk, #request{verb = 9, path = Path},
            fun(Resp) -> 
                {Resp#response.cas, Resp#response.path, Resp#response.value}
            end).    

monitor(Pid, Path) when is_list(Path) ->
  async_fun(Pid, monitor, #request{verb = 11, path = Path},
            fun(Resp) -> 
                {Resp#response.cas, Resp#response.path, Resp#response.value}
            end).    
  
getdir(Pid, Path) when is_list(Path) ->
  async_fun(Pid, getdir, #request{verb = 14, path = Path},
            fun(Resp) -> 
                {Resp#response.cas, Resp#response.value}
            end).    

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
async_fun(Pid, Op, Req, Fun) ->
  RPid = spawn(fun() -> loop(Op, Pid, Fun, init) end),
  {sent, _Tag} = doozer_conn:send(RPid, Req).
            
loop(Op, RPid, Fun, init) -> 
  MonRef = erlang:monitor(process, doozer_conn),
  loop(Op, RPid, Fun, MonRef);
loop(Op, RPid, Fun, MonRef) -> 
  receive
    {_, Tag, #response{err_code = EC, err_detail = ED}} when EC > 0 ->
      erlang:demonitor(MonRef),
      RPid ! {Op, error, Tag, {EC, ED}};
    {valid, Tag, Resp} -> 
      RPid ! {Op, valid, Tag, Fun(Resp)},
      loop(Op, RPid, Fun, MonRef);
    {done, Tag, Resp}  -> 
      erlang:demonitor(MonRef),
      RPid ! {Op, done, Tag, Fun(Resp)};
    {'DOWN', MonRef, _, _, _} ->
      RPid ! {error, retry};
    X ->
      io:format("Got smthing funny...[~p]~n", [X]),
      RPid ! {error, X}
  end.
  
reply(Tag, Fun, init) ->
  MonRef = erlang:monitor(process, doozer_conn),
  reply(Tag, Fun, MonRef, <<>>).
reply(Tag, Fun, MonRef, RetVal) ->  
  receive
    {_, Tag, #response{err_code = EC, err_detail = ED}} when EC > 0 ->
      erlang:demonitor(MonRef),
      {error, {EC, ED}};
    {valid, Tag, Resp} ->
      reply(Tag, Fun, MonRef, Fun(Resp));    
    {done, Tag, _} ->
      erlang:demonitor(MonRef),
      {ok, RetVal};    
    {'DOWN', MonRef, _, _, _} ->
      {error, retry};
    X ->
      io:format("Got smthing funny...[~p]~n", [X]),
      {error, X}
  end.  
