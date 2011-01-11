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
         %% snap/0,
         %% delsnap/1,
         %% noop/0,
         %% cancel/1,
         %% getdir/1,
         %% walk/1,
         %% syncpath/1,

         %% async : pass in a pid to get responces..
         watch/2,
         monitor/2         
        ]).

%%%===================================================================
%%% API
%%%===================================================================
checkin(Cas, Id) when is_list(Cas), is_integer(Id) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 0, cas = Cas, id = Id}),
  reply(Tag, fun(Resp) -> Resp#response.cas end).

get(Path, Id) when is_list(Path), is_integer(Id) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 1, path = Path, id = Id}),
  reply(Tag, fun(Resp) -> {Resp#response.cas, Resp#response.value} end).

set(Cas, Path, Value) when is_list(Cas), is_list(Path), is_binary(Value) ->
  {sent, Tag} = 
    doozer_conn:send(#request{verb = 2, cas = Cas, path = Path, value = Value}),
  reply(Tag, fun(Resp) -> Resp#response.cas end).

del(Cas, Path) when is_list(Cas), is_list(Path) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 3, cas = Cas, path = Path}),
  reply(Tag, fun(_Resp) -> void end).

eset(Cas, Path) when is_list(Cas), is_list(Path) ->
  {sent, Tag} = doozer_conn:send(#request{verb = 4, cas = Cas, path = Path}),
  reply(Tag, fun(_Resp) -> void end).
  


watch(Pid, Path) when is_list(Path) ->
  {sent, Tag} = doozer_conn:send(Pid, #request{verb = 8, path = Path}),
  spawn(fun() -> 
            loop(Tag, watch, Pid, 
                 fun(Resp) -> 
                     {Resp#response.cas, Resp#response.path, Resp#response.value}
                 end, init)
        end).


monitor(Pid, Path) when is_list(Path) ->
  {sent, Tag} = doozer_conn:send(Pid, #request{verb = 8, path = Path}),
  spawn(fun() -> 
            loop(Tag, watch, Pid, 
                 fun(Resp) -> 
                     {Resp#response.cas, Resp#response.path, Resp#response.value}
                 end, init)
        end).                                          

    
  

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% Internal functions
%%%===================================================================
loop(Tag, Op, RPid, Fun, init) -> 
  MonRef = erlang:monitor(process, doozer_conn),
  loop(Tag, Op, RPid, Fun, MonRef);
loop(Tag, Op, RPid, Fun, MonRef) -> 
  receive
    {_, Tag, #response{err_code = EC, err_detail = ED}} when EC > 0 ->
      erlang:demonitor(MonRef),
      RPid ! {Op, error, Tag, {EC, ED}};
    {valid, Tag, Resp} -> 
      RPid ! {Op, valid, Tag, Fun(Resp)},
      loop(Tag, Op, RPid, Fun, MonRef);
    {done, Tag, Resp}  -> 
      erlang:demonitor(MonRef),
      RPid ! {Op, done, Tag, Fun(Resp)};
    {'DOWN', MonRef, _, _, _} ->
      RPid ! {error, retry}
  end.
  

reply(Tag, Fun) ->
  MonRef = erlang:monitor(process, doozer_conn),
  receive
    {_, Tag, #response{err_code = EC, err_detail = ED}} when EC > 0 ->
      erlang:demonitor(MonRef),
      {error, {EC, ED}};
    {valid, Tag, Resp} ->
      erlang:demonitor(MonRef),
      {ok, Fun(Resp)};
    {'DOWN', MonRef, _, _, _} ->
      {error, retry}
  end.  