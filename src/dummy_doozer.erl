-module(dummy_doozer).
-include("doozer.hrl").

-export([start/2, server/1]).

start(Num,LPort) ->
  case gen_tcp:listen(LPort,[binary, {packet,0}]) of
    {ok, ListenSock} ->
      start_servers(Num,ListenSock),
      {ok, Port} = inet:port(ListenSock),
      Port;
    {error,Reason} ->
      {error,Reason}
  end.

start_servers(0,_) ->
    ok;
start_servers(Num,LS) ->
    spawn(?MODULE,server,[LS]),
    start_servers(Num-1,LS).

server(LS) ->
  case gen_tcp:accept(LS) of
    {ok,S} ->
      loop(S),
      server(LS);
    Other ->
      io:format("accept returned ~w - goodbye!~n",[Other]),
      ok
  end.

loop(S) ->
  receive
    {tcp,S,Data} ->
      Req = msg_pb:decode_request(Data),
      io:format("[dummy_dozer] Req : ~p~n", [Req]),
      process(S, Req),
      loop(S);
    {tcp_closed,S} ->
      io:format("Socket ~w closed [~w]~n",[S,self()]),
      ok
  end.

process(S, #request{tag = T, verb = 0, cas = Cas, id = _Id}) ->   
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = Cas})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 2, cas = Cas}));

process(S, #request{tag = T, verb = 1, path = _Path, id = _Id}) -> 
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              value = <<"Dummy">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 2, cas = "0"}));

process(S, #request{tag = T, verb = 2, cas = Cas, path = _Path, value = _V}) -> 
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = Cas})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 2, cas = Cas}));
  

process(S, #request{tag = T, verb = 8, path = Path, id = _Id}) -> 
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"Watch1">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"Watch2">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"Watch3">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 2, cas = "0"}));


process(S, #request{tag = T, verb = 9, path = Path, id = _Id}) -> 
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"Walk1">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"Walk2">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"Walk3">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 2, cas = "0"}));
  

process(S, #request{tag = T, verb = 14, path = Path, id = _Id}) -> 
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"GetDir1">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"GetDir2">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 1, cas = "0", 
                              path = Path, value = <<"GetDir3">>})), 
  timer:sleep(100),
  gen_tcp:send(S, msg_pb:encode_response(
                    #response{tag = T, flags = 2, cas = "0"})).
  
