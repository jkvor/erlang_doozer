-module(erlang_doozer_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
  ProtoFile = get_env(proto_file, "/tmp/msg.proto"),
  DHost = get_env(doozer_host, "127.0.0.1"),
  DPort = get_env(doozer_port, 8041),
  ok = protobuffs_compile:scan_file(ProtoFile),
  ProtoMod = 
    list_to_atom(
      filename:rootname(filename:basename(ProtoFile)) ++ "_pb"),
  application:set_env(erlang_doozer, pb_mod, ProtoMod),
  erlang_doozer_sup:start_link(DHost, DPort, ProtoMod).

stop(_State) ->
    ok.

get_env(Key, Def) ->
  case application:get_env(proto_file, Key) of
    undefined -> Def;
    {ok, Val} -> Val
  end.
       
