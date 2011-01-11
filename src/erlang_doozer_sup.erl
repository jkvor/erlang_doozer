
-module(erlang_doozer_sup).

-behaviour(supervisor).

%% API
-export([start_link/3]).

%% Supervisor callbacks
-export([init/1]).


%% ===================================================================
%% API functions
%% ===================================================================

start_link(PBMod, Host, Port) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [PBMod, Host, Port]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([PBMod, Host, Port]) ->
  RestartStrategy = one_for_one,
  MaxRestarts = 1000,
  MaxSecondsBetweenRestarts = 3600,

  SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

  Restart = permanent,
  Shutdown = 2000,
  Type = worker,

  TableManager = 
    {doozer_conn, {doozer_conn, start_link, [PBMod, Host, Port]},
     Restart, Shutdown, Type, [doozer_conn]},
  {ok, {SupFlags, [TableManager]}}.

