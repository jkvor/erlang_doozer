%%%-------------------------------------------------------------------
%%% @author Arun Suresh <>
%%% @copyright (C) 2011, Arun Suresh
%%% @doc
%%%
%%% @end
%%% Created : 10 Jan 2011 by Arun Suresh <>
%%%-------------------------------------------------------------------
-module(doozer_conn).
-behaviour(gen_server).
-include("doozer.hrl").

%% API
-export([start_link/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3, send/1, send/2]).

-define(SERVER, ?MODULE). 

-record(state, {
                pb_mod,
                socket, 
                tag = 1, 
                outstanding %% outstanding commands
               }).

%%%===================================================================
%%% API
%%%===================================================================
send(Data) when is_record(Data, request) ->
  send(self(), Data).
send(RPid, Data)  ->
  gen_server:cast(?SERVER, {send, RPid, Data}).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(PBMod, Host, Port) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, [PBMod, Host, Port], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([PBMod, Host, Port]) ->
  {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {packet, 0}, {active, once}]),  
  {ok, #state{pb_mod = PBMod, socket = Sock, outstanding = []}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({send, Pid, #request{} = Data}, _From,
            #state{tag = OTag, outstanding = OldOList, 
                   pb_mod = PBMod,
                   socket = Socket} = State) ->
  ok = gen_tcp:send(Socket, PBMod:encode_request(Data#request{tag = OTag})),
  MRef = erlang:monitor(process, Pid),
  {reply, {sent, OTag}, State#state{tag = (OTag + 1) rem 65536, 
                                    outstanding = [{OTag, Pid, MRef}|OldOList]}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
  {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({tcp, Sock, Data}, 
            #state{pb_mod = PBMod, socket = Sock, outstanding = OList} = State) ->
  Resp = PBMod:decode_response(Data),
  #response{tag = Tag, flags = Flag} = Resp,
  NOList = 
    case lists:keyfind(Tag, 1, OList) of
      false -> OList;
      {Tag, RetPid, _MonRef} ->      
        case Flag of
          2 -> 
            %% Done
            RetPid ! {done, Tag, Resp},
            lists:keydelete(Tag, 1, OList);
          1 ->
            %% Valid
            RetPid ! {valid, Tag, Resp},
            OList
        end
    end,
  inet:setopts(Sock, [{active,once}]),
  {noreply, State#state{outstanding = NOList}};

handle_info({'DOWN', MonRef, _, _, _}, #state{outstanding = OList} = State) ->
  {noreply, State#state{outstanding = lists:keydelete(MonRef, 3, OList)}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
  ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
