%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB session management module.
%%% Handles session creation, management and cleanup for transactions.
%%% Supports single node, replica set and sharded cluster deployments.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_session).
-author("mongodb-erlang team").

-behaviour(gen_server).

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  start_session/1,
  start_session/2,
  end_session/1,
  get_session_id/1,
  update_cluster_time/2,
  update_operation_time/2,
  is_session_valid/1,
  get_session_options/1
]).

%% gen_server callbacks
-export([
  init/1,
  handle_call/3,
  handle_cast/2,
  handle_info/2,
  terminate/2,
  code_change/3
]).

-define(SESSION_TIMEOUT, 30 * 60 * 1000). % 30 minutes default
-define(SESSION_REFRESH_INTERVAL, 10 * 60 * 1000). % 10 minutes

-record(session_state, {
  session :: session(),
  worker :: pid(),
  topology_type :: single | replica_set | sharded,
  last_used :: integer(),
  timeout :: integer(),
  timer_ref :: undefined | timer:tref()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start a new session with default options
-spec start_session(pid()) -> {ok, pid()} | {error, term()}.
start_session(Worker) ->
  start_session(Worker, #{}).

%% @doc Start a new session with specified options
-spec start_session(pid(), map()) -> {ok, pid()} | {error, term()}.
start_session(Worker, Options) ->
  case check_transaction_support(Worker) of
    {ok, TopologyType} ->
      gen_server:start_link(?MODULE, [Worker, TopologyType, Options], []);
    Error ->
      Error
  end.

%% @doc End a session and cleanup resources
-spec end_session(pid()) -> ok.
end_session(SessionPid) ->
  gen_server:call(SessionPid, end_session).

%% @doc Get the session ID
-spec get_session_id(pid()) -> {ok, binary()} | {error, term()}.
get_session_id(SessionPid) ->
  gen_server:call(SessionPid, get_session_id).

%% @doc Update cluster time from server response
-spec update_cluster_time(pid(), bson:document()) -> ok.
update_cluster_time(SessionPid, ClusterTime) ->
  gen_server:cast(SessionPid, {update_cluster_time, ClusterTime}).

%% @doc Update operation time from server response
-spec update_operation_time(pid(), bson:timestamp()) -> ok.
update_operation_time(SessionPid, OperationTime) ->
  gen_server:cast(SessionPid, {update_operation_time, OperationTime}).

%% @doc Check if session is still valid
-spec is_session_valid(pid()) -> boolean().
is_session_valid(SessionPid) ->
  try
    gen_server:call(SessionPid, is_valid, 1000)
  catch
    _:_ -> false
  end.

%% @doc Get session options for commands
-spec get_session_options(pid()) -> map().
get_session_options(SessionPid) ->
  gen_server:call(SessionPid, get_session_options).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Worker, TopologyType, Options]) ->
  process_flag(trap_exit, true),
  
  case create_server_session(Worker) of
    {ok, SessionId, ServerSession} ->
      Timeout = maps:get(timeout, Options, ?SESSION_TIMEOUT),
      Session = #session{
        id = SessionId,
        cluster_time = undefined,
        operation_time = undefined,
        implicit = maps:get(implicit, Options, false),
        server_session = ServerSession
      },
      
      State = #session_state{
        session = Session,
        worker = Worker,
        topology_type = TopologyType,
        last_used = erlang:system_time(millisecond),
        timeout = Timeout,
        timer_ref = undefined
      },
      
      {ok, TimerRef} = timer:send_interval(?SESSION_REFRESH_INTERVAL, refresh_session),
      NewState = State#session_state{timer_ref = TimerRef},
      
      {ok, NewState};
    Error ->
      {stop, Error}
  end.

handle_call(get_session_id, _From, State = #session_state{session = Session}) ->
  {reply, {ok, Session#session.id}, update_last_used(State)};

handle_call(end_session, _From, State) ->
  {stop, normal, ok, State};

handle_call(is_valid, _From, State = #session_state{last_used = LastUsed, timeout = Timeout}) ->
  Now = erlang:system_time(millisecond),
  Valid = (Now - LastUsed) < Timeout,
  {reply, Valid, State};

handle_call(get_session_options, _From, State = #session_state{session = Session}) ->
  Options = build_session_options(Session),
  {reply, Options, update_last_used(State)};

handle_call(_Request, _From, State) ->
  {reply, {error, unknown_request}, State}.

handle_cast({update_cluster_time, ClusterTime}, State = #session_state{session = Session}) ->
  UpdatedSession = Session#session{cluster_time = ClusterTime},
  NewState = State#session_state{session = UpdatedSession},
  {noreply, update_last_used(NewState)};

handle_cast({update_operation_time, OperationTime}, State = #session_state{session = Session}) ->
  UpdatedSession = Session#session{operation_time = OperationTime},
  NewState = State#session_state{session = UpdatedSession},
  {noreply, update_last_used(NewState)};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(refresh_session, State = #session_state{last_used = LastUsed, timeout = Timeout}) ->
  Now = erlang:system_time(millisecond),
  case (Now - LastUsed) > Timeout of
    true ->
      {stop, session_timeout, State};
    false ->
      {noreply, State}
  end;

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #session_state{timer_ref = TimerRef, worker = Worker, session = Session}) ->
  case TimerRef of
    undefined -> ok;
    _ -> timer:cancel(TimerRef)
  end,
  
  % End session on server
  try
    end_server_session(Worker, Session#session.id)
  catch
    _:_ -> ok
  end,
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Check if the MongoDB deployment supports transactions
-spec check_transaction_support(pid()) -> {ok, single | replica_set | sharded} | {error, term()}.
check_transaction_support(Worker) ->
  try
    % Get server info to determine topology and version
    Command = {<<"isMaster">>, 1},
    case mc_worker_api:command(Worker, Command) of
      {true, #{<<"ismaster">> := true} = Result} ->
        TopologyType = determine_topology_type(Result),
        case check_version_support(Worker, TopologyType) of
          ok -> {ok, TopologyType};
          Error -> Error
        end;
      {true, #{<<"ismaster">> := false}} ->
        {error, not_master};
      Error ->
        {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Determine topology type from isMaster response
-spec determine_topology_type(map()) -> single | replica_set | sharded.
determine_topology_type(#{<<"msg">> := <<"isdbgrid">>}) ->
  sharded;
determine_topology_type(#{<<"setName">> := _}) ->
  replica_set;
determine_topology_type(_) ->
  single.

%% @doc Check if MongoDB version supports transactions for the topology
-spec check_version_support(pid(), single | replica_set | sharded) -> ok | {error, term()}.
check_version_support(Worker, TopologyType) ->
  try
    Command = {<<"buildInfo">>, 1},
    case mc_worker_api:command(Worker, Command) of
      {true, #{<<"version">> := VersionBin}} ->
        Version = parse_version(VersionBin),
        case is_transaction_supported(Version, TopologyType) of
          true -> ok;
          false -> {error, {transactions_not_supported, Version, TopologyType}}
        end;
      Error ->
        {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Parse MongoDB version string
-spec parse_version(binary()) -> {integer(), integer(), integer()}.
parse_version(VersionBin) ->
  VersionStr = binary_to_list(VersionBin),
  case string:tokens(VersionStr, ".") of
    [MajorStr, MinorStr | _] ->
      Major = list_to_integer(MajorStr),
      Minor = list_to_integer(MinorStr),
      {Major, Minor, 0};
    _ ->
      {0, 0, 0}
  end.

%% @doc Check if transactions are supported for given version and topology
-spec is_transaction_supported({integer(), integer(), integer()}, single | replica_set | sharded) -> boolean().
is_transaction_supported({Major, Minor, _}, sharded) when Major > 4; (Major =:= 4 andalso Minor >= 2) ->
  true;
is_transaction_supported({Major, _, _}, replica_set) when Major >= 4 ->
  true;
is_transaction_supported({Major, Minor, _}, single) when Major > 4; (Major =:= 4 andalso Minor >= 2) ->
  true;
is_transaction_supported(_, _) ->
  false.

%% @doc Create a server session
-spec create_server_session(pid()) -> {ok, binary(), map()} | {error, term()}.
create_server_session(Worker) ->
  try
    Command = {<<"startSession">>, 1},
    case mc_worker_api:command(Worker, Command) of
      {true, #{<<"id">> := SessionId} = Result} ->
        {ok, SessionId, Result};
      Error ->
        {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc End server session
-spec end_server_session(pid(), binary()) -> ok | {error, term()}.
end_server_session(Worker, SessionId) ->
  try
    Command = {<<"endSessions">>, [SessionId]},
    mc_worker_api:command(Worker, Command),
    ok
  catch
    _:_ ->
      ok % Ignore errors when ending session
  end.

%% @doc Build session options for MongoDB commands
-spec build_session_options(session()) -> map().
build_session_options(#session{id = SessionId, cluster_time = ClusterTime, operation_time = OperationTime}) ->
  Options = #{<<"lsid">> => SessionId},
  
  Options1 = case ClusterTime of
    undefined -> Options;
    _ -> Options#{<<"$clusterTime">> => ClusterTime}
  end,
  
  case OperationTime of
    undefined -> Options1;
    _ -> Options1#{<<"operationTime">> => OperationTime}
  end.

%% @doc Update last used timestamp
-spec update_last_used(#session_state{}) -> #session_state{}.
update_last_used(State) ->
  State#session_state{last_used = erlang:system_time(millisecond)}.
