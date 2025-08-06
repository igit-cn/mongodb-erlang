%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB causal consistency support module.
%%% Implements session-level causal consistency guarantees.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_causal_consistency).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  enable_causal_consistency/1,
  disable_causal_consistency/1,
  update_cluster_time/2,
  update_operation_time/2,
  get_cluster_time/1,
  get_operation_time/1,
  advance_cluster_time/2,
  advance_operation_time/2,
  merge_cluster_times/1,
  validate_causal_consistency/2,
  create_causal_session_options/1,
  is_causally_consistent/1
]).

-define(CLUSTER_TIME_KEY, <<"$clusterTime">>).
-define(OPERATION_TIME_KEY, <<"operationTime">>).

-record(causal_state, {
  enabled :: boolean(),
  cluster_time :: undefined | bson:document(),
  operation_time :: undefined | bson:timestamp(),
  last_update :: integer()
}).

-type causal_state() :: #causal_state{}.

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Enable causal consistency for a session
-spec enable_causal_consistency(pid()) -> ok | {error, term()}.
enable_causal_consistency(SessionPid) ->
  try
    gen_server:call(SessionPid, {set_causal_consistency, true})
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Disable causal consistency for a session
-spec disable_causal_consistency(pid()) -> ok | {error, term()}.
disable_causal_consistency(SessionPid) ->
  try
    gen_server:call(SessionPid, {set_causal_consistency, false})
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Update cluster time from server response
-spec update_cluster_time(pid(), bson:document()) -> ok | {error, term()}.
update_cluster_time(SessionPid, ClusterTime) ->
  try
    gen_server:cast(SessionPid, {update_cluster_time, ClusterTime})
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Update operation time from server response
-spec update_operation_time(pid(), bson:timestamp()) -> ok | {error, term()}.
update_operation_time(SessionPid, OperationTime) ->
  try
    gen_server:cast(SessionPid, {update_operation_time, OperationTime})
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Get current cluster time
-spec get_cluster_time(pid()) -> {ok, bson:document()} | {error, term()}.
get_cluster_time(SessionPid) ->
  try
    gen_server:call(SessionPid, get_cluster_time)
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Get current operation time
-spec get_operation_time(pid()) -> {ok, bson:timestamp()} | {error, term()}.
get_operation_time(SessionPid) ->
  try
    gen_server:call(SessionPid, get_operation_time)
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Advance cluster time if newer
-spec advance_cluster_time(pid(), bson:document()) -> ok | {error, term()}.
advance_cluster_time(SessionPid, NewClusterTime) ->
  try
    case get_cluster_time(SessionPid) of
      {ok, CurrentClusterTime} ->
        case is_cluster_time_newer(NewClusterTime, CurrentClusterTime) of
          true -> update_cluster_time(SessionPid, NewClusterTime);
          false -> ok
        end;
      {error, undefined} ->
        update_cluster_time(SessionPid, NewClusterTime);
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Advance operation time if newer
-spec advance_operation_time(pid(), bson:timestamp()) -> ok | {error, term()}.
advance_operation_time(SessionPid, NewOperationTime) ->
  try
    case get_operation_time(SessionPid) of
      {ok, CurrentOperationTime} ->
        case is_operation_time_newer(NewOperationTime, CurrentOperationTime) of
          true -> update_operation_time(SessionPid, NewOperationTime);
          false -> ok
        end;
      {error, undefined} ->
        update_operation_time(SessionPid, NewOperationTime);
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Merge cluster times from multiple sources
-spec merge_cluster_times(list()) -> bson:document() | undefined.
merge_cluster_times([]) ->
  undefined;
merge_cluster_times([ClusterTime]) ->
  ClusterTime;
merge_cluster_times([CT1, CT2 | Rest]) ->
  Merged = case is_cluster_time_newer(CT1, CT2) of
    true -> CT1;
    false -> CT2
  end,
  merge_cluster_times([Merged | Rest]).

%% @doc Validate causal consistency requirements
-spec validate_causal_consistency(pid(), map()) -> ok | {error, term()}.
validate_causal_consistency(SessionPid, ReadConcern) ->
  try
    case is_causally_consistent(SessionPid) of
      true ->
        validate_read_concern_for_causal_consistency(ReadConcern);
      false ->
        ok
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Create session options with causal consistency
-spec create_causal_session_options(pid()) -> map().
create_causal_session_options(SessionPid) ->
  BaseOptions = #{},
  
  case is_causally_consistent(SessionPid) of
    true ->
      Options1 = case get_cluster_time(SessionPid) of
        {ok, ClusterTime} -> 
          BaseOptions#{?CLUSTER_TIME_KEY => ClusterTime};
        _ -> 
          BaseOptions
      end,
      
      case get_operation_time(SessionPid) of
        {ok, OperationTime} ->
          Options1#{?OPERATION_TIME_KEY => OperationTime};
        _ ->
          Options1
      end;
    false ->
      BaseOptions
  end.

%% @doc Check if session has causal consistency enabled
-spec is_causally_consistent(pid()) -> boolean().
is_causally_consistent(SessionPid) ->
  try
    case gen_server:call(SessionPid, is_causal_consistency_enabled) of
      {ok, Enabled} -> Enabled;
      _ -> false
    end
  catch
    _:_ -> false
  end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Initialize causal state
-spec init_causal_state() -> causal_state().
init_causal_state() ->
  #causal_state{
    enabled = false,
    cluster_time = undefined,
    operation_time = undefined,
    last_update = erlang:system_time(millisecond)
  }.

%% @doc Update causal state with new cluster time
-spec update_causal_cluster_time(causal_state(), bson:document()) -> causal_state().
update_causal_cluster_time(State, ClusterTime) ->
  State#causal_state{
    cluster_time = ClusterTime,
    last_update = erlang:system_time(millisecond)
  }.

%% @doc Update causal state with new operation time
-spec update_causal_operation_time(causal_state(), bson:timestamp()) -> causal_state().
update_causal_operation_time(State, OperationTime) ->
  State#causal_state{
    operation_time = OperationTime,
    last_update = erlang:system_time(millisecond)
  }.

%% @doc Check if cluster time is newer
-spec is_cluster_time_newer(bson:document(), bson:document()) -> boolean().
is_cluster_time_newer(NewTime, CurrentTime) ->
  try
    NewTimestamp = maps:get(<<"clusterTime">>, NewTime),
    CurrentTimestamp = maps:get(<<"clusterTime">>, CurrentTime),
    compare_timestamps(NewTimestamp, CurrentTimestamp) > 0
  catch
    _:_ -> false
  end.

%% @doc Check if operation time is newer
-spec is_operation_time_newer(bson:timestamp(), bson:timestamp()) -> boolean().
is_operation_time_newer(NewTime, CurrentTime) ->
  compare_timestamps(NewTime, CurrentTime) > 0.

%% @doc Compare BSON timestamps
-spec compare_timestamps(bson:timestamp(), bson:timestamp()) -> integer().
compare_timestamps({NewT, NewI}, {CurrentT, CurrentI}) ->
  if
    NewT > CurrentT -> 1;
    NewT < CurrentT -> -1;
    NewI > CurrentI -> 1;
    NewI < CurrentI -> -1;
    true -> 0
  end.

%% @doc Validate read concern for causal consistency
-spec validate_read_concern_for_causal_consistency(map()) -> ok | {error, term()}.
validate_read_concern_for_causal_consistency(ReadConcern) ->
  Level = maps:get(<<"level">>, ReadConcern, <<"local">>),
  case Level of
    <<"local">> -> ok;
    <<"available">> -> ok;
    <<"majority">> -> ok;
    <<"linearizable">> -> 
      {error, {incompatible_read_concern, linearizable_not_supported_with_causal_consistency}};
    <<"snapshot">> -> ok;
    _ -> 
      {error, {unknown_read_concern_level, Level}}
  end.

%% @doc Extract cluster time from server response
-spec extract_cluster_time(map()) -> bson:document() | undefined.
extract_cluster_time(Response) ->
  maps:get(?CLUSTER_TIME_KEY, Response, undefined).

%% @doc Extract operation time from server response
-spec extract_operation_time(map()) -> bson:timestamp() | undefined.
extract_operation_time(Response) ->
  maps:get(?OPERATION_TIME_KEY, Response, undefined).

%% @doc Process server response for causal consistency
-spec process_server_response(pid(), map()) -> ok.
process_server_response(SessionPid, Response) ->
  case extract_cluster_time(Response) of
    undefined -> ok;
    ClusterTime -> advance_cluster_time(SessionPid, ClusterTime)
  end,
  
  case extract_operation_time(Response) of
    undefined -> ok;
    OperationTime -> advance_operation_time(SessionPid, OperationTime)
  end.

%% @doc Create read concern with after cluster time
-spec create_after_cluster_time_read_concern(bson:document(), map()) -> map().
create_after_cluster_time_read_concern(ClusterTime, BaseReadConcern) ->
  AfterClusterTime = maps:get(<<"clusterTime">>, ClusterTime),
  BaseReadConcern#{<<"afterClusterTime">> => AfterClusterTime}.

%% @doc Validate session for causal operations
-spec validate_session_for_causal_ops(pid()) -> ok | {error, term()}.
validate_session_for_causal_ops(SessionPid) ->
  case is_causally_consistent(SessionPid) of
    true -> ok;
    false -> {error, causal_consistency_not_enabled}
  end.

%% @doc Get maximum cluster time from multiple sessions
-spec get_max_cluster_time(list()) -> bson:document() | undefined.
get_max_cluster_time(SessionPids) ->
  ClusterTimes = [begin
    case get_cluster_time(Pid) of
      {ok, CT} -> CT;
      _ -> undefined
    end
  end || Pid <- SessionPids],
  
  ValidTimes = [CT || CT <- ClusterTimes, CT =/= undefined],
  merge_cluster_times(ValidTimes).

%% @doc Synchronize cluster time across sessions
-spec sync_cluster_time(list()) -> ok.
sync_cluster_time(SessionPids) ->
  case get_max_cluster_time(SessionPids) of
    undefined -> ok;
    MaxClusterTime ->
      [advance_cluster_time(Pid, MaxClusterTime) || Pid <- SessionPids],
      ok
  end.
