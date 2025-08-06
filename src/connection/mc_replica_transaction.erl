%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB replica set transaction support module.
%%% Handles primary node selection, failover, and read preference management.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_replica_transaction).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  ensure_primary_connection/1,
  handle_primary_failover/2,
  validate_read_preference/2,
  get_replica_set_status/1,
  select_read_node/3,
  check_replica_set_health/1,
  get_primary_node/1,
  wait_for_primary_election/2,
  validate_write_concern/2,
  calculate_majority_write_concern/1
]).

-define(PRIMARY_ELECTION_TIMEOUT, 30000). % 30 seconds
-define(HEALTH_CHECK_INTERVAL, 5000). % 5 seconds
-define(MAX_FAILOVER_RETRIES, 3).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Ensure connection is to primary node for transactions
-spec ensure_primary_connection(pid()) -> ok | {error, term()}.
ensure_primary_connection(Worker) ->
  case check_is_primary(Worker) of
    true -> ok;
    false -> find_and_connect_primary(Worker);
    {error, Reason} -> {error, Reason}
  end.

%% @doc Handle primary node failover during transaction
-spec handle_primary_failover(pid(), pid()) -> ok | {error, term()}.
handle_primary_failover(SessionPid, TransactionPid) ->
  try
    % Abort current transaction
    mc_transaction:abort_transaction(TransactionPid),

    % Wait for new primary election
    case wait_for_primary_election(SessionPid, ?PRIMARY_ELECTION_TIMEOUT) of
      {ok, NewPrimary} ->
        % Update connection to new primary
        update_connection_to_primary(SessionPid, NewPrimary);
      {error, FailoverReason} ->
        {error, {failover_failed, FailoverReason}}
    end
  catch
    _:CatchReason ->
      {error, {failover_error, CatchReason}}
  end.

%% @doc Validate read preference for transaction context
-spec validate_read_preference(map(), atom()) -> ok | {error, term()}.
validate_read_preference(ReadPreference, TransactionState) ->
  Mode = maps:get(<<"mode">>, ReadPreference, <<"primary">>),
  case {Mode, TransactionState} of
    {<<"primary">>, _} -> ok;
    {_, in_transaction} -> 
      % Transactions must read from primary
      {error, {invalid_read_preference, transactions_require_primary}};
    _ -> ok
  end.

%% @doc Get replica set status and member information
-spec get_replica_set_status(pid()) -> {ok, map()} | {error, term()}.
get_replica_set_status(Worker) ->
  try
    Command = {<<"replSetGetStatus">>, 1},
    case mc_worker_api:command(Worker, Command) of
      {true, Status} -> {ok, Status};
      {false, Error} -> {error, Error};
      Error -> {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Select appropriate node for read operations
-spec select_read_node(pid(), map(), atom()) -> {ok, pid()} | {error, term()}.
select_read_node(Worker, ReadPreference, TransactionState) ->
  case validate_read_preference(ReadPreference, TransactionState) of
    ok ->
      Mode = maps:get(<<"mode">>, ReadPreference, <<"primary">>),
      Tags = maps:get(<<"tags">>, ReadPreference, []),
      select_node_by_preference(Worker, Mode, Tags);
    Error ->
      Error
  end.

%% @doc Check overall replica set health
-spec check_replica_set_health(pid()) -> {ok, map()} | {error, term()}.
check_replica_set_health(Worker) ->
  try
    case get_replica_set_status(Worker) of
      {ok, Status} ->
        Members = maps:get(<<"members">>, Status, []),
        HealthInfo = analyze_member_health(Members),
        {ok, HealthInfo};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Get current primary node information
-spec get_primary_node(pid()) -> {ok, map()} | {error, term()}.
get_primary_node(Worker) ->
  try
    case get_replica_set_status(Worker) of
      {ok, Status} ->
        Members = maps:get(<<"members">>, Status, []),
        case find_primary_member(Members) of
          {ok, Primary} -> {ok, Primary};
          not_found -> {error, no_primary_found}
        end;
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Wait for primary election to complete
-spec wait_for_primary_election(pid(), integer()) -> {ok, map()} | {error, term()}.
wait_for_primary_election(Worker, Timeout) ->
  StartTime = erlang:system_time(millisecond),
  wait_for_primary_loop(Worker, StartTime, Timeout).

%% @doc Validate write concern for replica set
-spec validate_write_concern(map(), pid()) -> ok | {error, term()}.
validate_write_concern(WriteConcern, Worker) ->
  try
    W = maps:get(<<"w">>, WriteConcern, 1),
    case validate_w_value(W, Worker) of
      ok ->
        % Validate other write concern options
        validate_write_concern_options(WriteConcern);
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Calculate majority write concern for replica set
-spec calculate_majority_write_concern(pid()) -> {ok, map()} | {error, term()}.
calculate_majority_write_concern(Worker) ->
  try
    case get_replica_set_status(Worker) of
      {ok, Status} ->
        Members = maps:get(<<"members">>, Status, []),
        VotingMembers = count_voting_members(Members),
        Majority = (VotingMembers div 2) + 1,
        WriteConcern = #{
          <<"w">> => Majority,
          <<"j">> => true,
          <<"wtimeout">> => 10000
        },
        {ok, WriteConcern};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Check if current connection is to primary
-spec check_is_primary(pid()) -> boolean() | {error, term()}.
check_is_primary(Worker) ->
  try
    Command = {<<"isMaster">>, 1},
    case mc_worker_api:command(Worker, Command) of
      {true, #{<<"ismaster">> := true}} -> true;
      {true, #{<<"ismaster">> := false}} -> false;
      {false, Error} -> {error, Error};
      Error -> {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Find and connect to primary node
-spec find_and_connect_primary(pid()) -> ok | {error, term()}.
find_and_connect_primary(Worker) ->
  try
    case get_primary_node(Worker) of
      {ok, #{<<"name">> := _PrimaryHost}} ->
        % In a real implementation, would establish new connection to primary
        % For now, return ok if primary is found
        ok;
      {error, no_primary_found} ->
        {error, no_primary_available};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Update connection to new primary after failover
-spec update_connection_to_primary(pid(), map()) -> ok | {error, term()}.
update_connection_to_primary(_SessionPid, _NewPrimary) ->
  % Simplified implementation - in real scenario would update connection
  ok.

%% @doc Select node based on read preference
-spec select_node_by_preference(pid(), binary(), list()) -> {ok, pid()} | {error, term()}.
select_node_by_preference(Worker, <<"primary">>, _Tags) ->
  case check_is_primary(Worker) of
    true -> {ok, Worker};
    false -> find_and_connect_primary(Worker)
  end;
select_node_by_preference(Worker, <<"secondary">>, Tags) ->
  select_secondary_node(Worker, Tags);
select_node_by_preference(Worker, <<"primaryPreferred">>, Tags) ->
  case check_is_primary(Worker) of
    true -> {ok, Worker};
    false -> select_secondary_node(Worker, Tags)
  end;
select_node_by_preference(Worker, <<"secondaryPreferred">>, Tags) ->
  case select_secondary_node(Worker, Tags) of
    {ok, Secondary} -> {ok, Secondary};
    {error, _} -> 
      case check_is_primary(Worker) of
        true -> {ok, Worker};
        false -> {error, no_suitable_node}
      end
  end;
select_node_by_preference(Worker, <<"nearest">>, Tags) ->
  select_nearest_node(Worker, Tags).

%% @doc Select secondary node based on tags
-spec select_secondary_node(pid(), list()) -> {ok, pid()} | {error, term()}.
select_secondary_node(Worker, _Tags) ->
  try
    case get_replica_set_status(Worker) of
      {ok, Status} ->
        Members = maps:get(<<"members">>, Status, []),
        case find_suitable_secondary(Members) of
          {ok, _Secondary} -> {ok, Worker}; % Simplified - would return actual secondary connection
          not_found -> {error, no_secondary_available}
        end;
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Select nearest node based on network latency
-spec select_nearest_node(pid(), list()) -> {ok, pid()} | {error, term()}.
select_nearest_node(Worker, _Tags) ->
  % Simplified implementation - would measure latency to all nodes
  {ok, Worker}.

%% @doc Analyze health of replica set members
-spec analyze_member_health(list()) -> map().
analyze_member_health(Members) ->
  Total = length(Members),
  Healthy = length([M || M <- Members, is_member_healthy(M)]),
  Primary = case find_primary_member(Members) of
    {ok, _} -> 1;
    not_found -> 0
  end,
  
  #{
    total_members => Total,
    healthy_members => Healthy,
    has_primary => Primary =:= 1,
    health_percentage => (Healthy / Total) * 100
  }.

%% @doc Check if a member is healthy
-spec is_member_healthy(map()) -> boolean().
is_member_healthy(#{<<"health">> := Health, <<"state">> := State}) ->
  Health =:= 1 andalso (State =:= 1 orelse State =:= 2). % PRIMARY or SECONDARY

%% @doc Find primary member in replica set
-spec find_primary_member(list()) -> {ok, map()} | not_found.
find_primary_member([]) ->
  not_found;
find_primary_member([#{<<"state">> := 1} = Member | _]) ->
  {ok, Member}; % State 1 = PRIMARY
find_primary_member([_ | Rest]) ->
  find_primary_member(Rest).

%% @doc Find suitable secondary member
-spec find_suitable_secondary(list()) -> {ok, map()} | not_found.
find_suitable_secondary([]) ->
  not_found;
find_suitable_secondary([#{<<"state">> := 2, <<"health">> := 1} = Member | _]) ->
  {ok, Member}; % State 2 = SECONDARY, Health 1 = UP
find_suitable_secondary([_ | Rest]) ->
  find_suitable_secondary(Rest).

%% @doc Wait for primary election in a loop
-spec wait_for_primary_loop(pid(), integer(), integer()) -> {ok, map()} | {error, term()}.
wait_for_primary_loop(Worker, StartTime, Timeout) ->
  CurrentTime = erlang:system_time(millisecond),
  if
    CurrentTime - StartTime > Timeout ->
      {error, primary_election_timeout};
    true ->
      case get_primary_node(Worker) of
        {ok, Primary} -> {ok, Primary};
        {error, no_primary_found} ->
          timer:sleep(?HEALTH_CHECK_INTERVAL),
          wait_for_primary_loop(Worker, StartTime, Timeout);
        Error ->
          Error
      end
  end.

%% @doc Validate write concern W value
-spec validate_w_value(term(), pid()) -> ok | {error, term()}.
validate_w_value(W, Worker) when is_integer(W) ->
  case get_replica_set_status(Worker) of
    {ok, Status} ->
      Members = maps:get(<<"members">>, Status, []),
      VotingMembers = count_voting_members(Members),
      if
        W > VotingMembers ->
          {error, {write_concern_too_high, W, VotingMembers}};
        W < 0 ->
          {error, {invalid_write_concern, W}};
        true ->
          ok
      end;
    Error ->
      Error
  end;
validate_w_value(<<"majority">>, _Worker) ->
  ok;
validate_w_value(W, _Worker) when is_binary(W) ->
  % Tag-based write concern - would validate against replica set config
  ok;
validate_w_value(W, _Worker) ->
  {error, {invalid_write_concern_type, W}}.

%% @doc Validate other write concern options
-spec validate_write_concern_options(map()) -> ok | {error, term()}.
validate_write_concern_options(WriteConcern) ->
  % Validate journal option
  case maps:get(<<"j">>, WriteConcern, undefined) of
    undefined -> ok;
    true -> ok;
    false -> ok;
    J -> {error, {invalid_journal_option, J}}
  end,
  
  % Validate wtimeout
  case maps:get(<<"wtimeout">>, WriteConcern, undefined) of
    undefined -> ok;
    Timeout when is_integer(Timeout), Timeout >= 0 -> ok;
    Timeout -> {error, {invalid_wtimeout, Timeout}}
  end.

%% @doc Count voting members in replica set
-spec count_voting_members(list()) -> integer().
count_voting_members(Members) ->
  length([M || M <- Members, is_voting_member(M)]).

%% @doc Check if member has voting rights
-spec is_voting_member(map()) -> boolean().
is_voting_member(Member) ->
  Votes = maps:get(<<"votes">>, Member, 1),
  Votes > 0.
