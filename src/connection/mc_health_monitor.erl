%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB health monitoring module.
%%% Provides comprehensive health checking for mongos, shards, and replica sets.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_health_monitor).
-author("mongodb-erlang team").

-behaviour(gen_server).

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").
-include("mongo_health.hrl").

%% API
-export([
  start_link/0,
  start_link/1,
  stop/0,
  check_mongos_health/1,
  check_mongos_health/2,
  measure_mongos_latency/2,
  check_mongos_load/1,
  check_shard_health/1,
  check_replica_set_health/1,
  get_health_status/1,
  get_all_health_status/0,
  register_for_monitoring/2,
  unregister_from_monitoring/1,
  set_health_thresholds/1
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

-define(SERVER, ?MODULE).
-define(HEALTH_CHECK_INTERVAL, 30000). % 30 seconds
-define(LATENCY_TIMEOUT, 5000). % 5 seconds
-define(CONNECTION_TIMEOUT, 10000). % 10 seconds
-define(HEALTH_CACHE_TTL, 60000). % 1 minute

-record(state, {
  monitored_hosts :: map(),
  health_cache :: ets:tid(),
  check_timer :: timer:tref(),
  thresholds :: map()
}).



%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the health monitor
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
  start_link([]).

%% @doc Start the health monitor with options
-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, Options, []).

%% @doc Stop the health monitor
-spec stop() -> ok.
stop() ->
  gen_server:stop(?SERVER).

%% @doc Check health of a specific mongos with default timeout
-spec check_mongos_health(binary()) -> {ok, #health_status{}} | {error, term()}.
check_mongos_health(MongosHost) ->
  check_mongos_health(MongosHost, ?CONNECTION_TIMEOUT).

%% @doc Check health of a specific mongos with custom timeout
-spec check_mongos_health(binary(), integer()) -> {ok, #health_status{}} | {error, term()}.
check_mongos_health(MongosHost, Timeout) ->
  try
    case get_cached_health_status(MongosHost) of
      {ok, HealthStatus} ->
        {ok, HealthStatus};
      cache_miss ->
        case perform_comprehensive_health_check(MongosHost, Timeout) of
          {ok, HealthStatus} ->
            cache_health_status(MongosHost, HealthStatus),
            {ok, HealthStatus};
          Error ->
            Error
        end
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Measure network latency to mongos
-spec measure_mongos_latency(binary(), integer()) -> {ok, integer()} | {error, term()}.
measure_mongos_latency(MongosHost, Timeout) ->
  try
    {Host, Port} = parse_host_port(MongosHost),
    StartTime = erlang:system_time(microsecond),
    
    case gen_tcp:connect(binary_to_list(Host), Port, 
                        [binary, {active, false}, {packet, 0}], Timeout) of
      {ok, Socket} ->
        gen_tcp:close(Socket),
        EndTime = erlang:system_time(microsecond),
        Latency = (EndTime - StartTime) div 1000, % Convert to milliseconds
        {ok, Latency};
      {error, Reason} ->
        {error, {connection_failed, Reason}}
    end
  catch
    _:CatchReason ->
      {error, CatchReason}
  end.

%% @doc Check current load on mongos
-spec check_mongos_load(binary()) -> {ok, map()} | {error, term()}.
check_mongos_load(MongosHost) ->
  try
    case establish_connection(MongosHost, ?CONNECTION_TIMEOUT) of
      {ok, Connection} ->
        try
          LoadMetrics = get_server_status_metrics(Connection),
          mc_worker_api:disconnect(Connection),
          {ok, LoadMetrics}
        catch
          _:Reason ->
            mc_worker_api:disconnect(Connection),
            {error, Reason}
        end;
      Error ->
        Error
    end
  catch
    _:LoadReason ->
      {error, LoadReason}
  end.

%% @doc Check health of a shard
-spec check_shard_health(binary()) -> {ok, #health_status{}} | {error, term()}.
check_shard_health(ShardHost) ->
  check_mongos_health(ShardHost). % Same logic for now

%% @doc Check health of a replica set
-spec check_replica_set_health(list()) -> {ok, map()} | {error, term()}.
check_replica_set_health(ReplicaSetHosts) ->
  try
    HealthChecks = [check_mongos_health(Host) || Host <- ReplicaSetHosts],
    
    {Successful, Failed} = lists:partition(
      fun({ok, _}) -> true; (_) -> false end,
      HealthChecks
    ),
    
    SuccessfulResults = [Status || {ok, Status} <- Successful],
    FailedResults = [Error || {error, Error} <- Failed],
    
    % Analyze replica set health
    ReplicaSetStatus = analyze_replica_set_status(SuccessfulResults),
    
    {ok, #{
      overall_status => ReplicaSetStatus,
      healthy_members => length(SuccessfulResults),
      total_members => length(ReplicaSetHosts),
      failed_members => FailedResults,
      member_details => SuccessfulResults
    }}
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Get health status for a specific host
-spec get_health_status(binary()) -> {ok, #health_status{}} | {error, term()}.
get_health_status(Host) ->
  gen_server:call(?SERVER, {get_health_status, Host}).

%% @doc Get health status for all monitored hosts
-spec get_all_health_status() -> {ok, list()} | {error, term()}.
get_all_health_status() ->
  gen_server:call(?SERVER, get_all_health_status).

%% @doc Register a host for continuous monitoring
-spec register_for_monitoring(binary(), map()) -> ok.
register_for_monitoring(Host, Options) ->
  gen_server:cast(?SERVER, {register_host, Host, Options}).

%% @doc Unregister a host from monitoring
-spec unregister_from_monitoring(binary()) -> ok.
unregister_from_monitoring(Host) ->
  gen_server:cast(?SERVER, {unregister_host, Host}).

%% @doc Set health check thresholds
-spec set_health_thresholds(map()) -> ok.
set_health_thresholds(Thresholds) ->
  gen_server:cast(?SERVER, {set_thresholds, Thresholds}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Options) ->
  process_flag(trap_exit, true),
  
  % Create ETS table for health cache
  HealthCache = ets:new(health_cache, [set, private, {keypos, #health_status.host}]),
  
  % Set default thresholds
  DefaultThresholds = #{
    max_latency_ms => 1000,
    max_cpu_usage => 80.0,
    max_memory_usage => 85.0,
    max_disk_usage => 90.0,
    max_replication_lag => 10000,
    max_error_rate => 5.0
  },
  
  Thresholds = maps:merge(DefaultThresholds, maps:get(thresholds, Options, #{})),
  
  % Start periodic health checks
  {ok, Timer} = timer:send_interval(?HEALTH_CHECK_INTERVAL, periodic_health_check),
  
  State = #state{
    monitored_hosts = #{},
    health_cache = HealthCache,
    check_timer = Timer,
    thresholds = Thresholds
  },
  
  {ok, State}.

handle_call({get_health_status, Host}, _From, State) ->
  case ets:lookup(State#state.health_cache, Host) of
    [HealthStatus] ->
      {reply, {ok, HealthStatus}, State};
    [] ->
      {reply, {error, not_found}, State}
  end;

handle_call(get_all_health_status, _From, State) ->
  AllStatus = ets:tab2list(State#state.health_cache),
  {reply, {ok, AllStatus}, State};

handle_call(_Request, _From, State) ->
  {reply, {error, unknown_request}, State}.

handle_cast({register_host, Host, Options}, State) ->
  MonitoredHosts = State#state.monitored_hosts,
  UpdatedHosts = MonitoredHosts#{Host => Options},
  NewState = State#state{monitored_hosts = UpdatedHosts},
  {noreply, NewState};

handle_cast({unregister_host, Host}, State) ->
  MonitoredHosts = State#state.monitored_hosts,
  UpdatedHosts = maps:remove(Host, MonitoredHosts),
  ets:delete(State#state.health_cache, Host),
  NewState = State#state{monitored_hosts = UpdatedHosts},
  {noreply, NewState};

handle_cast({set_thresholds, Thresholds}, State) ->
  NewState = State#state{thresholds = Thresholds},
  {noreply, NewState};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(periodic_health_check, State) ->
  % Perform health checks for all monitored hosts
  MonitoredHosts = maps:keys(State#state.monitored_hosts),
  spawn(fun() -> perform_batch_health_checks(MonitoredHosts) end),
  {noreply, State};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{check_timer = Timer, health_cache = Cache}) ->
  timer:cancel(Timer),
  ets:delete(Cache),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Perform comprehensive health check for a host
-spec perform_comprehensive_health_check(binary(), integer()) -> {ok, #health_status{}} | {error, term()}.
perform_comprehensive_health_check(MongosHost, Timeout) ->
  try
    StartTime = erlang:system_time(millisecond),

    % Step 1: Check basic connectivity
    case measure_mongos_latency(MongosHost, Timeout) of
      {ok, Latency} ->
        % Step 2: Establish MongoDB connection
        case establish_connection(MongosHost, Timeout) of
          {ok, Connection} ->
            try
              % Step 3: Perform detailed health checks
              HealthMetrics = perform_detailed_health_checks(Connection),
              mc_worker_api:disconnect(Connection),

              % Step 4: Evaluate overall health status
              OverallStatus = evaluate_health_status(HealthMetrics, Latency),

              HealthStatus = #health_status{
                host = MongosHost,
                status = OverallStatus,
                last_check = StartTime,
                latency_ms = Latency,
                connection_count = maps:get(connection_count, HealthMetrics, 0),
                cpu_usage = maps:get(cpu_usage, HealthMetrics, 0.0),
                memory_usage = maps:get(memory_usage, HealthMetrics, 0.0),
                disk_usage = maps:get(disk_usage, HealthMetrics, 0.0),
                replication_lag = maps:get(replication_lag, HealthMetrics, 0),
                error_rate = maps:get(error_rate, HealthMetrics, 0.0),
                details = HealthMetrics
              },

              {ok, HealthStatus}
            catch
              _:Reason ->
                mc_worker_api:disconnect(Connection),
                {error, {health_check_failed, Reason}}
            end;
          {error, ConnectionError} ->
            % Connection failed - create unhealthy status
            HealthStatus = #health_status{
              host = MongosHost,
              status = unreachable,
              last_check = StartTime,
              latency_ms = 999999,
              details = #{error => ConnectionError}
            },
            {ok, HealthStatus}
        end;
      {error, LatencyError} ->
        % Network connectivity failed
        HealthStatus = #health_status{
          host = MongosHost,
          status = unreachable,
          last_check = StartTime,
          latency_ms = 999999,
          details = #{error => LatencyError}
        },
        {ok, HealthStatus}
    end
  catch
    _:HealthReason ->
      {error, HealthReason}
  end.

%% @doc Establish MongoDB connection
-spec establish_connection(binary(), integer()) -> {ok, pid()} | {error, term()}.
establish_connection(MongosHost, Timeout) ->
  try
    {Host, Port} = parse_host_port(MongosHost),

    ConnectionOptions = [
      {host, binary_to_list(Host)},
      {port, Port},
      {database, <<"admin">>},
      {connect_timeout_ms, Timeout},
      {socket_timeout_ms, Timeout}
    ],

    mc_worker_api:connect(ConnectionOptions)
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Parse host:port string
-spec parse_host_port(binary()) -> {binary(), integer()}.
parse_host_port(HostPort) ->
  case binary:split(HostPort, <<":">>) of
    [Host, PortBin] ->
      Port = binary_to_integer(PortBin),
      {Host, Port};
    [Host] ->
      {Host, 27017} % Default MongoDB port
  end.

%% @doc Perform detailed health checks using MongoDB commands
-spec perform_detailed_health_checks(pid()) -> map().
perform_detailed_health_checks(Connection) ->
  try
    % Get server status
    ServerStatus = get_server_status(Connection),

    % Get database stats
    DbStats = get_database_stats(Connection),

    % Get replication status (if applicable)
    ReplStatus = get_replication_status(Connection),

    % Get connection metrics
    ConnectionMetrics = get_connection_metrics(Connection),

    % Combine all metrics
    maps:merge(maps:merge(ServerStatus, DbStats),
               maps:merge(ReplStatus, ConnectionMetrics))
  catch
    _:Reason ->
      #{error => Reason}
  end.

%% @doc Get server status metrics
-spec get_server_status(pid()) -> map().
get_server_status(Connection) ->
  try
    case mc_worker_api:command(Connection, {<<"serverStatus">>, 1}) of
      {true, Status} ->
        extract_server_metrics(Status);
      _ ->
        #{error => server_status_failed}
    end
  catch
    _:_ ->
      #{error => server_status_exception}
  end.

%% @doc Extract relevant metrics from serverStatus
-spec extract_server_metrics(map()) -> map().
extract_server_metrics(Status) ->
  try
    Connections = maps:get(<<"connections">>, Status, #{}),
    Memory = maps:get(<<"mem">>, Status, #{}),
    Network = maps:get(<<"network">>, Status, #{}),
    Opcounters = maps:get(<<"opcounters">>, Status, #{}),

    #{
      connection_count => maps:get(<<"current">>, Connections, 0),
      available_connections => maps:get(<<"available">>, Connections, 0),
      memory_resident_mb => maps:get(<<"resident">>, Memory, 0),
      memory_virtual_mb => maps:get(<<"virtual">>, Memory, 0),
      network_bytes_in => maps:get(<<"bytesIn">>, Network, 0),
      network_bytes_out => maps:get(<<"bytesOut">>, Network, 0),
      ops_insert => maps:get(<<"insert">>, Opcounters, 0),
      ops_query => maps:get(<<"query">>, Opcounters, 0),
      ops_update => maps:get(<<"update">>, Opcounters, 0),
      ops_delete => maps:get(<<"delete">>, Opcounters, 0),
      uptime_seconds => maps:get(<<"uptime">>, Status, 0)
    }
  catch
    _:_ ->
      #{error => metric_extraction_failed}
  end.

%% @doc Get database statistics
-spec get_database_stats(pid()) -> map().
get_database_stats(Connection) ->
  try
    case mc_worker_api:command(Connection, {<<"dbStats">>, 1}) of
      {true, Stats} ->
        #{
          db_size_bytes => maps:get(<<"dataSize">>, Stats, 0),
          storage_size_bytes => maps:get(<<"storageSize">>, Stats, 0),
          index_size_bytes => maps:get(<<"indexSize">>, Stats, 0),
          collection_count => maps:get(<<"collections">>, Stats, 0),
          object_count => maps:get(<<"objects">>, Stats, 0)
        };
      _ ->
        #{error => db_stats_failed}
    end
  catch
    _:_ ->
      #{error => db_stats_exception}
  end.

%% @doc Get replication status
-spec get_replication_status(pid()) -> map().
get_replication_status(Connection) ->
  try
    case mc_worker_api:command(Connection, {<<"replSetGetStatus">>, 1}) of
      {true, ReplStatus} ->
        Members = maps:get(<<"members">>, ReplStatus, []),
        PrimaryMember = find_primary_member(Members),
        SecondaryMembers = find_secondary_members(Members),

        #{
          replication_lag => calculate_max_replication_lag(Members),
          primary_available => PrimaryMember =/= undefined,
          secondary_count => length(SecondaryMembers),
          replica_set_name => maps:get(<<"set">>, ReplStatus, <<"unknown">>)
        };
      _ ->
        % Not a replica set or command failed
        #{replication_lag => 0, primary_available => true}
    end
  catch
    _:_ ->
      #{replication_lag => 0, primary_available => true}
  end.

%% @doc Get connection metrics
-spec get_connection_metrics(pid()) -> map().
get_connection_metrics(Connection) ->
  try
    case mc_worker_api:command(Connection, {<<"connPoolStats">>, 1}) of
      {true, ConnStats} ->
        #{
          pool_created_connections => maps:get(<<"totalCreated">>, ConnStats, 0),
          pool_available_connections => maps:get(<<"totalAvailable">>, ConnStats, 0),
          pool_in_use_connections => maps:get(<<"totalInUse">>, ConnStats, 0)
        };
      _ ->
        #{error => connection_stats_failed}
    end
  catch
    _:_ ->
      #{error => connection_stats_exception}
  end.

%% @doc Evaluate overall health status based on metrics
-spec evaluate_health_status(map(), integer()) -> healthy | degraded | unhealthy.
evaluate_health_status(Metrics, Latency) ->
  try
    % Get default thresholds (in real implementation, would get from state)
    Thresholds = #{
      max_latency_ms => 1000,
      max_cpu_usage => 80.0,
      max_memory_usage => 85.0,
      max_replication_lag => 10000,
      max_error_rate => 5.0
    },

    Issues = check_health_thresholds(Metrics, Latency, Thresholds),

    case length(Issues) of
      0 -> healthy;
      N when N =< 2 -> degraded;
      _ -> unhealthy
    end
  catch
    _:_ -> unhealthy
  end.

%% @doc Check metrics against thresholds
-spec check_health_thresholds(map(), integer(), map()) -> list().
check_health_thresholds(Metrics, Latency, Thresholds) ->
  Issues = [],

  % Check latency
  Issues1 = case Latency > maps:get(max_latency_ms, Thresholds) of
    true -> [high_latency | Issues];
    false -> Issues
  end,

  % Check replication lag
  ReplLag = maps:get(replication_lag, Metrics, 0),
  Issues2 = case ReplLag > maps:get(max_replication_lag, Thresholds) of
    true -> [high_replication_lag | Issues1];
    false -> Issues1
  end,

  % Check for errors in metrics
  Issues3 = case maps:get(error, Metrics, undefined) of
    undefined -> Issues2;
    _ -> [metric_errors | Issues2]
  end,

  Issues3.

%% @doc Get cached health status
-spec get_cached_health_status(binary()) -> {ok, #health_status{}} | cache_miss.
get_cached_health_status(Host) ->
  try
    case ets:info(health_cache_table) of
      undefined ->
        % Create cache table if it doesn't exist
        ets:new(health_cache_table, [named_table, public, {keypos, #health_status.host}]),
        cache_miss;
      _ ->
        case ets:lookup(health_cache_table, Host) of
          [HealthStatus] ->
            Now = erlang:system_time(millisecond),
            if (Now - HealthStatus#health_status.last_check) < ?HEALTH_CACHE_TTL ->
              {ok, HealthStatus};
            true ->
              ets:delete(health_cache_table, Host),
              cache_miss
            end;
          [] ->
            cache_miss
        end
    end
  catch
    _:_ -> cache_miss
  end.

%% @doc Cache health status
-spec cache_health_status(binary(), #health_status{}) -> ok.
cache_health_status(_Host, HealthStatus) ->
  try
    case ets:info(health_cache_table) of
      undefined ->
        ets:new(health_cache_table, [named_table, public, {keypos, #health_status.host}]);
      _ ->
        ok
    end,
    ets:insert(health_cache_table, HealthStatus),
    ok
  catch
    _:_ -> ok
  end.

%% @doc Find primary member in replica set
-spec find_primary_member(list()) -> map() | undefined.
find_primary_member([]) ->
  undefined;
find_primary_member([#{<<"state">> := 1} = Member | _]) ->
  Member; % State 1 = PRIMARY
find_primary_member([_ | Rest]) ->
  find_primary_member(Rest).

%% @doc Find secondary members in replica set
-spec find_secondary_members(list()) -> list().
find_secondary_members(Members) ->
  [Member || #{<<"state">> := State} = Member <- Members, State =:= 2].

%% @doc Calculate maximum replication lag across all members
-spec calculate_max_replication_lag(list()) -> integer().
calculate_max_replication_lag(Members) ->
  try
    PrimaryMember = find_primary_member(Members),
    case PrimaryMember of
      undefined -> 0;
      _ ->
        PrimaryOpTime = get_member_optime(PrimaryMember),
        SecondaryMembers = find_secondary_members(Members),

        Lags = [calculate_member_lag(PrimaryOpTime, get_member_optime(Member))
                || Member <- SecondaryMembers],

        case Lags of
          [] -> 0;
          _ -> lists:max(Lags)
        end
    end
  catch
    _:_ -> 0
  end.

%% @doc Get operation time from member status
-spec get_member_optime(map()) -> integer().
get_member_optime(Member) ->
  try
    OptimeDoc = maps:get(<<"optime">>, Member, #{}),
    case maps:get(<<"ts">>, OptimeDoc, undefined) of
      undefined -> 0;
      {Timestamp, _} -> Timestamp;
      Timestamp when is_integer(Timestamp) -> Timestamp;
      _ -> 0
    end
  catch
    _:_ -> 0
  end.

%% @doc Calculate lag between primary and secondary
-spec calculate_member_lag(integer(), integer()) -> integer().
calculate_member_lag(PrimaryOpTime, SecondaryOpTime) ->
  abs(PrimaryOpTime - SecondaryOpTime).

%% @doc Analyze replica set overall status
-spec analyze_replica_set_status(list()) -> healthy | degraded | unhealthy.
analyze_replica_set_status([]) ->
  unhealthy;
analyze_replica_set_status(MemberStatuses) ->
  HealthyCount = length([S || #health_status{status = healthy} = S <- MemberStatuses]),
  TotalCount = length(MemberStatuses),

  HealthRatio = HealthyCount / TotalCount,

  if
    HealthRatio >= 0.8 -> healthy;
    HealthRatio >= 0.5 -> degraded;
    true -> unhealthy
  end.

%% @doc Perform batch health checks for multiple hosts
-spec perform_batch_health_checks(list()) -> ok.
perform_batch_health_checks(Hosts) ->
  try
    % Perform health checks in parallel
    CheckTasks = [spawn_monitor(fun() ->
      case check_mongos_health(Host) of
        {ok, HealthStatus} ->
          cache_health_status(Host, HealthStatus);
        _ ->
          ok
      end
    end) || Host <- Hosts],

    % Wait for all checks to complete (with timeout)
    wait_for_health_checks(CheckTasks, 30000),
    ok
  catch
    _:_ -> ok
  end.

%% @doc Wait for health check tasks to complete
-spec wait_for_health_checks(list(), integer()) -> ok.
wait_for_health_checks([], _Timeout) ->
  ok;
wait_for_health_checks(Tasks, Timeout) ->
  StartTime = erlang:system_time(millisecond),
  wait_for_health_checks_loop(Tasks, StartTime, Timeout).

%% @doc Wait loop for health check tasks
-spec wait_for_health_checks_loop(list(), integer(), integer()) -> ok.
wait_for_health_checks_loop([], _StartTime, _Timeout) ->
  ok;
wait_for_health_checks_loop(Tasks, StartTime, Timeout) ->
  Now = erlang:system_time(millisecond),
  if
    (Now - StartTime) > Timeout ->
      % Timeout reached, kill remaining tasks
      [exit(Pid, kill) || {Pid, _Ref} <- Tasks],
      ok;
    true ->
      receive
        {'DOWN', Ref, process, Pid, _Reason} ->
          RemainingTasks = [{P, R} || {P, R} <- Tasks, P =/= Pid, R =/= Ref],
          wait_for_health_checks_loop(RemainingTasks, StartTime, Timeout)
      after 1000 ->
        wait_for_health_checks_loop(Tasks, StartTime, Timeout)
      end
  end.

%% @doc Get server status metrics for load checking
-spec get_server_status_metrics(pid()) -> map().
get_server_status_metrics(Connection) ->
  try
    case mc_worker_api:command(Connection, {<<"serverStatus">>, 1}) of
      {true, Status} ->
        extract_load_metrics(Status);
      _ ->
        #{error => server_status_failed}
    end
  catch
    _:_ ->
      #{error => server_status_exception}
  end.

%% @doc Extract load-specific metrics from serverStatus
-spec extract_load_metrics(map()) -> map().
extract_load_metrics(Status) ->
  try
    Connections = maps:get(<<"connections">>, Status, #{}),
    Memory = maps:get(<<"mem">>, Status, #{}),
    GlobalLock = maps:get(<<"globalLock">>, Status, #{}),

    CurrentConnections = maps:get(<<"current">>, Connections, 0),
    AvailableConnections = maps:get(<<"available">>, Connections, 0),
    TotalConnections = CurrentConnections + AvailableConnections,

    ConnectionUtilization = case TotalConnections of
      0 -> 0.0;
      _ -> (CurrentConnections / TotalConnections) * 100
    end,

    #{
      cpu_usage => calculate_cpu_usage(GlobalLock),
      memory_usage => calculate_memory_usage(Memory),
      connection_utilization => ConnectionUtilization,
      current_connections => CurrentConnections,
      available_connections => AvailableConnections
    }
  catch
    _:_ ->
      #{error => load_metric_extraction_failed}
  end.

%% @doc Calculate CPU usage from global lock stats
-spec calculate_cpu_usage(map()) -> float().
calculate_cpu_usage(GlobalLock) ->
  try
    TotalTime = maps:get(<<"totalTime">>, GlobalLock, 0),
    LockTime = maps:get(<<"lockTime">>, GlobalLock, 0),

    case TotalTime of
      0 -> 0.0;
      _ -> (LockTime / TotalTime) * 100
    end
  catch
    _:_ -> 0.0
  end.

%% @doc Calculate memory usage percentage
-spec calculate_memory_usage(map()) -> float().
calculate_memory_usage(Memory) ->
  try
    Resident = maps:get(<<"resident">>, Memory, 0),
    Virtual = maps:get(<<"virtual">>, Memory, 0),

    % Simple heuristic: if virtual memory is much larger than resident,
    % memory pressure might be high
    case Virtual of
      0 -> 0.0;
      _ -> min((Resident / Virtual) * 100, 100.0)
    end
  catch
    _:_ -> 0.0
  end.
