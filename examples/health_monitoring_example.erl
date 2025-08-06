%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB health monitoring examples demonstrating comprehensive health checks.
%%% @end
%%%-------------------------------------------------------------------
-module(health_monitoring_example).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").
-include("mongo_health.hrl").

-export([
  run_basic_health_check/0,
  run_comprehensive_monitoring/0,
  run_replica_set_monitoring/0,
  run_sharded_cluster_monitoring/0,
  run_continuous_monitoring/0,
  demonstrate_health_thresholds/0
]).

%%%===================================================================
%%% Basic Health Check Examples
%%%===================================================================

%% @doc Demonstrate basic health check functionality
run_basic_health_check() ->
  io:format("=== Basic Health Check Example ===~n"),
  
  % Check health of a single mongos
  MongosHost = <<"localhost:27017">>,
  
  case mc_health_monitor:check_mongos_health(MongosHost) of
    {ok, HealthStatus} ->
      io:format("Health Status for ~s:~n", [MongosHost]),
      print_health_status(HealthStatus),
      
      % Check specific metrics
      case HealthStatus#health_status.status of
        healthy ->
          io:format("✅ Host is healthy~n");
        degraded ->
          io:format("⚠️  Host is degraded~n");
        unhealthy ->
          io:format("❌ Host is unhealthy~n");
        unreachable ->
          io:format("🔌 Host is unreachable~n")
      end;
    {error, Reason} ->
      io:format("❌ Health check failed: ~p~n", [Reason])
  end.

%% @doc Demonstrate comprehensive monitoring with multiple metrics
run_comprehensive_monitoring() ->
  io:format("=== Comprehensive Monitoring Example ===~n"),
  
  % Start health monitor
  {ok, _MonitorPid} = mc_health_monitor:start_link([
    {thresholds, #{
      max_latency_ms => 500,
      max_cpu_usage => 70.0,
      max_memory_usage => 80.0,
      max_replication_lag => 5000
    }}
  ]),
  
  % Register multiple hosts for monitoring
  Hosts = [
    <<"localhost:27017">>,
    <<"localhost:27018">>,
    <<"localhost:27019">>
  ],
  
  [mc_health_monitor:register_for_monitoring(Host, #{}) || Host <- Hosts],
  
  % Wait a bit for initial health checks
  timer:sleep(2000),
  
  % Get health status for all hosts
  case mc_health_monitor:get_all_health_status() of
    {ok, AllStatus} ->
      io:format("Health status for all monitored hosts:~n"),
      [print_health_status(Status) || Status <- AllStatus];
    {error, Reason} ->
      io:format("Failed to get health status: ~p~n", [Reason])
  end,
  
  % Stop monitor
  mc_health_monitor:stop().

%% @doc Demonstrate replica set health monitoring
run_replica_set_monitoring() ->
  io:format("=== Replica Set Monitoring Example ===~n"),
  
  ReplicaSetHosts = [
    <<"localhost:27017">>,
    <<"localhost:27018">>,
    <<"localhost:27019">>
  ],
  
  case mc_health_monitor:check_replica_set_health(ReplicaSetHosts) of
    {ok, ReplicaSetStatus} ->
      io:format("Replica Set Health Status:~n"),
      io:format("  Overall Status: ~p~n", [maps:get(overall_status, ReplicaSetStatus)]),
      io:format("  Healthy Members: ~p/~p~n", [
        maps:get(healthy_members, ReplicaSetStatus),
        maps:get(total_members, ReplicaSetStatus)
      ]),
      
      case maps:get(failed_members, ReplicaSetStatus) of
        [] ->
          io:format("  ✅ All members are accessible~n");
        FailedMembers ->
          io:format("  ❌ Failed members: ~p~n", [FailedMembers])
      end;
    {error, Reason} ->
      io:format("❌ Replica set health check failed: ~p~n", [Reason])
  end.

%% @doc Demonstrate sharded cluster monitoring
run_sharded_cluster_monitoring() ->
  io:format("=== Sharded Cluster Monitoring Example ===~n"),
  
  % Monitor mongos instances
  MongosHosts = [
    <<"mongos1:27017">>,
    <<"mongos2:27017">>
  ],
  
  io:format("Checking mongos health:~n"),
  MongoHealthChecks = [begin
    case mc_health_monitor:check_mongos_health(Host) of
      {ok, HealthStatus} ->
        io:format("  ~s: ~p (latency: ~pms)~n", [
          Host, 
          HealthStatus#health_status.status,
          HealthStatus#health_status.latency_ms
        ]),
        {Host, HealthStatus#health_status.status};
      {error, Reason} ->
        io:format("  ~s: ERROR - ~p~n", [Host, Reason]),
        {Host, error}
    end
  end || Host <- MongosHosts],
  
  % Select best mongos based on health
  HealthyMongos = [Host || {Host, Status} <- MongoHealthChecks, Status =:= healthy],
  
  case HealthyMongos of
    [] ->
      io:format("❌ No healthy mongos available~n");
    [BestMongos | _] ->
      io:format("✅ Selected mongos: ~s~n", [BestMongos])
  end.

%% @doc Demonstrate continuous monitoring
run_continuous_monitoring() ->
  io:format("=== Continuous Monitoring Example ===~n"),
  
  % Start health monitor with custom settings
  {ok, _MonitorPid} = mc_health_monitor:start_link([
    {detailed_logging, true},
    {thresholds, #{
      max_latency_ms => 1000,
      max_cpu_usage => 75.0,
      max_memory_usage => 85.0
    }}
  ]),
  
  % Register hosts for continuous monitoring
  Hosts = [<<"localhost:27017">>, <<"localhost:27018">>],
  [mc_health_monitor:register_for_monitoring(Host, #{
    check_interval => 30000,
    alert_on_degraded => true
  }) || Host <- Hosts],
  
  io:format("Monitoring started. Checking health every 30 seconds...~n"),
  
  % Monitor for a short period
  monitor_loop(5), % Monitor for 5 iterations
  
  % Stop monitoring
  mc_health_monitor:stop(),
  io:format("Monitoring stopped.~n").

%% @doc Demonstrate health threshold configuration
demonstrate_health_thresholds() ->
  io:format("=== Health Thresholds Example ===~n"),
  
  % Start monitor with strict thresholds
  {ok, _MonitorPid} = mc_health_monitor:start_link(),
  
  % Set custom thresholds
  StrictThresholds = #{
    max_latency_ms => 200,      % Very low latency requirement
    max_cpu_usage => 50.0,      % Conservative CPU usage
    max_memory_usage => 60.0,   % Conservative memory usage
    max_replication_lag => 1000 % Low replication lag tolerance
  },
  
  mc_health_monitor:set_health_thresholds(StrictThresholds),
  
  % Check health with strict thresholds
  TestHost = <<"localhost:27017">>,
  case mc_health_monitor:check_mongos_health(TestHost) of
    {ok, HealthStatus} ->
      io:format("Health check with strict thresholds:~n"),
      print_health_status(HealthStatus),
      
      % Demonstrate threshold impact
      case HealthStatus#health_status.status of
        healthy ->
          io:format("✅ Host meets strict requirements~n");
        degraded ->
          io:format("⚠️  Host fails some strict requirements~n");
        _ ->
          io:format("❌ Host fails strict requirements~n")
      end;
    {error, Reason} ->
      io:format("❌ Health check failed: ~p~n", [Reason])
  end,
  
  mc_health_monitor:stop().

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% @doc Print detailed health status
-spec print_health_status(#health_status{}) -> ok.
print_health_status(HealthStatus) ->
  io:format("  Host: ~s~n", [HealthStatus#health_status.host]),
  io:format("  Status: ~p~n", [HealthStatus#health_status.status]),
  io:format("  Latency: ~pms~n", [HealthStatus#health_status.latency_ms]),
  io:format("  Connections: ~p~n", [HealthStatus#health_status.connection_count]),
  io:format("  CPU Usage: ~.1f%~n", [HealthStatus#health_status.cpu_usage]),
  io:format("  Memory Usage: ~.1f%~n", [HealthStatus#health_status.memory_usage]),
  io:format("  Replication Lag: ~pms~n", [HealthStatus#health_status.replication_lag]),
  io:format("  Last Check: ~p~n", [HealthStatus#health_status.last_check]),
  
  case HealthStatus#health_status.details of
    #{error := Error} ->
      io:format("  Error Details: ~p~n", [Error]);
    Details when map_size(Details) > 0 ->
      io:format("  Additional Details: ~p~n", [Details]);
    _ ->
      ok
  end,
  io:format("~n").

%% @doc Monitor loop for continuous monitoring demonstration
-spec monitor_loop(integer()) -> ok.
monitor_loop(0) ->
  ok;
monitor_loop(Iterations) ->
  timer:sleep(10000), % Wait 10 seconds
  
  case mc_health_monitor:get_all_health_status() of
    {ok, AllStatus} ->
      io:format("=== Health Check Iteration ~p ===~n", [6 - Iterations]),
      
      HealthySummary = lists:foldl(
        fun(Status, Acc) ->
          case Status#health_status.status of
            healthy -> Acc#{healthy => maps:get(healthy, Acc, 0) + 1};
            degraded -> Acc#{degraded => maps:get(degraded, Acc, 0) + 1};
            unhealthy -> Acc#{unhealthy => maps:get(unhealthy, Acc, 0) + 1};
            unreachable -> Acc#{unreachable => maps:get(unreachable, Acc, 0) + 1}
          end
        end,
        #{},
        AllStatus
      ),
      
      io:format("Health Summary: ~p~n", [HealthySummary]);
    {error, Reason} ->
      io:format("Failed to get health status: ~p~n", [Reason])
  end,
  
  monitor_loop(Iterations - 1).

%% @doc Run all health monitoring examples
run_all_examples() ->
  io:format("=== MongoDB Health Monitoring Examples ===~n~n"),
  
  run_basic_health_check(),
  io:format("~n"),
  
  run_comprehensive_monitoring(),
  io:format("~n"),
  
  run_replica_set_monitoring(),
  io:format("~n"),
  
  run_sharded_cluster_monitoring(),
  io:format("~n"),
  
  demonstrate_health_thresholds(),
  io:format("~n"),
  
  io:format("=== All Examples Completed ===~n").
