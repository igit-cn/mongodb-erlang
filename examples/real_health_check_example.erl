%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% Real MongoDB health check and load monitoring examples.
%%% Demonstrates the enhanced, production-ready health checking capabilities.
%%% @end
%%%-------------------------------------------------------------------
-module(real_health_check_example).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").
-include("mongo_health.hrl").

-export([
  test_real_latency_measurement/0,
  test_real_load_monitoring/0,
  test_comprehensive_health_check/0,
  test_mongos_selection_strategies/0,
  compare_old_vs_new_implementation/0,
  run_production_readiness_test/0
]).

%%%===================================================================
%%% Real Implementation Tests
%%%===================================================================

%% @doc Test real network latency measurement
test_real_latency_measurement() ->
  io:format("=== Real Latency Measurement Test ===~n"),
  
  TestHosts = [
    <<"localhost:27017">>,
    <<"127.0.0.1:27017">>,
    <<"nonexistent.host:27017">>
  ],
  
  lists:foreach(fun(Host) ->
    io:format("Testing latency to ~s:~n", [Host]),
    
    % Test TCP latency
    case mc_sharded_transaction:measure_mongos_latency(Host, 5000) of
      {ok, Latency} ->
        io:format("  ✅ Latency: ~pms~n", [Latency]),
        
        % Categorize latency
        case Latency of
          L when L < 10 -> io:format("  🚀 Excellent latency~n");
          L when L < 50 -> io:format("  ✅ Good latency~n");
          L when L < 200 -> io:format("  ⚠️  Acceptable latency~n");
          _ -> io:format("  ❌ High latency~n")
        end;
      {error, Reason} ->
        io:format("  ❌ Failed: ~p~n", [Reason])
    end,
    io:format("~n")
  end, TestHosts).

%% @doc Test real load monitoring
test_real_load_monitoring() ->
  io:format("=== Real Load Monitoring Test ===~n"),
  
  TestHost = <<"localhost:27017">>,
  
  case mc_sharded_transaction:check_mongos_load(TestHost) of
    {ok, LoadMetrics} ->
      io:format("Load metrics for ~s:~n", [TestHost]),
      print_load_metrics(LoadMetrics),
      
      % Analyze load level
      OverallScore = maps:get(overall_load_score, LoadMetrics, 50.0),
      case OverallScore of
        Score when Score < 30 -> io:format("🟢 Low load (~.1f%)~n", [Score]);
        Score when Score < 60 -> io:format("🟡 Moderate load (~.1f%)~n", [Score]);
        Score when Score < 80 -> io:format("🟠 High load (~.1f%)~n", [Score]);
        Score -> io:format("🔴 Critical load (~.1f%)~n", [Score])
      end;
    {error, Reason} ->
      io:format("❌ Load check failed: ~p~n", [Reason])
  end.

%% @doc Test comprehensive health check
test_comprehensive_health_check() ->
  io:format("=== Comprehensive Health Check Test ===~n"),
  
  TestHost = <<"localhost:27017">>,
  
  case mc_health_monitor:check_mongos_health(TestHost, 10000) of
    {ok, HealthStatus} ->
      io:format("Comprehensive health status for ~s:~n", [TestHost]),
      print_comprehensive_health_status(HealthStatus),
      
      % Health recommendations
      provide_health_recommendations(HealthStatus);
    {error, Reason} ->
      io:format("❌ Health check failed: ~p~n", [Reason])
  end.

%% @doc Test different mongos selection strategies
test_mongos_selection_strategies() ->
  io:format("=== Mongos Selection Strategies Test ===~n"),
  
  MongosList = [
    <<"localhost:27017">>,
    <<"localhost:27018">>,
    <<"localhost:27019">>
  ],
  
  Strategies = [round_robin, health_based, latency_based, load_based],
  
  lists:foreach(fun(Strategy) ->
    io:format("Testing ~p strategy:~n", [Strategy]),
    
    case mc_sharded_transaction:select_optimal_mongos(MongosList, #{selection_strategy => Strategy}) of
      {ok, SelectedMongos} ->
        io:format("  ✅ Selected: ~s~n", [SelectedMongos]);
      {error, Reason} ->
        io:format("  ❌ Selection failed: ~p~n", [Reason])
    end
  end, Strategies).

%% @doc Compare old vs new implementation
compare_old_vs_new_implementation() ->
  io:format("=== Old vs New Implementation Comparison ===~n"),
  
  TestHost = <<"localhost:27017">>,
  
  % Simulate old implementation results
  io:format("Old Implementation (Simulated):~n"),
  io:format("  Status: healthy (random)~n"),
  io:format("  Response Time: ~pms (random)~n", [rand:uniform(100)]),
  io:format("  Connections: ~p (random)~n", [rand:uniform(1000)]),
  io:format("  ❌ No real metrics~n"),
  io:format("  ❌ No actual connectivity test~n"),
  io:format("  ❌ No MongoDB-specific checks~n~n"),
  
  % New implementation results
  io:format("New Implementation (Real):~n"),
  case mc_health_monitor:check_mongos_health(TestHost) of
    {ok, HealthStatus} ->
      io:format("  Status: ~p (real assessment)~n", [HealthStatus#health_status.status]),
      io:format("  Latency: ~pms (real TCP + MongoDB)~n", [HealthStatus#health_status.latency_ms]),
      io:format("  Connections: ~p (real serverStatus)~n", [HealthStatus#health_status.connection_count]),
      io:format("  CPU Usage: ~.1f% (real metrics)~n", [HealthStatus#health_status.cpu_usage]),
      io:format("  Memory Usage: ~.1f% (real metrics)~n", [HealthStatus#health_status.memory_usage]),
      io:format("  ✅ Real connectivity test~n"),
      io:format("  ✅ Real MongoDB metrics~n"),
      io:format("  ✅ Comprehensive health assessment~n");
    {error, Reason} ->
      io:format("  ❌ Real check failed: ~p~n", [Reason])
  end.

%% @doc Run production readiness test
run_production_readiness_test() ->
  io:format("=== Production Readiness Test ===~n"),
  
  TestHost = <<"localhost:27017">>,
  
  % Test 1: Connection reliability
  io:format("1. Testing connection reliability...~n"),
  ConnectionTests = [test_connection_reliability(TestHost) || _ <- lists:seq(1, 5)],
  SuccessfulConnections = length([ok || ok <- ConnectionTests]),
  io:format("   Connection success rate: ~p/5 (~.1f%)~n", 
    [SuccessfulConnections, (SuccessfulConnections/5)*100]),
  
  % Test 2: Latency consistency
  io:format("2. Testing latency consistency...~n"),
  LatencyTests = [measure_latency_sample(TestHost) || _ <- lists:seq(1, 10)],
  ValidLatencies = [L || {ok, L} <- LatencyTests],
  case ValidLatencies of
    [] ->
      io:format("   ❌ No valid latency measurements~n");
    _ ->
      AvgLatency = lists:sum(ValidLatencies) / length(ValidLatencies),
      MaxLatency = lists:max(ValidLatencies),
      MinLatency = lists:min(ValidLatencies),
      io:format("   Average latency: ~.1fms~n", [AvgLatency]),
      io:format("   Latency range: ~p-~pms~n", [MinLatency, MaxLatency])
  end,
  
  % Test 3: Load metrics accuracy
  io:format("3. Testing load metrics accuracy...~n"),
  case mc_sharded_transaction:check_mongos_load(TestHost) of
    {ok, LoadMetrics} ->
      RequiredMetrics = [connection_utilization, cpu_utilization, memory_utilization, overall_load_score],
      PresentMetrics = [M || M <- RequiredMetrics, maps:is_key(M, LoadMetrics)],
      io:format("   Metrics coverage: ~p/~p (~.1f%)~n", 
        [length(PresentMetrics), length(RequiredMetrics), 
         (length(PresentMetrics)/length(RequiredMetrics))*100]);
    {error, _} ->
      io:format("   ❌ Load metrics collection failed~n")
  end,
  
  % Test 4: Error handling
  io:format("4. Testing error handling...~n"),
  ErrorTests = [
    test_invalid_host_handling(),
    test_timeout_handling(),
    test_connection_failure_handling()
  ],
  HandledErrors = length([ok || ok <- ErrorTests]),
  io:format("   Error handling: ~p/3 scenarios handled~n", [HandledErrors]),
  
  % Overall assessment
  io:format("~n=== Production Readiness Assessment ===~n"),
  TotalScore = (SuccessfulConnections * 20) + 
               (min(length(ValidLatencies), 10) * 5) +
               (length(PresentMetrics) * 10) +
               (HandledErrors * 15),
  
  case TotalScore of
    Score when Score >= 90 -> io:format("🟢 PRODUCTION READY (~p/100)~n", [Score]);
    Score when Score >= 70 -> io:format("🟡 MOSTLY READY (~p/100)~n", [Score]);
    Score when Score >= 50 -> io:format("🟠 NEEDS IMPROVEMENT (~p/100)~n", [Score]);
    Score -> io:format("🔴 NOT READY (~p/100)~n", [Score])
  end.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

%% @doc Print load metrics in a readable format
print_load_metrics(LoadMetrics) ->
  io:format("  Connection Utilization: ~.1f%~n", 
    [maps:get(connection_utilization, LoadMetrics, 0.0)]),
  io:format("  CPU Utilization: ~.1f%~n", 
    [maps:get(cpu_utilization, LoadMetrics, 0.0)]),
  io:format("  Memory Utilization: ~.1f%~n", 
    [maps:get(memory_utilization, LoadMetrics, 0.0)]),
  io:format("  Current Connections: ~p~n", 
    [maps:get(current_connections, LoadMetrics, 0)]),
  io:format("  Available Connections: ~p~n", 
    [maps:get(available_connections, LoadMetrics, 0)]),
  io:format("  Total Operations: ~p~n", 
    [maps:get(total_operations, LoadMetrics, 0)]),
  io:format("  Overall Load Score: ~.1f%~n", 
    [maps:get(overall_load_score, LoadMetrics, 0.0)]).

%% @doc Print comprehensive health status
print_comprehensive_health_status(HealthStatus) ->
  io:format("  Host: ~s~n", [HealthStatus#health_status.host]),
  io:format("  Overall Status: ~p~n", [HealthStatus#health_status.status]),
  io:format("  Network Latency: ~pms~n", [HealthStatus#health_status.latency_ms]),
  io:format("  Active Connections: ~p~n", [HealthStatus#health_status.connection_count]),
  io:format("  CPU Usage: ~.1f%~n", [HealthStatus#health_status.cpu_usage]),
  io:format("  Memory Usage: ~.1f%~n", [HealthStatus#health_status.memory_usage]),
  io:format("  Replication Lag: ~pms~n", [HealthStatus#health_status.replication_lag]),
  io:format("  Last Check: ~p~n", [HealthStatus#health_status.last_check]).

%% @doc Provide health recommendations based on status
provide_health_recommendations(HealthStatus) ->
  io:format("~nHealth Recommendations:~n"),
  
  case HealthStatus#health_status.latency_ms of
    L when L > 1000 ->
      io:format("  ⚠️  High latency detected. Check network connectivity.~n");
    _ -> ok
  end,
  
  case HealthStatus#health_status.cpu_usage of
    C when C > 80.0 ->
      io:format("  ⚠️  High CPU usage. Consider scaling or optimization.~n");
    _ -> ok
  end,
  
  case HealthStatus#health_status.memory_usage of
    M when M > 85.0 ->
      io:format("  ⚠️  High memory usage. Monitor for memory leaks.~n");
    _ -> ok
  end,
  
  case HealthStatus#health_status.replication_lag of
    R when R > 10000 ->
      io:format("  ⚠️  High replication lag. Check replica set health.~n");
    _ -> ok
  end,
  
  case HealthStatus#health_status.status of
    healthy ->
      io:format("  ✅ All systems operating normally.~n");
    degraded ->
      io:format("  ⚠️  Some performance issues detected.~n");
    unhealthy ->
      io:format("  ❌ Significant issues detected. Immediate attention required.~n");
    unreachable ->
      io:format("  🔌 Host unreachable. Check connectivity and service status.~n")
  end.

%% @doc Test connection reliability
test_connection_reliability(Host) ->
  case mc_sharded_transaction:measure_mongos_latency(Host, 5000) of
    {ok, _} -> ok;
    {error, _} -> error
  end.

%% @doc Measure latency sample
measure_latency_sample(Host) ->
  mc_sharded_transaction:measure_mongos_latency(Host, 3000).

%% @doc Test invalid host handling
test_invalid_host_handling() ->
  case mc_sharded_transaction:check_mongos_load(<<"invalid.host:27017">>) of
    {error, _} -> ok; % Expected error
    _ -> error
  end.

%% @doc Test timeout handling
test_timeout_handling() ->
  case mc_sharded_transaction:measure_mongos_latency(<<"slow.host:27017">>, 100) of
    {error, _} -> ok; % Expected timeout
    _ -> error
  end.

%% @doc Test connection failure handling
test_connection_failure_handling() ->
  case mc_sharded_transaction:check_mongos_load(<<"127.0.0.1:99999">>) of
    {error, _} -> ok; % Expected connection failure
    _ -> error
  end.
