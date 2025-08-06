%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB health monitoring record definitions.
%%% @end
%%%-------------------------------------------------------------------

-ifndef(MONGO_HEALTH_HRL).
-define(MONGO_HEALTH_HRL, true).

%% Health status record
-record(health_status, {
  host :: binary(),
  status :: healthy | degraded | unhealthy | unreachable,
  last_check :: integer(),
  latency_ms :: integer(),
  connection_count :: integer(),
  cpu_usage :: float(),
  memory_usage :: float(),
  disk_usage :: float(),
  replication_lag :: integer(),
  error_rate :: float(),
  details :: map()
}).

%% Health thresholds record
-record(health_thresholds, {
  max_latency_ms :: integer(),
  max_cpu_usage :: float(),
  max_memory_usage :: float(),
  max_disk_usage :: float(),
  max_replication_lag :: integer(),
  max_error_rate :: float()
}).

%% Type definitions
-type health_status() :: #health_status{}.
-type health_thresholds() :: #health_thresholds{}.

-endif.
