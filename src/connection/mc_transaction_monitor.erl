%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB transaction monitoring and diagnostics module.
%%% Provides performance monitoring, metrics collection, and diagnostics.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_transaction_monitor).
-author("mongodb-erlang team").

-behaviour(gen_server).

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  start_link/0,
  start_link/1,
  stop/0,
  register_transaction/2,
  unregister_transaction/1,
  update_transaction_state/2,
  get_transaction_metrics/0,
  get_transaction_metrics/1,
  get_active_transactions/0,
  get_transaction_history/1,
  enable_detailed_logging/0,
  disable_detailed_logging/0,
  export_metrics/1,
  reset_metrics/0
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
-define(METRICS_TABLE, transaction_metrics).
-define(ACTIVE_TABLE, active_transactions).
-define(HISTORY_TABLE, transaction_history).
-define(MAX_HISTORY_SIZE, 1000).
-define(CLEANUP_INTERVAL, 60000). % 1 minute

-record(state, {
  detailed_logging :: boolean(),
  cleanup_timer :: timer:tref(),
  start_time :: integer()
}).

-record(transaction_info, {
  id :: pid(),
  session_id :: binary(),
  start_time :: integer(),
  end_time :: undefined | integer(),
  state :: atom(),
  operations_count :: integer(),
  retry_count :: integer(),
  error :: undefined | term(),
  topology_type :: atom()
}).

-record(metrics, {
  total_transactions :: integer(),
  successful_transactions :: integer(),
  failed_transactions :: integer(),
  aborted_transactions :: integer(),
  avg_duration_ms :: float(),
  max_duration_ms :: integer(),
  min_duration_ms :: integer(),
  total_retries :: integer(),
  active_count :: integer(),
  last_updated :: integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start the transaction monitor
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
  start_link([]).

%% @doc Start the transaction monitor with options
-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(Options) ->
  gen_server:start_link({local, ?SERVER}, ?MODULE, Options, []).

%% @doc Stop the transaction monitor
-spec stop() -> ok.
stop() ->
  gen_server:stop(?SERVER).

%% @doc Register a new transaction for monitoring
-spec register_transaction(pid(), map()) -> ok.
register_transaction(TransactionPid, Info) ->
  gen_server:cast(?SERVER, {register_transaction, TransactionPid, Info}).

%% @doc Unregister a transaction
-spec unregister_transaction(pid()) -> ok.
unregister_transaction(TransactionPid) ->
  gen_server:cast(?SERVER, {unregister_transaction, TransactionPid}).

%% @doc Update transaction state
-spec update_transaction_state(pid(), atom()) -> ok.
update_transaction_state(TransactionPid, NewState) ->
  gen_server:cast(?SERVER, {update_state, TransactionPid, NewState}).

%% @doc Get overall transaction metrics
-spec get_transaction_metrics() -> {ok, map()} | {error, term()}.
get_transaction_metrics() ->
  gen_server:call(?SERVER, get_metrics).

%% @doc Get metrics for specific transaction
-spec get_transaction_metrics(pid()) -> {ok, map()} | {error, term()}.
get_transaction_metrics(TransactionPid) ->
  gen_server:call(?SERVER, {get_transaction_metrics, TransactionPid}).

%% @doc Get list of active transactions
-spec get_active_transactions() -> {ok, list()} | {error, term()}.
get_active_transactions() ->
  gen_server:call(?SERVER, get_active_transactions).

%% @doc Get transaction history
-spec get_transaction_history(integer()) -> {ok, list()} | {error, term()}.
get_transaction_history(Limit) ->
  gen_server:call(?SERVER, {get_history, Limit}).

%% @doc Enable detailed logging
-spec enable_detailed_logging() -> ok.
enable_detailed_logging() ->
  gen_server:cast(?SERVER, enable_detailed_logging).

%% @doc Disable detailed logging
-spec disable_detailed_logging() -> ok.
disable_detailed_logging() ->
  gen_server:cast(?SERVER, disable_detailed_logging).

%% @doc Export metrics to file
-spec export_metrics(string()) -> ok | {error, term()}.
export_metrics(Filename) ->
  gen_server:call(?SERVER, {export_metrics, Filename}).

%% @doc Reset all metrics
-spec reset_metrics() -> ok.
reset_metrics() ->
  gen_server:cast(?SERVER, reset_metrics).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init(Options) ->
  process_flag(trap_exit, true),
  
  % Create ETS tables
  ets:new(?METRICS_TABLE, [named_table, public, {keypos, 1}]),
  ets:new(?ACTIVE_TABLE, [named_table, public, {keypos, #transaction_info.id}]),
  ets:new(?HISTORY_TABLE, [named_table, public, ordered_set]),
  
  % Initialize metrics
  init_metrics(),
  
  % Start cleanup timer
  {ok, Timer} = timer:send_interval(?CLEANUP_INTERVAL, cleanup),
  
  DetailedLogging = proplists:get_value(detailed_logging, Options, false),
  
  State = #state{
    detailed_logging = DetailedLogging,
    cleanup_timer = Timer,
    start_time = erlang:system_time(millisecond)
  },
  
  {ok, State}.

handle_call(get_metrics, _From, State) ->
  Metrics = calculate_current_metrics(),
  {reply, {ok, Metrics}, State};

handle_call({get_transaction_metrics, TransactionPid}, _From, State) ->
  case ets:lookup(?ACTIVE_TABLE, TransactionPid) of
    [TxnInfo] ->
      Metrics = transaction_info_to_metrics(TxnInfo),
      {reply, {ok, Metrics}, State};
    [] ->
      {reply, {error, transaction_not_found}, State}
  end;

handle_call(get_active_transactions, _From, State) ->
  Active = ets:tab2list(?ACTIVE_TABLE),
  ActiveList = [transaction_info_to_map(Info) || Info <- Active],
  {reply, {ok, ActiveList}, State};

handle_call({get_history, Limit}, _From, State) ->
  History = get_recent_history(Limit),
  {reply, {ok, History}, State};

handle_call({export_metrics, Filename}, _From, State) ->
  Result = export_metrics_to_file(Filename),
  {reply, Result, State};

handle_call(_Request, _From, State) ->
  {reply, {error, unknown_request}, State}.

handle_cast({register_transaction, TransactionPid, Info}, State) ->
  TxnInfo = create_transaction_info(TransactionPid, Info),
  ets:insert(?ACTIVE_TABLE, TxnInfo),
  update_metrics_counter(total_transactions, 1),
  update_metrics_counter(active_count, 1),
  log_transaction_event(register, TxnInfo, State),
  {noreply, State};

handle_cast({unregister_transaction, TransactionPid}, State) ->
  case ets:lookup(?ACTIVE_TABLE, TransactionPid) of
    [TxnInfo] ->
      ets:delete(?ACTIVE_TABLE, TransactionPid),
      CompletedInfo = TxnInfo#transaction_info{end_time = erlang:system_time(millisecond)},
      add_to_history(CompletedInfo),
      update_completion_metrics(CompletedInfo),
      update_metrics_counter(active_count, -1),
      log_transaction_event(unregister, CompletedInfo, State);
    [] ->
      ok
  end,
  {noreply, State};

handle_cast({update_state, TransactionPid, NewState}, State) ->
  case ets:lookup(?ACTIVE_TABLE, TransactionPid) of
    [TxnInfo] ->
      UpdatedInfo = TxnInfo#transaction_info{state = NewState},
      ets:insert(?ACTIVE_TABLE, UpdatedInfo),
      log_transaction_event(state_change, UpdatedInfo, State);
    [] ->
      ok
  end,
  {noreply, State};

handle_cast(enable_detailed_logging, State) ->
  {noreply, State#state{detailed_logging = true}};

handle_cast(disable_detailed_logging, State) ->
  {noreply, State#state{detailed_logging = false}};

handle_cast(reset_metrics, State) ->
  reset_all_metrics(),
  {noreply, State};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(cleanup, State) ->
  cleanup_old_history(),
  cleanup_stale_transactions(),
  {noreply, State};

handle_info({'EXIT', Pid, _Reason}, State) ->
  % Handle transaction process exit
  handle_cast({unregister_transaction, Pid}, State);

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #state{cleanup_timer = Timer}) ->
  timer:cancel(Timer),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Initialize metrics table
-spec init_metrics() -> ok.
init_metrics() ->
  Metrics = #metrics{
    total_transactions = 0,
    successful_transactions = 0,
    failed_transactions = 0,
    aborted_transactions = 0,
    avg_duration_ms = 0.0,
    max_duration_ms = 0,
    min_duration_ms = 999999999,
    total_retries = 0,
    active_count = 0,
    last_updated = erlang:system_time(millisecond)
  },
  ets:insert(?METRICS_TABLE, {global_metrics, Metrics}),
  ok.

%% @doc Create transaction info record
-spec create_transaction_info(pid(), map()) -> #transaction_info{}.
create_transaction_info(TransactionPid, Info) ->
  #transaction_info{
    id = TransactionPid,
    session_id = maps:get(session_id, Info, <<"unknown">>),
    start_time = erlang:system_time(millisecond),
    end_time = undefined,
    state = maps:get(state, Info, starting),
    operations_count = maps:get(operations_count, Info, 0),
    retry_count = maps:get(retry_count, Info, 0),
    error = undefined,
    topology_type = maps:get(topology_type, Info, unknown)
  }.

%% @doc Convert transaction info to metrics map
-spec transaction_info_to_metrics(#transaction_info{}) -> map().
transaction_info_to_metrics(#transaction_info{
  start_time = StartTime,
  end_time = EndTime,
  state = State,
  operations_count = OpsCount,
  retry_count = RetryCount,
  topology_type = TopologyType
}) ->
  Duration = case EndTime of
    undefined -> erlang:system_time(millisecond) - StartTime;
    _ -> EndTime - StartTime
  end,
  
  #{
    duration_ms => Duration,
    state => State,
    operations_count => OpsCount,
    retry_count => RetryCount,
    topology_type => TopologyType
  }.

%% @doc Convert transaction info to map
-spec transaction_info_to_map(#transaction_info{}) -> map().
transaction_info_to_map(#transaction_info{
  id = Id,
  session_id = SessionId,
  start_time = StartTime,
  state = State,
  operations_count = OpsCount,
  retry_count = RetryCount,
  topology_type = TopologyType
}) ->
  #{
    transaction_id => Id,
    session_id => SessionId,
    start_time => StartTime,
    state => State,
    operations_count => OpsCount,
    retry_count => RetryCount,
    topology_type => TopologyType,
    duration_ms => erlang:system_time(millisecond) - StartTime
  }.

%% @doc Update metrics counter
-spec update_metrics_counter(atom(), integer()) -> ok.
update_metrics_counter(Counter, Delta) ->
  case ets:lookup(?METRICS_TABLE, global_metrics) of
    [{global_metrics, Metrics}] ->
      UpdatedMetrics = update_metric_field(Metrics, Counter, Delta),
      ets:insert(?METRICS_TABLE, {global_metrics, UpdatedMetrics});
    [] ->
      init_metrics()
  end,
  ok.

%% @doc Update specific metric field
-spec update_metric_field(#metrics{}, atom(), integer()) -> #metrics{}.
update_metric_field(Metrics, total_transactions, Delta) ->
  Metrics#metrics{
    total_transactions = Metrics#metrics.total_transactions + Delta,
    last_updated = erlang:system_time(millisecond)
  };
update_metric_field(Metrics, successful_transactions, Delta) ->
  Metrics#metrics{
    successful_transactions = Metrics#metrics.successful_transactions + Delta,
    last_updated = erlang:system_time(millisecond)
  };
update_metric_field(Metrics, failed_transactions, Delta) ->
  Metrics#metrics{
    failed_transactions = Metrics#metrics.failed_transactions + Delta,
    last_updated = erlang:system_time(millisecond)
  };
update_metric_field(Metrics, aborted_transactions, Delta) ->
  Metrics#metrics{
    aborted_transactions = Metrics#metrics.aborted_transactions + Delta,
    last_updated = erlang:system_time(millisecond)
  };
update_metric_field(Metrics, total_retries, Delta) ->
  Metrics#metrics{
    total_retries = Metrics#metrics.total_retries + Delta,
    last_updated = erlang:system_time(millisecond)
  };
update_metric_field(Metrics, active_count, Delta) ->
  Metrics#metrics{
    active_count = max(0, Metrics#metrics.active_count + Delta),
    last_updated = erlang:system_time(millisecond)
  }.

%% @doc Calculate current metrics
-spec calculate_current_metrics() -> map().
calculate_current_metrics() ->
  case ets:lookup(?METRICS_TABLE, global_metrics) of
    [{global_metrics, Metrics}] ->
      metrics_to_map(Metrics);
    [] ->
      #{error => metrics_not_initialized}
  end.

%% @doc Convert metrics record to map
-spec metrics_to_map(#metrics{}) -> map().
metrics_to_map(#metrics{
  total_transactions = Total,
  successful_transactions = Successful,
  failed_transactions = Failed,
  aborted_transactions = Aborted,
  avg_duration_ms = AvgDuration,
  max_duration_ms = MaxDuration,
  min_duration_ms = MinDuration,
  total_retries = TotalRetries,
  active_count = ActiveCount,
  last_updated = LastUpdated
}) ->
  #{
    total_transactions => Total,
    successful_transactions => Successful,
    failed_transactions => Failed,
    aborted_transactions => Aborted,
    avg_duration_ms => AvgDuration,
    max_duration_ms => MaxDuration,
    min_duration_ms => MinDuration,
    total_retries => TotalRetries,
    active_count => ActiveCount,
    success_rate => calculate_success_rate(Successful, Total),
    last_updated => LastUpdated
  }.

%% @doc Calculate success rate
-spec calculate_success_rate(integer(), integer()) -> float().
calculate_success_rate(_Successful, 0) -> 0.0;
calculate_success_rate(Successful, Total) -> (Successful / Total) * 100.

%% @doc Update completion metrics
-spec update_completion_metrics(#transaction_info{}) -> ok.
update_completion_metrics(#transaction_info{state = State, retry_count = RetryCount}) ->
  case State of
    committed -> update_metrics_counter(successful_transactions, 1);
    aborted -> update_metrics_counter(aborted_transactions, 1);
    _ -> update_metrics_counter(failed_transactions, 1)
  end,
  
  if RetryCount > 0 ->
    update_metrics_counter(total_retries, RetryCount);
  true -> ok
  end.

%% @doc Add transaction to history
-spec add_to_history(#transaction_info{}) -> ok.
add_to_history(TxnInfo) ->
  Timestamp = erlang:system_time(millisecond),
  ets:insert(?HISTORY_TABLE, {Timestamp, TxnInfo}),
  ok.

%% @doc Get recent transaction history
-spec get_recent_history(integer()) -> list().
get_recent_history(Limit) ->
  AllHistory = ets:tab2list(?HISTORY_TABLE),
  SortedHistory = lists:reverse(lists:keysort(1, AllHistory)),
  LimitedHistory = lists:sublist(SortedHistory, Limit),
  [transaction_info_to_map(Info) || {_Timestamp, Info} <- LimitedHistory].

%% @doc Log transaction event
-spec log_transaction_event(atom(), #transaction_info{}, #state{}) -> ok.
log_transaction_event(Event, TxnInfo, #state{detailed_logging = true}) ->
  logger:info("Transaction ~p: ~p for transaction ~p", 
    [Event, TxnInfo#transaction_info.state, TxnInfo#transaction_info.id]);
log_transaction_event(_Event, _TxnInfo, _State) ->
  ok.

%% @doc Cleanup old history entries
-spec cleanup_old_history() -> ok.
cleanup_old_history() ->
  AllHistory = ets:tab2list(?HISTORY_TABLE),
  if length(AllHistory) > ?MAX_HISTORY_SIZE ->
    SortedHistory = lists:keysort(1, AllHistory),
    ToDelete = lists:sublist(SortedHistory, length(AllHistory) - ?MAX_HISTORY_SIZE),
    [ets:delete(?HISTORY_TABLE, Timestamp) || {Timestamp, _} <- ToDelete];
  true -> ok
  end.

%% @doc Cleanup stale transactions
-spec cleanup_stale_transactions() -> ok.
cleanup_stale_transactions() ->
  Now = erlang:system_time(millisecond),
  StaleThreshold = Now - 300000, % 5 minutes
  
  AllActive = ets:tab2list(?ACTIVE_TABLE),
  StaleTransactions = [TxnInfo || TxnInfo <- AllActive, 
    TxnInfo#transaction_info.start_time < StaleThreshold],
  
  [begin
    ets:delete(?ACTIVE_TABLE, TxnInfo#transaction_info.id),
    update_metrics_counter(active_count, -1)
  end || TxnInfo <- StaleTransactions],
  
  ok.

%% @doc Export metrics to file
-spec export_metrics_to_file(string()) -> ok | {error, term()}.
export_metrics_to_file(Filename) ->
  try
    Metrics = calculate_current_metrics(),
    ActiveTransactions = ets:tab2list(?ACTIVE_TABLE),
    History = get_recent_history(100),
    
    Data = #{
      metrics => Metrics,
      active_transactions => [transaction_info_to_map(Info) || Info <- ActiveTransactions],
      recent_history => History,
      export_time => erlang:system_time(millisecond)
    },
    
    Json = jsx:encode(Data),
    file:write_file(Filename, Json)
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Reset all metrics
-spec reset_all_metrics() -> ok.
reset_all_metrics() ->
  ets:delete_all_objects(?METRICS_TABLE),
  ets:delete_all_objects(?HISTORY_TABLE),
  init_metrics(),
  ok.
