%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB transaction management module.
%%% Handles transaction lifecycle: start, commit, abort.
%%% Supports single node, replica set and sharded cluster deployments.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_transaction).
-author("mongodb-erlang team").

-behaviour(gen_server).

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  start_transaction/2,
  start_transaction/3,
  commit_transaction/1,
  abort_transaction/1,
  get_transaction_state/1,
  is_in_transaction/1,
  with_transaction/3,
  with_transaction/4,
  get_worker/1,
  get_session_options/1,
  get_transaction_options/1,
  get_start_time/1
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

-define(DEFAULT_TRANSACTION_TIMEOUT, 60000). % 60 seconds
-define(MAX_COMMIT_RETRY, 3).

-record(transaction_state_record, {
  session_pid :: pid(),
  worker :: pid(),
  state :: undefined | starting | in_progress | committed | aborted,
  options :: transaction_options(),
  transaction_number :: integer(),
  start_time :: integer(),
  timeout :: integer(),
  retry_count :: integer(),
  timer_ref :: undefined | timer:tref()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Start a transaction with default options
-spec start_transaction(pid(), pid()) -> {ok, pid()} | {error, term()}.
start_transaction(SessionPid, Worker) ->
  start_transaction(SessionPid, Worker, #transaction_options{}).

%% @doc Start a transaction with specified options
-spec start_transaction(pid(), pid(), transaction_options()) -> {ok, pid()} | {error, term()}.
start_transaction(SessionPid, Worker, Options) ->
  case mc_session:is_session_valid(SessionPid) of
    true ->
      gen_server:start_link(?MODULE, [SessionPid, Worker, Options], []);
    false ->
      {error, invalid_session}
  end.

%% @doc Commit the transaction
-spec commit_transaction(pid()) -> ok | {error, term()}.
commit_transaction(TransactionPid) ->
  gen_server:call(TransactionPid, commit_transaction, ?DEFAULT_TRANSACTION_TIMEOUT).

%% @doc Abort the transaction
-spec abort_transaction(pid()) -> ok | {error, term()}.
abort_transaction(TransactionPid) ->
  gen_server:call(TransactionPid, abort_transaction, ?DEFAULT_TRANSACTION_TIMEOUT).

%% @doc Get current transaction state
-spec get_transaction_state(pid()) -> {ok, atom()} | {error, term()}.
get_transaction_state(TransactionPid) ->
  gen_server:call(TransactionPid, get_state).

%% @doc Check if transaction is active
-spec is_in_transaction(pid()) -> boolean().
is_in_transaction(TransactionPid) ->
  try
    case get_transaction_state(TransactionPid) of
      {ok, State} when State =:= starting; State =:= in_progress ->
        true;
      _ ->
        false
    end
  catch
    _:_ -> false
  end.

%% @doc Get worker from transaction
-spec get_worker(pid()) -> {ok, pid()} | {error, term()}.
get_worker(TransactionPid) ->
  gen_server:call(TransactionPid, get_worker).

%% @doc Get session options for commands from transaction
-spec get_session_options(pid()) -> map().
get_session_options(TransactionPid) ->
  gen_server:call(TransactionPid, get_session_options).

%% @doc Get transaction options
-spec get_transaction_options(pid()) -> {ok, transaction_options()} | {error, term()}.
get_transaction_options(TransactionPid) ->
  gen_server:call(TransactionPid, get_transaction_options).

%% @doc Get transaction start time
-spec get_start_time(pid()) -> integer() | undefined.
get_start_time(TransactionPid) ->
  gen_server:call(TransactionPid, get_start_time).

%% @doc Execute a function within a transaction with automatic retry
-spec with_transaction(pid(), pid(), fun()) -> {ok, term()} | {error, term()}.
with_transaction(SessionPid, Worker, Fun) ->
  with_transaction(SessionPid, Worker, Fun, #transaction_options{}).

%% @doc Execute a function within a transaction with options and automatic retry
-spec with_transaction(pid(), pid(), fun(), transaction_options()) -> {ok, term()} | {error, term()}.
with_transaction(SessionPid, Worker, Fun, Options) ->
  case start_transaction(SessionPid, Worker, Options) of
    {ok, TransactionPid} ->
      try
        Result = Fun(TransactionPid),
        case commit_transaction(TransactionPid) of
          ok -> {ok, Result};
          {error, CommitReason} -> {error, CommitReason}
        end
      catch
        Class:CatchReason:Stacktrace ->
          abort_transaction(TransactionPid),
          erlang:raise(Class, CatchReason, Stacktrace)
      end;
    Error ->
      Error
  end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([SessionPid, Worker, Options]) ->
  process_flag(trap_exit, true),
  
  Timeout = case Options#transaction_options.max_commit_time_ms of
    undefined -> ?DEFAULT_TRANSACTION_TIMEOUT;
    T -> T
  end,
  
  State = #transaction_state_record{
    session_pid = SessionPid,
    worker = Worker,
    state = undefined,
    options = Options,
    transaction_number = 1,
    start_time = erlang:system_time(millisecond),
    timeout = Timeout,
    retry_count = 0,
    timer_ref = undefined
  },
  
  % Start the transaction
  case start_server_transaction(State) of
    {ok, NewState} ->
      {ok, TimerRef} = timer:send_after(Timeout, transaction_timeout),
      FinalState = NewState#transaction_state_record{timer_ref = TimerRef},
      {ok, FinalState};
    {error, Reason} ->
      {stop, Reason}
  end.

handle_call(commit_transaction, _From, State = #transaction_state_record{state = TxnState}) ->
  case TxnState of
    in_progress ->
      case commit_server_transaction(State) of
        {ok, NewState} ->
          {stop, normal, ok, NewState};
        {error, Reason} ->
          {reply, {error, Reason}, State}
      end;
    committed ->
      {reply, ok, State};
    aborted ->
      {reply, {error, transaction_aborted}, State};
    _ ->
      {reply, {error, invalid_transaction_state}, State}
  end;

handle_call(abort_transaction, _From, State = #transaction_state_record{state = TxnState}) ->
  case TxnState of
    starting ->
      NewState = State#transaction_state_record{state = aborted},
      {stop, normal, ok, NewState};
    in_progress ->
      case abort_server_transaction(State) of
        {ok, NewState} ->
          {stop, normal, ok, NewState};
        {error, Reason} ->
          {reply, {error, Reason}, State}
      end;
    aborted ->
      {reply, ok, State};
    committed ->
      {reply, {error, transaction_committed}, State};
    _ ->
      {reply, {error, invalid_transaction_state}, State}
  end;

handle_call(get_state, _From, State = #transaction_state_record{state = TxnState}) ->
  {reply, {ok, TxnState}, State};

handle_call(get_worker, _From, State = #transaction_state_record{worker = Worker}) ->
  {reply, {ok, Worker}, State};

handle_call(get_session_options, _From, State = #transaction_state_record{session_pid = SessionPid}) ->
  try
    Options = mc_session:get_session_options(SessionPid),
    {reply, Options, State}
  catch
    _:Reason ->
      {reply, {error, Reason}, State}
  end;

handle_call(get_transaction_options, _From, State = #transaction_state_record{options = Options}) ->
  {reply, {ok, Options}, State};

handle_call(get_start_time, _From, State = #transaction_state_record{start_time = StartTime}) ->
  {reply, StartTime, State};

handle_call(_Request, _From, State) ->
  {reply, {error, unknown_request}, State}.

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info(transaction_timeout, State) ->
  abort_server_transaction(State),
  {stop, transaction_timeout, State};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, #transaction_state_record{timer_ref = TimerRef, state = TxnState} = State) ->
  case TimerRef of
    undefined -> ok;
    _ -> timer:cancel(TimerRef)
  end,
  
  % Auto-abort if transaction is still active
  case TxnState of
    starting -> abort_server_transaction(State);
    in_progress -> abort_server_transaction(State);
    _ -> ok
  end,
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Start transaction on MongoDB server
-spec start_server_transaction(#transaction_state_record{}) -> {ok, #transaction_state_record{}} | {error, term()}.
start_server_transaction(State = #transaction_state_record{
  session_pid = SessionPid,
  worker = Worker,
  options = Options,
  transaction_number = TxnNumber
}) ->
  try
    SessionOptions = mc_session:get_session_options(SessionPid),
    
    Command = build_start_transaction_command(SessionOptions, Options, TxnNumber),
    
    case mc_worker_api:command(Worker, Command) of
      {true, _Result} ->
        NewState = State#transaction_state_record{state = in_progress},
        {ok, NewState};
      {false, Error} ->
        {error, Error};
      Error ->
        {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Commit transaction on MongoDB server
-spec commit_server_transaction(#transaction_state_record{}) -> {ok, #transaction_state_record{}} | {error, term()}.
commit_server_transaction(State = #transaction_state_record{
  session_pid = SessionPid,
  worker = Worker,
  transaction_number = TxnNumber
}) ->
  try
    SessionOptions = mc_session:get_session_options(SessionPid),
    
    Command = build_commit_transaction_command(SessionOptions, TxnNumber),
    
    case mc_worker_api:command(Worker, Command) of
      {true, _Result} ->
        NewState = State#transaction_state_record{state = committed},
        {ok, NewState};
      {false, Error} ->
        case should_retry_commit(Error) of
          true ->
            retry_commit_transaction(State);
          false ->
            {error, Error}
        end;
      Error ->
        {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Abort transaction on MongoDB server
-spec abort_server_transaction(#transaction_state_record{}) -> {ok, #transaction_state_record{}} | {error, term()}.
abort_server_transaction(State = #transaction_state_record{
  session_pid = SessionPid,
  worker = Worker,
  transaction_number = TxnNumber
}) ->
  AbortedState = State#transaction_state_record{state = aborted},
  try
    SessionOptions = mc_session:get_session_options(SessionPid),

    Command = build_abort_transaction_command(SessionOptions, TxnNumber),

    case mc_worker_api:command(Worker, Command) of
      {true, _Result} ->
        {ok, AbortedState};
      {false, _Error} ->
        % Even if abort fails, consider transaction aborted
        {ok, AbortedState};
      _Error ->
        {ok, AbortedState}
    end
  catch
    _:_ ->
      {ok, AbortedState}
  end.

%% @doc Build startTransaction command
-spec build_start_transaction_command(map(), transaction_options(), integer()) -> bson:document().
build_start_transaction_command(SessionOptions, Options, TxnNumber) ->
  BaseCommand = [
    {<<"startTransaction">>, 1},
    {<<"txnNumber">>, TxnNumber},
    {<<"autocommit">>, false}
  ],
  
  Command1 = case Options#transaction_options.read_concern of
    undefined -> BaseCommand;
    ReadConcern -> BaseCommand ++ [{<<"readConcern">>, ReadConcern}]
  end,
  
  Command2 = case Options#transaction_options.write_concern of
    undefined -> Command1;
    WriteConcern -> Command1 ++ [{<<"writeConcern">>, WriteConcern}]
  end,
  
  Command3 = case Options#transaction_options.max_commit_time_ms of
    undefined -> Command2;
    MaxTime -> Command2 ++ [{<<"maxTimeMS">>, MaxTime}]
  end,
  
  merge_session_options(Command3, SessionOptions).

%% @doc Build commitTransaction command
-spec build_commit_transaction_command(map(), integer()) -> bson:document().
build_commit_transaction_command(SessionOptions, TxnNumber) ->
  Command = [
    {<<"commitTransaction">>, 1},
    {<<"txnNumber">>, TxnNumber}
  ],
  merge_session_options(Command, SessionOptions).

%% @doc Build abortTransaction command
-spec build_abort_transaction_command(map(), integer()) -> bson:document().
build_abort_transaction_command(SessionOptions, TxnNumber) ->
  Command = [
    {<<"abortTransaction">>, 1},
    {<<"txnNumber">>, TxnNumber}
  ],
  merge_session_options(Command, SessionOptions).

%% @doc Merge session options into command
-spec merge_session_options(bson:document(), map()) -> bson:document().
merge_session_options(Command, SessionOptions) ->
  maps:fold(
    fun(Key, Value, Acc) ->
      Acc ++ [{Key, Value}]
    end,
    Command,
    SessionOptions
  ).

%% @doc Check if commit should be retried
-spec should_retry_commit(term()) -> boolean().
should_retry_commit(#{<<"code">> := Code}) when Code =:= 50; Code =:= 91 ->
  true; % MaxTimeMSExpired, ShutdownInProgress
should_retry_commit(#{<<"codeName">> := <<"UnknownTransactionCommitResult">>}) ->
  true;
should_retry_commit(_) ->
  false.

%% @doc Retry commit transaction
-spec retry_commit_transaction(#transaction_state_record{}) -> {ok, #transaction_state_record{}} | {error, term()}.
retry_commit_transaction(State = #transaction_state_record{retry_count = RetryCount}) ->
  case RetryCount < ?MAX_COMMIT_RETRY of
    true ->
      NewState = State#transaction_state_record{retry_count = RetryCount + 1},
      timer:sleep(100 * (RetryCount + 1)), % Exponential backoff
      commit_server_transaction(NewState);
    false ->
      {error, max_commit_retries_exceeded}
  end.
