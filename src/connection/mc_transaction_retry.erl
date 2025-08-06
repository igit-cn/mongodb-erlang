%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB transaction retry mechanism module.
%%% Implements intelligent retry strategies for different error types.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_transaction_retry).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  retry_transaction/4,
  should_retry_error/1,
  calculate_backoff/2,
  get_retry_strategy/1,
  execute_with_retry/3,
  is_transient_error/1,
  is_retryable_write_error/1,
  is_retryable_commit_error/1,
  get_max_retries/1,
  reset_retry_state/1
]).

-define(DEFAULT_MAX_RETRIES, 3).
-define(DEFAULT_BASE_BACKOFF, 100). % milliseconds
-define(DEFAULT_MAX_BACKOFF, 5000). % milliseconds
-define(JITTER_FACTOR, 0.1).

-record(retry_state, {
  attempt :: integer(),
  max_retries :: integer(),
  base_backoff :: integer(),
  max_backoff :: integer(),
  last_error :: term(),
  start_time :: integer()
}).

-type retry_state() :: #retry_state{}.
-type retry_strategy() :: exponential | linear | fixed | adaptive.
-type error_category() :: transient | network | write_conflict | commit_unknown | permanent.

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Retry transaction with intelligent strategy
-spec retry_transaction(fun(), retry_strategy(), map(), retry_state()) -> 
  {ok, term()} | {error, term()}.
retry_transaction(TransactionFun, Strategy, Options, RetryState) ->
  case should_continue_retry(RetryState) of
    true ->
      case execute_transaction_attempt(TransactionFun) of
        {ok, Result} ->
          {ok, Result};
        {error, Error} ->
          case should_retry_error(Error) of
            true ->
              NewRetryState = update_retry_state(RetryState, Error),
              Backoff = calculate_backoff(Strategy, NewRetryState),
              timer:sleep(Backoff),
              retry_transaction(TransactionFun, Strategy, Options, NewRetryState);
            false ->
              {error, {non_retryable_error, Error}}
          end
      end;
    false ->
      {error, {max_retries_exceeded, RetryState#retry_state.last_error}}
  end.

%% @doc Determine if an error should trigger a retry
-spec should_retry_error(term()) -> boolean().
should_retry_error(Error) ->
  ErrorCategory = categorize_error(Error),
  case ErrorCategory of
    transient -> true;
    network -> true;
    write_conflict -> true;
    commit_unknown -> true;
    permanent -> false
  end.

%% @doc Calculate backoff delay based on strategy
-spec calculate_backoff(retry_strategy(), retry_state()) -> integer().
calculate_backoff(exponential, #retry_state{attempt = Attempt, base_backoff = Base, max_backoff = Max}) ->
  Backoff = Base * round(math:pow(2, Attempt - 1)),
  add_jitter(min(Backoff, Max));

calculate_backoff(linear, #retry_state{attempt = Attempt, base_backoff = Base, max_backoff = Max}) ->
  Backoff = Base * Attempt,
  add_jitter(min(Backoff, Max));

calculate_backoff(fixed, #retry_state{base_backoff = Base}) ->
  add_jitter(Base);

calculate_backoff(adaptive, RetryState) ->
  calculate_adaptive_backoff(RetryState).

%% @doc Get retry strategy based on error type
-spec get_retry_strategy(term()) -> retry_strategy().
get_retry_strategy(Error) ->
  case categorize_error(Error) of
    transient -> exponential;
    network -> exponential;
    write_conflict -> linear;
    commit_unknown -> fixed;
    permanent -> fixed % Won't retry anyway
  end.

%% @doc Execute function with automatic retry
-spec execute_with_retry(fun(), map(), integer()) -> {ok, term()} | {error, term()}.
execute_with_retry(Fun, Options, MaxRetries) ->
  RetryState = init_retry_state(MaxRetries, Options),
  Strategy = maps:get(strategy, Options, exponential),
  retry_with_state(Fun, Strategy, Options, RetryState).

%% @doc Check if error is transient
-spec is_transient_error(term()) -> boolean().
is_transient_error(Error) ->
  categorize_error(Error) =:= transient.

%% @doc Check if error is retryable write error
-spec is_retryable_write_error(term()) -> boolean().
is_retryable_write_error(Error) ->
  case categorize_error(Error) of
    write_conflict -> true;
    transient -> true;
    network -> true;
    _ -> false
  end.

%% @doc Check if error is retryable commit error
-spec is_retryable_commit_error(term()) -> boolean().
is_retryable_commit_error(Error) ->
  case categorize_error(Error) of
    commit_unknown -> true;
    transient -> true;
    network -> true;
    _ -> false
  end.

%% @doc Get maximum retries for error type
-spec get_max_retries(term()) -> integer().
get_max_retries(Error) ->
  case categorize_error(Error) of
    transient -> 5;
    network -> 3;
    write_conflict -> 3;
    commit_unknown -> 3;
    permanent -> 0
  end.

%% @doc Reset retry state for new transaction
-spec reset_retry_state(retry_state()) -> retry_state().
reset_retry_state(RetryState) ->
  RetryState#retry_state{
    attempt = 0,
    last_error = undefined,
    start_time = erlang:system_time(millisecond)
  }.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Initialize retry state
-spec init_retry_state(integer(), map()) -> retry_state().
init_retry_state(MaxRetries, Options) ->
  #retry_state{
    attempt = 0,
    max_retries = MaxRetries,
    base_backoff = maps:get(base_backoff, Options, ?DEFAULT_BASE_BACKOFF),
    max_backoff = maps:get(max_backoff, Options, ?DEFAULT_MAX_BACKOFF),
    last_error = undefined,
    start_time = erlang:system_time(millisecond)
  }.

%% @doc Check if should continue retrying
-spec should_continue_retry(retry_state()) -> boolean().
should_continue_retry(#retry_state{attempt = Attempt, max_retries = MaxRetries}) ->
  Attempt < MaxRetries.

%% @doc Execute single transaction attempt
-spec execute_transaction_attempt(fun()) -> {ok, term()} | {error, term()}.
execute_transaction_attempt(TransactionFun) ->
  try
    Result = TransactionFun(),
    {ok, Result}
  catch
    Class:Reason:Stacktrace ->
      Error = {Class, Reason, Stacktrace},
      {error, Error}
  end.

%% @doc Update retry state after failed attempt
-spec update_retry_state(retry_state(), term()) -> retry_state().
update_retry_state(RetryState, Error) ->
  RetryState#retry_state{
    attempt = RetryState#retry_state.attempt + 1,
    last_error = Error
  }.

%% @doc Categorize error for retry decision
-spec categorize_error(term()) -> error_category().
categorize_error({error, #{<<"code">> := Code}}) ->
  categorize_by_error_code(Code);
categorize_error({error, #{<<"codeName">> := CodeName}}) ->
  categorize_by_error_name(CodeName);
categorize_error({error, {write_conflict, _}}) ->
  write_conflict;
categorize_error({error, {network_error, _}}) ->
  network;
categorize_error({error, timeout}) ->
  transient;
categorize_error({error, {connection_failed, _}}) ->
  network;
categorize_error({error, not_master}) ->
  transient;
categorize_error({error, {bad_query, {not_master, _}}}) ->
  transient;
categorize_error(_) ->
  permanent.

%% @doc Categorize error by MongoDB error code
-spec categorize_by_error_code(integer()) -> error_category().
categorize_by_error_code(Code) ->
  case Code of
    % Transient errors
    11600 -> transient; % InterruptedAtShutdown
    11602 -> transient; % InterruptedDueToReplStateChange
    10107 -> transient; % NotMaster
    13435 -> transient; % NotMasterNoSlaveOk
    13436 -> transient; % NotMasterOrSecondary
    189 -> transient;   % PrimarySteppedDown
    91 -> transient;    % ShutdownInProgress
    
    % Write conflicts
    112 -> write_conflict; % WriteConflict
    
    % Network errors
    89 -> network;      % NetworkTimeout
    6 -> network;       % HostUnreachable
    7 -> network;       % HostNotFound
    
    % Commit unknown
    50 -> commit_unknown; % MaxTimeMSExpired (on commit)
    
    % Default to permanent
    _ -> permanent
  end.

%% @doc Categorize error by MongoDB error name
-spec categorize_by_error_name(binary()) -> error_category().
categorize_by_error_name(CodeName) ->
  case CodeName of
    <<"InterruptedAtShutdown">> -> transient;
    <<"InterruptedDueToReplStateChange">> -> transient;
    <<"NotMaster">> -> transient;
    <<"NotMasterNoSlaveOk">> -> transient;
    <<"NotMasterOrSecondary">> -> transient;
    <<"PrimarySteppedDown">> -> transient;
    <<"ShutdownInProgress">> -> transient;
    <<"WriteConflict">> -> write_conflict;
    <<"NetworkTimeout">> -> network;
    <<"HostUnreachable">> -> network;
    <<"HostNotFound">> -> network;
    <<"MaxTimeMSExpired">> -> commit_unknown;
    <<"UnknownTransactionCommitResult">> -> commit_unknown;
    _ -> permanent
  end.

%% @doc Add jitter to backoff delay
-spec add_jitter(integer()) -> integer().
add_jitter(Backoff) ->
  Jitter = round(Backoff * ?JITTER_FACTOR * (rand:uniform() - 0.5)),
  max(0, Backoff + Jitter).

%% @doc Calculate adaptive backoff based on error history
-spec calculate_adaptive_backoff(retry_state()) -> integer().
calculate_adaptive_backoff(#retry_state{attempt = Attempt, last_error = Error, base_backoff = Base}) ->
  % Adjust backoff based on error type and attempt number
  ErrorMultiplier = case categorize_error(Error) of
    write_conflict -> 1.5; % Longer backoff for write conflicts
    network -> 2.0;        % Even longer for network issues
    transient -> 1.0;      % Standard backoff for transient errors
    _ -> 1.0
  end,
  
  Backoff = round(Base * ErrorMultiplier * math:pow(1.5, Attempt - 1)),
  add_jitter(min(Backoff, ?DEFAULT_MAX_BACKOFF)).

%% @doc Retry with state management
-spec retry_with_state(fun(), retry_strategy(), map(), retry_state()) -> 
  {ok, term()} | {error, term()}.
retry_with_state(Fun, Strategy, Options, RetryState) ->
  case should_continue_retry(RetryState) of
    true ->
      case execute_transaction_attempt(Fun) of
        {ok, Result} ->
          {ok, Result};
        {error, Error} ->
          case should_retry_error(Error) of
            true ->
              NewRetryState = update_retry_state(RetryState, Error),
              Backoff = calculate_backoff(Strategy, NewRetryState),
              timer:sleep(Backoff),
              retry_with_state(Fun, Strategy, Options, NewRetryState);
            false ->
              {error, {non_retryable_error, Error}}
          end
      end;
    false ->
      {error, {max_retries_exceeded, RetryState#retry_state.last_error}}
  end.
