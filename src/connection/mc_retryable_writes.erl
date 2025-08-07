%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB Retryable Writes implementation for MongoDB 4.x+
%%% Implements automatic retry for write operations according to MongoDB spec
%%% @end
%%%-------------------------------------------------------------------
-module(mc_retryable_writes).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  execute_retryable_write/3,
  execute_retryable_write/4,
  is_retryable_write_operation/1,
  is_retryable_write_error/1,
  should_retry_write/2,
  add_transaction_id/2,
  get_retry_write_options/1,
  execute_with_retry/4
]).

%% Types
-type retryable_write_options() :: #{
  retry_writes => boolean(),
  max_retries => integer(),
  retry_delay_ms => integer()
}.

-type write_operation() :: insert | update | delete | findAndModify | bulkWrite.

-export_type([retryable_write_options/0, write_operation/0]).

-define(DEFAULT_MAX_RETRIES, 1).
-define(DEFAULT_RETRY_DELAY_MS, 100).
-define(RETRYABLE_WRITE_ERROR_CODES, [
  6,      % HostUnreachable
  7,      % HostNotFound
  89,     % NetworkTimeout
  91,     % ShutdownInProgress
  189,    % PrimarySteppedDown
  9001,   % SocketException
  10107,  % NotMaster
  11600,  % InterruptedAtShutdown
  11602,  % InterruptedDueToReplStateChange
  13435,  % NotMasterNoSlaveOk
  13436,  % NotMasterOrSecondary
  63,     % StaleShardVersion
  150,    % StaleEpoch
  13388,  % StaleConfig
  234,    % RetryChangeStream
  133     % FailedToSatisfyReadPreference
]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Execute a retryable write operation
-spec execute_retryable_write(pid(), fun(), map()) -> {ok, term()} | {error, term()}.
execute_retryable_write(Connection, WriteFun, Command) ->
  execute_retryable_write(Connection, WriteFun, Command, #{}).

-spec execute_retryable_write(pid(), fun(), map(), retryable_write_options()) -> {ok, term()} | {error, term()}.
execute_retryable_write(Connection, WriteFun, Command, Options) ->
  case should_enable_retryable_writes(Connection, Command, Options) of
    true ->
      execute_with_retry(Connection, WriteFun, Command, Options);
    false ->
      WriteFun(Command)
  end.

%% @doc Check if operation is retryable write
-spec is_retryable_write_operation(map()) -> boolean().
is_retryable_write_operation(Command) ->
  case get_command_name(Command) of
    <<"insert">> -> true;
    <<"update">> -> not has_multi_update(Command);
    <<"delete">> -> not has_multi_delete(Command);
    <<"findAndModify">> -> true;
    <<"bulkWrite">> -> all_single_writes(Command);
    _ -> false
  end.

%% @doc Check if error is retryable for writes
-spec is_retryable_write_error(term()) -> boolean().
is_retryable_write_error({error, #{<<"code">> := Code}}) ->
  lists:member(Code, ?RETRYABLE_WRITE_ERROR_CODES);
is_retryable_write_error({error, #{<<"codeName">> := <<"RetryableWriteError">>}}) ->
  true;
is_retryable_write_error({error, #{<<"errmsg">> := ErrMsg}}) ->
  is_retryable_error_message(ErrMsg);
is_retryable_write_error(_) ->
  false.

%% @doc Determine if write should be retried
-spec should_retry_write(term(), integer()) -> boolean().
should_retry_write(Error, RetryCount) ->
  RetryCount < ?DEFAULT_MAX_RETRIES andalso is_retryable_write_error(Error).

%% @doc Add transaction ID to command for retryable writes
-spec add_transaction_id(map(), binary()) -> map().
add_transaction_id(Command, SessionId) ->
  TxnNumber = generate_transaction_number(),
  Command#{
    <<"lsid">> => SessionId,
    <<"txnNumber">> => TxnNumber
  }.

%% @doc Get retry write options with defaults
-spec get_retry_write_options(map()) -> retryable_write_options().
get_retry_write_options(Options) ->
  #{
    retry_writes => maps:get(retry_writes, Options, true),
    max_retries => maps:get(max_retries, Options, ?DEFAULT_MAX_RETRIES),
    retry_delay_ms => maps:get(retry_delay_ms, Options, ?DEFAULT_RETRY_DELAY_MS)
  }.

%% @doc Execute write with retry logic
-spec execute_with_retry(pid(), fun(), map(), retryable_write_options()) -> {ok, term()} | {error, term()}.
execute_with_retry(Connection, WriteFun, Command, Options) ->
  case maps:get(enhanced_retry, Options, true) of
    true ->
      execute_with_retry_enhanced(Connection, WriteFun, Command, Options);
    false ->
      % Fallback to simple retry logic
      MaxRetries = maps:get(max_retries, Options, ?DEFAULT_MAX_RETRIES),
      RetryDelay = maps:get(retry_delay_ms, Options, ?DEFAULT_RETRY_DELAY_MS),

      % Add session and transaction number for retryable writes
      EnhancedCommand = case get_or_create_session(Connection) of
        {ok, SessionId} ->
          add_transaction_id(Command, SessionId);
        _ ->
          Command
      end,

      execute_with_retry_loop(WriteFun, EnhancedCommand, 0, MaxRetries, RetryDelay)
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
should_enable_retryable_writes(Connection, Command, Options) ->
  case maps:get(retry_writes, Options, true) of
    false -> false;
    true ->
      is_retryable_write_operation(Command) andalso
      supports_retryable_writes(Connection) andalso
      not in_transaction(Connection)
  end.

%% @private
supports_retryable_writes(Connection) ->
  try
    case mc_worker_api:command(Connection, #{<<"isMaster">> => 1}) of
      {true, #{<<"logicalSessionTimeoutMinutes">> := _}} -> true;
      _ -> false
    end
  catch
    _:_ -> false
  end.

%% @private
in_transaction(_Connection) ->
  % TODO: Check if connection is in transaction
  false.

%% @private
get_command_name(Command) when is_map(Command) ->
  case maps:keys(Command) of
    [FirstKey | _] -> FirstKey;
    [] -> undefined
  end;
get_command_name(_) ->
  undefined.

%% @private
has_multi_update(#{<<"updates">> := Updates}) ->
  lists:any(fun(Update) ->
    maps:get(<<"multi">>, Update, false)
  end, Updates);
has_multi_update(_) ->
  false.

%% @private
has_multi_delete(#{<<"deletes">> := Deletes}) ->
  lists:any(fun(Delete) ->
    maps:get(<<"limit">>, Delete, 1) =:= 0
  end, Deletes);
has_multi_delete(_) ->
  false.

%% @private
all_single_writes(#{<<"ops">> := Ops}) ->
  lists:all(fun(Op) ->
    case maps:get(<<"type">>, Op) of
      <<"update">> -> not maps:get(<<"multi">>, Op, false);
      <<"delete">> -> maps:get(<<"limit">>, Op, 1) =/= 0;
      _ -> true
    end
  end, Ops);
all_single_writes(_) ->
  false.

%% @private
is_retryable_error_message(ErrMsg) ->
  RetryableMessages = [
    <<"not master">>,
    <<"node is recovering">>,
    <<"not master and slaveok=false">>,
    <<"not master or secondary">>,
    <<"connection closed">>,
    <<"socket exception">>,
    <<"network timeout">>
  ],
  
  ErrMsgLower = string:lowercase(binary_to_list(ErrMsg)),
  lists:any(fun(Pattern) ->
    PatternLower = string:lowercase(binary_to_list(Pattern)),
    string:find(ErrMsgLower, PatternLower) =/= nomatch
  end, RetryableMessages).

%% @private
generate_transaction_number() ->
  erlang:system_time(microsecond).

%% @private
get_or_create_session(Connection) ->
  try
    case mc_worker_api:start_session(Connection) of
      {ok, SessionId} -> {ok, SessionId};
      Error -> Error
    end
  catch
    _:_ -> {error, session_creation_failed}
  end.

%% @private
execute_with_retry_loop(WriteFun, Command, RetryCount, MaxRetries, RetryDelay) ->
  try
    case WriteFun(Command) of
      {ok, Result} ->
        {ok, Result};
      {true, Result} ->
        {ok, Result};
      Error ->
        case should_retry_write(Error, RetryCount) andalso RetryCount < MaxRetries of
          true ->
            timer:sleep(RetryDelay),
            execute_with_retry_loop(WriteFun, Command, RetryCount + 1, MaxRetries, RetryDelay);
          false ->
            Error
        end
    end
  catch
    Class:Reason:Stacktrace ->
      CaughtError = {error, {Class, Reason, Stacktrace}},
      case should_retry_write(CaughtError, RetryCount) andalso RetryCount < MaxRetries of
        true ->
          timer:sleep(RetryDelay),
          execute_with_retry_loop(WriteFun, Command, RetryCount + 1, MaxRetries, RetryDelay);
        false ->
          CaughtError
      end
  end.

%% @private
format_retry_error(OriginalError, RetryCount) ->
  #{
    <<"error">> => OriginalError,
    <<"retryCount">> => RetryCount,
    <<"message">> => <<"Write operation failed after retries">>
  }.

%% @doc Check if server supports retryable writes
-spec supports_retryable_writes_detailed(pid()) -> {boolean(), map()}.
supports_retryable_writes_detailed(Connection) ->
  try
    case mc_worker_api:command(Connection, #{<<"isMaster">> => 1}) of
      {true, #{<<"logicalSessionTimeoutMinutes">> := Timeout} = Result} ->
        Features = #{
          sessions_supported => true,
          logical_session_timeout => Timeout,
          retryable_writes => maps:get(<<"retryableWrites">>, Result, true),
          wire_version => maps:get(<<"maxWireVersion">>, Result, 0)
        },
        {true, Features};
      {true, Result} ->
        Features = #{
          sessions_supported => false,
          retryable_writes => false,
          wire_version => maps:get(<<"maxWireVersion">>, Result, 0)
        },
        {false, Features};
      _ ->
        {false, #{error => <<"Failed to get server info">>}}
    end
  catch
    _:Reason ->
      {false, #{error => Reason}}
  end.

%% @doc Enhanced error categorization for retryable writes
-spec categorize_retryable_error(term()) -> {retryable | non_retryable, atom()}.
categorize_retryable_error({error, #{<<"code">> := Code}}) ->
  case lists:member(Code, ?RETRYABLE_WRITE_ERROR_CODES) of
    true -> {retryable, error_code};
    false -> {non_retryable, non_retryable_error_code}
  end;
categorize_retryable_error({error, #{<<"codeName">> := <<"RetryableWriteError">>}}) ->
  {retryable, retryable_write_error};
categorize_retryable_error({error, #{<<"codeName">> := CodeName}}) ->
  case is_retryable_code_name(CodeName) of
    true -> {retryable, code_name};
    false -> {non_retryable, non_retryable_code_name}
  end;
categorize_retryable_error({error, #{<<"errmsg">> := ErrMsg}}) ->
  case is_retryable_error_message(ErrMsg) of
    true -> {retryable, error_message};
    false -> {non_retryable, non_retryable_message}
  end;
categorize_retryable_error({error, timeout}) ->
  {retryable, timeout};
categorize_retryable_error({error, {socket_error, _}}) ->
  {retryable, socket_error};
categorize_retryable_error({error, {connection_error, _}}) ->
  {retryable, connection_error};
categorize_retryable_error(_) ->
  {non_retryable, unknown_error}.

%% @private
is_retryable_code_name(<<"HostUnreachable">>) -> true;
is_retryable_code_name(<<"HostNotFound">>) -> true;
is_retryable_code_name(<<"NetworkTimeout">>) -> true;
is_retryable_code_name(<<"ShutdownInProgress">>) -> true;
is_retryable_code_name(<<"PrimarySteppedDown">>) -> true;
is_retryable_code_name(<<"NotMaster">>) -> true;
is_retryable_code_name(<<"NotMasterNoSlaveOk">>) -> true;
is_retryable_code_name(<<"NotMasterOrSecondary">>) -> true;
is_retryable_code_name(<<"InterruptedAtShutdown">>) -> true;
is_retryable_code_name(<<"InterruptedDueToReplStateChange">>) -> true;
is_retryable_code_name(<<"StaleShardVersion">>) -> true;
is_retryable_code_name(<<"StaleEpoch">>) -> true;
is_retryable_code_name(<<"StaleConfig">>) -> true;
is_retryable_code_name(<<"FailedToSatisfyReadPreference">>) -> true;
is_retryable_code_name(_) -> false.

%% @doc Execute write with enhanced retry logic and monitoring
-spec execute_with_retry_enhanced(pid(), fun(), map(), retryable_write_options()) ->
  {ok, term()} | {error, term()}.
execute_with_retry_enhanced(Connection, WriteFun, Command, Options) ->
  MaxRetries = maps:get(max_retries, Options, ?DEFAULT_MAX_RETRIES),
  RetryDelay = maps:get(retry_delay_ms, Options, ?DEFAULT_RETRY_DELAY_MS),

  % Check server capabilities
  case supports_retryable_writes_detailed(Connection) of
    {true, ServerFeatures} ->
      % Add session and transaction number for retryable writes
      EnhancedCommand = case get_or_create_session(Connection) of
        {ok, SessionId} ->
          add_transaction_id(Command, SessionId);
        _ ->
          Command
      end,

      RetryState = #{
        attempt => 0,
        max_retries => MaxRetries,
        retry_delay => RetryDelay,
        server_features => ServerFeatures,
        start_time => erlang:system_time(millisecond),
        errors => []
      },

      execute_with_retry_loop_enhanced(WriteFun, EnhancedCommand, RetryState);

    {false, ServerInfo} ->
      % Server doesn't support retryable writes, execute once
      case WriteFun(Command) of
        {ok, Result} -> {ok, Result};
        {true, Result} -> {ok, Result};
        Error ->
          {error, #{
            original_error => Error,
            reason => <<"Server does not support retryable writes">>,
            server_info => ServerInfo
          }}
      end
  end.

%% @private
execute_with_retry_loop_enhanced(WriteFun, Command, RetryState) ->
  try
    case WriteFun(Command) of
      {ok, Result} ->
        log_retry_success(RetryState),
        {ok, Result};
      {true, Result} ->
        log_retry_success(RetryState),
        {ok, Result};
      Error ->
        handle_retry_error(Error, WriteFun, Command, RetryState)
    end
  catch
    Class:Reason:Stacktrace ->
      CaughtError = {error, {Class, Reason, Stacktrace}},
      handle_retry_error(CaughtError, WriteFun, Command, RetryState)
  end.

%% @private
handle_retry_error(Error, WriteFun, Command, RetryState) ->
  #{attempt := Attempt, max_retries := MaxRetries, retry_delay := RetryDelay,
    errors := PreviousErrors} = RetryState,

  {Retryable, ErrorType} = categorize_retryable_error(Error),
  UpdatedErrors = [#{error => Error, type => ErrorType, attempt => Attempt} | PreviousErrors],

  case Retryable =:= retryable andalso Attempt < MaxRetries of
    true ->
      log_retry_attempt(Error, Attempt + 1, MaxRetries),
      timer:sleep(calculate_backoff_delay(RetryDelay, Attempt)),

      NewRetryState = RetryState#{
        attempt => Attempt + 1,
        errors => UpdatedErrors
      },

      execute_with_retry_loop_enhanced(WriteFun, Command, NewRetryState);

    false ->
      log_retry_failure(Error, Attempt, UpdatedErrors),
      {error, #{
        final_error => Error,
        retry_attempts => Attempt,
        all_errors => lists:reverse(UpdatedErrors),
        reason => case Retryable of
          retryable -> <<"Maximum retries exceeded">>;
          non_retryable -> <<"Non-retryable error encountered">>
        end
      }}
  end.

%% @private
calculate_backoff_delay(BaseDelay, Attempt) ->
  % Exponential backoff with jitter
  BackoffDelay = BaseDelay * round(math:pow(2, Attempt)),
  Jitter = rand:uniform(round(BackoffDelay * 0.1)),
  min(BackoffDelay + Jitter, 5000). % Cap at 5 seconds

%% @private
log_retry_attempt(Error, Attempt, MaxRetries) ->
  error_logger:warning_msg(
    "Retryable write failed, attempting retry ~p/~p. Error: ~p~n",
    [Attempt, MaxRetries, Error]).

%% @private
log_retry_success(#{attempt := Attempt}) when Attempt > 0 ->
  error_logger:info_msg("Retryable write succeeded after ~p attempts~n", [Attempt]);
log_retry_success(_) ->
  ok.

%% @private
log_retry_failure(FinalError, Attempts, AllErrors) ->
  error_logger:error_msg(
    "Retryable write failed permanently after ~p attempts. Final error: ~p. All errors: ~p~n",
    [Attempts, FinalError, AllErrors]).
