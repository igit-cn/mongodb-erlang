%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB Change Streams implementation for MongoDB 4.x+
%%% Provides watch operations, resume tokens, and event handling
%%% @end
%%%-------------------------------------------------------------------
-module(mc_change_stream).
-author("mongodb-erlang team").

-behaviour(gen_server).

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  watch/2, watch/3, watch/4,
  watch_collection/3, watch_collection/4, watch_collection/5,
  watch_database/3, watch_database/4,
  next/1, next/2,
  close/1,
  get_resume_token/1,
  resume_after/2,
  start_after/2
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

%% Types
-type change_stream_options() :: #{
  full_document => binary(),           % "default" | "updateLookup"
  full_document_before_change => binary(), % "off" | "whenAvailable" | "required"
  resume_after => map(),               % Resume token
  start_after => map(),                % Start after token
  start_at_operation_time => bson:timestamp(),
  max_await_time_ms => integer(),
  batch_size => integer(),
  collation => map(),
  read_concern => map(),
  read_preference => map()
}.

-type change_event() :: #{
  '_id' := map(),                      % Resume token
  'operationType' := binary(),         % insert, update, delete, etc.
  'clusterTime' := bson:timestamp(),
  'fullDocument' => map(),
  'fullDocumentBeforeChange' => map(),
  'ns' := #{
    'db' := binary(),
    'coll' := binary()
  },
  'documentKey' := map(),
  'updateDescription' => #{
    'updatedFields' => map(),
    'removedFields' => [binary()]
  }
}.

-export_type([change_stream_options/0, change_event/0]).

-record(change_stream_state, {
  connection :: pid(),
  cursor :: pid() | undefined,
  pipeline :: [map()],
  options :: change_stream_options(),
  target :: database | collection | deployment,
  target_spec :: {binary()} | {binary(), binary()} | deployment,
  resume_token :: map() | undefined,
  last_operation_time :: bson:timestamp() | undefined,
  closed = false :: boolean()
}).

-define(DEFAULT_BATCH_SIZE, 100).
-define(DEFAULT_MAX_AWAIT_TIME_MS, 1000).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Watch changes on entire deployment
-spec watch(pid(), [map()]) -> {ok, pid()} | {error, term()}.
watch(Connection, Pipeline) ->
  watch(Connection, Pipeline, #{}).

-spec watch(pid(), [map()], change_stream_options()) -> {ok, pid()} | {error, term()}.
watch(Connection, Pipeline, Options) ->
  gen_server:start_link(?MODULE, [Connection, deployment, deployment, Pipeline, Options], []).

-spec watch(pid(), [map()], change_stream_options(), timeout()) -> {ok, pid()} | {error, term()}.
watch(Connection, Pipeline, Options, Timeout) ->
  gen_server:start_link(?MODULE, [Connection, deployment, deployment, Pipeline, Options], [{timeout, Timeout}]).

%% @doc Watch changes on specific collection
-spec watch_collection(pid(), binary(), binary()) -> {ok, pid()} | {error, term()}.
watch_collection(Connection, Database, Collection) ->
  watch_collection(Connection, Database, Collection, []).

-spec watch_collection(pid(), binary(), binary(), [map()]) -> {ok, pid()} | {error, term()}.
watch_collection(Connection, Database, Collection, Pipeline) ->
  watch_collection(Connection, Database, Collection, Pipeline, #{}).

-spec watch_collection(pid(), binary(), binary(), [map()], change_stream_options()) -> {ok, pid()} | {error, term()}.
watch_collection(Connection, Database, Collection, Pipeline, Options) ->
  gen_server:start_link(?MODULE, [Connection, collection, {Database, Collection}, Pipeline, Options], []).

%% @doc Watch changes on specific database
-spec watch_database(pid(), binary(), [map()]) -> {ok, pid()} | {error, term()}.
watch_database(Connection, Database, Pipeline) ->
  watch_database(Connection, Database, Pipeline, #{}).

-spec watch_database(pid(), binary(), [map()], change_stream_options()) -> {ok, pid()} | {error, term()}.
watch_database(Connection, Database, Pipeline, Options) ->
  gen_server:start_link(?MODULE, [Connection, database, {Database}, Pipeline, Options], []).

%% @doc Get next change event
-spec next(pid()) -> {ok, change_event()} | {error, term()} | timeout.
next(ChangeStream) ->
  next(ChangeStream, infinity).

-spec next(pid(), timeout()) -> {ok, change_event()} | {error, term()} | timeout.
next(ChangeStream, Timeout) ->
  gen_server:call(ChangeStream, next, Timeout).

%% @doc Close change stream
-spec close(pid()) -> ok.
close(ChangeStream) ->
  gen_server:cast(ChangeStream, close).

%% @doc Get current resume token
-spec get_resume_token(pid()) -> {ok, map()} | {error, term()}.
get_resume_token(ChangeStream) ->
  gen_server:call(ChangeStream, get_resume_token).

%% @doc Resume change stream after specific token
-spec resume_after(pid(), map()) -> ok | {error, term()}.
resume_after(ChangeStream, ResumeToken) ->
  gen_server:call(ChangeStream, {resume_after, ResumeToken}).

%% @doc Start change stream after specific token
-spec start_after(pid(), map()) -> ok | {error, term()}.
start_after(ChangeStream, StartToken) ->
  gen_server:call(ChangeStream, {start_after, StartToken}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([Connection, Target, TargetSpec, Pipeline, Options]) ->
  process_flag(trap_exit, true),
  
  State = #change_stream_state{
    connection = Connection,
    pipeline = Pipeline,
    options = Options,
    target = Target,
    target_spec = TargetSpec
  },
  
  case start_change_stream(State) of
    {ok, NewState} ->
      {ok, NewState};
    {error, Reason} ->
      {stop, Reason}
  end.

handle_call(next, _From, #change_stream_state{closed = true} = State) ->
  {reply, {error, closed}, State};

handle_call(next, _From, #change_stream_state{cursor = undefined} = State) ->
  case start_change_stream(State) of
    {ok, NewState} ->
      get_next_event(NewState);
    {error, Reason} ->
      {reply, {error, Reason}, State}
  end;

handle_call(next, _From, State) ->
  get_next_event(State);

handle_call(get_resume_token, _From, State) ->
  {reply, {ok, State#change_stream_state.resume_token}, State};

handle_call({resume_after, ResumeToken}, _From, State) ->
  NewOptions = maps:put(resume_after, ResumeToken, State#change_stream_state.options),
  NewState = State#change_stream_state{
    options = NewOptions,
    resume_token = ResumeToken,
    cursor = undefined
  },
  {reply, ok, NewState};

handle_call({start_after, StartToken}, _From, State) ->
  NewOptions = maps:put(start_after, StartToken, State#change_stream_state.options),
  NewState = State#change_stream_state{
    options = NewOptions,
    cursor = undefined
  },
  {reply, ok, NewState};

handle_call(_Request, _From, State) ->
  {reply, {error, unknown_request}, State}.

handle_cast(close, State) ->
  close_cursor(State),
  {noreply, State#change_stream_state{closed = true, cursor = undefined}};

handle_cast(_Msg, State) ->
  {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, #change_stream_state{cursor = Pid} = State) ->
  {noreply, State#change_stream_state{cursor = undefined}};

handle_info(_Info, State) ->
  {noreply, State}.

terminate(_Reason, State) ->
  close_cursor(State),
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
start_change_stream(State) ->
  try
    Command = build_change_stream_command(State),
    case mc_worker_api:command(State#change_stream_state.connection, Command) of
      {true, #{<<"cursor">> := CursorInfo}} ->
        case create_cursor_from_response(State, CursorInfo) of
          {ok, Cursor} ->
            {ok, State#change_stream_state{cursor = Cursor}};
          Error ->
            Error
        end;
      {false, Error} ->
        {error, Error};
      Error ->
        {error, Error}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @private
build_change_stream_command(#change_stream_state{
  target = Target,
  target_spec = TargetSpec,
  pipeline = Pipeline,
  options = Options
}) ->
  BaseCommand = case Target of
    deployment ->
      #{<<"aggregate">> => 1};
    database ->
      {Database} = TargetSpec,
      #{<<"aggregate">> => 1, <<"$db">> => Database};
    collection ->
      {Database, Collection} = TargetSpec,
      #{<<"aggregate">> => Collection, <<"$db">> => Database}
  end,
  
  % Build change stream stage
  ChangeStreamStage = build_change_stream_stage(Options),
  FullPipeline = [ChangeStreamStage | Pipeline],
  
  % Add pipeline and cursor options
  Command = maps:merge(BaseCommand, #{
    <<"pipeline">> => FullPipeline,
    <<"cursor">> => build_cursor_options(Options)
  }),
  
  % Add read concern if specified
  case maps:get(read_concern, Options, undefined) of
    undefined -> Command;
    ReadConcern -> maps:put(<<"readConcern">>, ReadConcern, Command)
  end.

%% @private
build_change_stream_stage(Options) ->
  Stage = #{<<"$changeStream">> => #{}},
  ChangeStreamOptions = maps:get(<<"$changeStream">>, Stage),
  
  % Add various options
  UpdatedOptions = lists:foldl(fun({Key, MapKey}, Acc) ->
    case maps:get(Key, Options, undefined) of
      undefined -> Acc;
      Value -> maps:put(MapKey, Value, Acc)
    end
  end, ChangeStreamOptions, [
    {full_document, <<"fullDocument">>},
    {full_document_before_change, <<"fullDocumentBeforeChange">>},
    {resume_after, <<"resumeAfter">>},
    {start_after, <<"startAfter">>},
    {start_at_operation_time, <<"startAtOperationTime">>}
  ]),
  
  maps:put(<<"$changeStream">>, UpdatedOptions, Stage).

%% @private
build_cursor_options(Options) ->
  CursorOptions = #{},
  
  % Add batch size
  BatchSize = maps:get(batch_size, Options, ?DEFAULT_BATCH_SIZE),
  CursorWithBatch = maps:put(<<"batchSize">>, BatchSize, CursorOptions),
  
  % Add max await time
  MaxAwaitTime = maps:get(max_await_time_ms, Options, ?DEFAULT_MAX_AWAIT_TIME_MS),
  maps:put(<<"maxTimeMS">>, MaxAwaitTime, CursorWithBatch).

%% @private
create_cursor_from_response(State, CursorInfo) ->
  try
    CursorId = maps:get(<<"id">>, CursorInfo),
    Namespace = maps:get(<<"ns">>, CursorInfo),
    FirstBatch = maps:get(<<"firstBatch">>, CursorInfo, []),
    
    % Parse namespace
    [_Database, Collection] = binary:split(Namespace, <<".">>),
    
    % Create cursor process
    case mc_cursor:start_link(
      State#change_stream_state.connection,
      Collection,
      CursorId,
      maps:get(batch_size, State#change_stream_state.options, ?DEFAULT_BATCH_SIZE),
      FirstBatch
    ) of
      {ok, Cursor} ->
        {ok, Cursor};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, {cursor_creation_failed, Reason}}
  end.

%% @private
get_next_event(State) ->
  case mc_cursor:next(State#change_stream_state.cursor) of
    error ->
      {reply, timeout, State};
    {Doc} ->
      % Update resume token
      ResumeToken = maps:get(<<"_id">>, Doc, undefined),
      OperationTime = maps:get(<<"clusterTime">>, Doc, undefined),
      
      NewState = State#change_stream_state{
        resume_token = ResumeToken,
        last_operation_time = OperationTime
      },
      
      {reply, {ok, Doc}, NewState}
  end.

%% @private
close_cursor(#change_stream_state{cursor = undefined}) ->
  ok;
close_cursor(#change_stream_state{cursor = Cursor}) ->
  mc_cursor:close(Cursor).
