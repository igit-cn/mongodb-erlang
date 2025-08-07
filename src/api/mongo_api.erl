%%%-------------------------------------------------------------------
%%% @author tihon
%%% @copyright (C) 2016, <COMPANY>
%%% @doc
%%% Api helper module. You can use it as an example for your own api
%%% to mongoc, as not all parameters are passed.
%%% @end
%%% Created : 19. Jan 2016 16:04
%%%-------------------------------------------------------------------
-module(mongo_api).
-author("tihon").

-include("mongoc.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  connect/4,
  insert/3,
  find/4,
  find/6,
  find_one/4,
  find_one/5,
  find_one/6,
  update/5,
  delete/3,
  count/4,
  command/3,
  ensure_index/3,
  disconnect/1]).

%% MongoDB 4.x+ Features
-export([
  aggregate/3,
  aggregate/4,
  watch/2,
  watch/3,
  watch_collection/4,
  watch_collection/5,
  watch_database/3,
  watch_database/4,
  retryable_insert/3,
  retryable_insert/4,
  retryable_update/5,
  retryable_update/6,
  retryable_delete/3,
  retryable_delete/4,
  % Modern MongoDB features
  bulk_write/3,
  bulk_write/4,
  create_index_with_options/4,
  drop_index/3,
  list_indexes/2,
  find_with_hint/4,
  find_with_hint/5,
  update_with_hint/5,
  update_with_hint/6,
  delete_with_hint/4,
  delete_with_hint/5,
  explain_query/3,
  explain_query/4,
  get_collection_stats/2,
  get_collection_stats/3,
  validate_collection/2,
  validate_collection/3,
  compact_collection/2,
  reindex_collection/2
]).

%% Transaction API
-export([
  start_session/1,
  start_session/2,
  end_session/1,
  start_transaction/2,
  start_transaction/3,
  commit_transaction/1,
  abort_transaction/1,
  with_transaction/3,
  with_transaction/4,
  % Enhanced transaction operations
  transaction_insert/4,
  transaction_update/6,
  transaction_delete/4,
  transaction_find/5,
  transaction_find_one/5,
  transaction_count/4,
  % Transaction utilities
  get_transaction_stats/1,
  is_transaction_active/1
]).

-spec connect(atom()|{atom(), binary()}, list(), proplists:proplist(), proplists:proplist()) -> {ok, pid()}.
connect(Type, Hosts, TopologyOptions, WorkerOptions) when is_atom(Type) ->
  mongoc:connect({Type, Hosts}, TopologyOptions, WorkerOptions);
connect(Type, Hosts, TopologyOptions, WorkerOptions) ->
  mongoc:connect(erlang:append_element(Type, Hosts), TopologyOptions, WorkerOptions).

-spec insert(atom() | pid(), collection(), list() | map() | bson:document()) ->
  transaction_result({{boolean(), map()}, list()}).
insert(Topology, Collection, Document) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:insert(Worker, Collection, Document)
    end,
    #{}).

-spec update(atom() | pid(), collection(), selector(), map(), map()) ->
  transaction_result({boolean(), map()}).
update(Topology, Collection, Selector, Doc, Opts) ->
  Upsert = maps:get(upsert, Opts, false),
  MultiUpdate = maps:get(multi, Opts, false),
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:update(Worker, Collection, Selector, Doc, Upsert, MultiUpdate)
    end, Opts).

-spec delete(atom() | pid(), collection(), selector()) ->
  transaction_result({boolean(), map()}).
delete(Topology, Collection, Selector) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:delete(Worker, Collection, Selector)
    end,
    #{}).

-spec find(atom() | pid(), collection(), selector(), projector()) ->
  transaction_result({ok, cursor()} | []).
find(Topology, Collection, Selector, Projector) ->
  find(Topology, Collection, Selector, Projector, 0, 0).

-spec find(atom() | pid(), collection(), selector(), projector(), integer(), integer()) ->
  transaction_result({ok, cursor()} | []).
find(Topology, Collection, Selector, Projector, Skip, Batchsize) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:find_query(Conf, Collection, Selector, Projector, Skip, Batchsize),
      mc_worker_api:find(Worker, Query)
    end, #{}).

-spec find_one(atom() | pid(), collection(), selector(), projector()) ->
  transaction_result(map() | undefined).
find_one(Topology, Collection, Selector, Projector) ->
  find_one(Topology, Collection, Selector, Projector, 0).

-spec find_one(atom() | pid(), collection(), selector(), projector(), integer()) ->
  transaction_result(map() | undefined).
find_one(Topology, Collection, Selector, Projector, Skip) ->
  find_one(Topology, Collection, Selector, Projector, Skip, ?TRANSACTION_TIMEOUT).

-spec find_one(atom() | pid(), collection(), selector(), projector(), integer(), timeout()) ->
  transaction_result(map() | undefined).
find_one(Topology, Collection, Selector, Projector, Skip, Timeout) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:find_one_query(Conf, Collection, Selector, Projector, Skip),
      mc_worker_api:find_one(Worker, Query)
    end, #{}, Timeout).

-spec count(atom() | pid(), collection(), selector(), integer()) ->
    transaction_result(integer()).
count(Topology, Collection, Selector, Limit) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:count_query(Conf, Collection, Selector, Limit),
      mc_worker_api:count(Worker, Query)
    end,
    #{}).

-spec command(atom() | pid(), selector(), timeout()) -> transaction_result(integer()).
command(Topology, Command, Timeout) ->
  mongoc:transaction_query(Topology,
    fun(Conf = #{pool := Worker}) ->
      Query = mongoc:command_query(Conf, Command),
      mc_worker_api:command(Worker, Query)
    end, #{}, Timeout).

%% @doc Creates index on collection according to given spec. This function does
%% not work if you have configured the driver to use the new version of the
%% protocol with application:set_env(mongodb, use_legacy_protocol, false). In
%% that case you can call the createIndexes
%% (https://www.mongodb.com/docs/manual/reference/command/createIndexes/#mongodb-dbcommand-dbcmd.createIndexes)
%% command using the `mc_worker_api:command/2` function instead. 
%%
%%      The key specification is a bson documents with the following fields:
%%      IndexSpec      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
%%      The key specification is a bson documents with the following fields:
%%      key      :: bson document, for e.g. {field, 1, other, -1, location, 2d}, <strong>required</strong>
-spec ensure_index(pid() | atom(), collection(), bson:document()) -> transaction_result(ok).
ensure_index(Topology, Coll, IndexSpec) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:ensure_index(Worker, Coll, IndexSpec)
    end, #{}).

-spec disconnect(atom() | pid()) -> ok.
disconnect(Topology) ->
  mongoc:disconnect(Topology).

%%%===================================================================
%%% Transaction API
%%%===================================================================

%% @doc Start a new session with default options
-spec start_session(atom() | pid()) -> {ok, pid()} | {error, term()}.
start_session(Topology) ->
  start_session(Topology, #{}).

%% @doc Start a new session with specified options
-spec start_session(atom() | pid(), map()) -> {ok, pid()} | {error, term()}.
start_session(Topology, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:start_session(Worker, Options)
    end, #{}).

%% @doc End a session
-spec end_session(pid()) -> ok.
end_session(SessionPid) ->
  mc_worker_api:end_session(SessionPid).

%% @doc Start a transaction with default options
-spec start_transaction(pid(), atom() | pid()) -> {ok, pid()} | {error, term()}.
start_transaction(SessionPid, Topology) ->
  start_transaction(SessionPid, Topology, #transaction_options{}).

%% @doc Start a transaction with specified options
-spec start_transaction(pid(), atom() | pid(), transaction_options()) -> {ok, pid()} | {error, term()}.
start_transaction(SessionPid, Topology, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:start_transaction(SessionPid, Worker, Options)
    end, #{}).

%% @doc Commit a transaction
-spec commit_transaction(pid()) -> ok | {error, term()}.
commit_transaction(TransactionPid) ->
  mc_worker_api:commit_transaction(TransactionPid).

%% @doc Abort a transaction
-spec abort_transaction(pid()) -> ok | {error, term()}.
abort_transaction(TransactionPid) ->
  mc_worker_api:abort_transaction(TransactionPid).

%% @doc Execute a function within a transaction with automatic retry
-spec with_transaction(pid(), atom() | pid(), fun()) -> {ok, term()} | {error, term()}.
with_transaction(SessionPid, Topology, Fun) ->
  with_transaction(SessionPid, Topology, Fun, #transaction_options{}).

%% @doc Execute a function within a transaction with options and automatic retry
-spec with_transaction(pid(), atom() | pid(), fun(), transaction_options()) -> {ok, term()} | {error, term()}.
with_transaction(SessionPid, Topology, Fun, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:with_transaction(SessionPid, Worker, Fun, Options)
    end, #{}).

%%%===================================================================
%%% Enhanced Transaction Operations
%%%===================================================================

%% @doc Insert documents within a transaction context
-spec transaction_insert(pid(), atom() | pid(), collection(), list() | map() | bson:document()) ->
  transaction_result({{boolean(), map()}, list()}).
transaction_insert(TransactionPid, Topology, Collection, Documents) ->
  case mc_transaction:is_in_transaction(TransactionPid) of
    true ->
      mongoc:transaction(Topology,
        fun(#{pool := Worker}) ->
          SessionOptions = mc_transaction:get_session_options(TransactionPid),
          EnhancedDocs = add_transaction_context(Documents, SessionOptions),
          mc_worker_api:insert(Worker, Collection, EnhancedDocs)
        end, get_transaction_read_preference(TransactionPid));
    false ->
      {error, not_in_transaction}
  end.

%% @doc Update documents within a transaction context
-spec transaction_update(pid(), atom() | pid(), collection(), selector(), map(), map()) ->
  transaction_result({boolean(), map()}).
transaction_update(TransactionPid, Topology, Collection, Selector, Update, Options) ->
  case mc_transaction:is_in_transaction(TransactionPid) of
    true ->
      mongoc:transaction(Topology,
        fun(#{pool := Worker}) ->
          SessionOptions = mc_transaction:get_session_options(TransactionPid),
          EnhancedSelector = add_transaction_context(Selector, SessionOptions),
          Upsert = maps:get(upsert, Options, false),
          MultiUpdate = maps:get(multi, Options, false),
          mc_worker_api:update(Worker, Collection, EnhancedSelector, Update, Upsert, MultiUpdate)
        end, get_transaction_read_preference(TransactionPid));
    false ->
      {error, not_in_transaction}
  end.

%% @doc Delete documents within a transaction context
-spec transaction_delete(pid(), atom() | pid(), collection(), selector()) ->
  transaction_result({boolean(), map()}).
transaction_delete(TransactionPid, Topology, Collection, Selector) ->
  case mc_transaction:is_in_transaction(TransactionPid) of
    true ->
      mongoc:transaction(Topology,
        fun(#{pool := Worker}) ->
          SessionOptions = mc_transaction:get_session_options(TransactionPid),
          EnhancedSelector = add_transaction_context(Selector, SessionOptions),
          mc_worker_api:delete(Worker, Collection, EnhancedSelector)
        end, get_transaction_read_preference(TransactionPid));
    false ->
      {error, not_in_transaction}
  end.

%% @doc Find documents within a transaction context
-spec transaction_find(pid(), atom() | pid(), collection(), selector(), projector()) ->
  transaction_result({ok, cursor()} | []).
transaction_find(TransactionPid, Topology, Collection, Selector, Projector) ->
  case mc_transaction:is_in_transaction(TransactionPid) of
    true ->
      mongoc:transaction_query(Topology,
        fun(Conf = #{pool := Worker}) ->
          SessionOptions = mc_transaction:get_session_options(TransactionPid),
          EnhancedSelector = add_transaction_context(Selector, SessionOptions),
          Query = mongoc:find_query(Conf, Collection, EnhancedSelector, Projector, 0, 0),
          mc_worker_api:find(Worker, Query)
        end, get_transaction_read_preference(TransactionPid));
    false ->
      {error, not_in_transaction}
  end.

%% @doc Find one document within a transaction context
-spec transaction_find_one(pid(), atom() | pid(), collection(), selector(), projector()) ->
  transaction_result(map() | undefined).
transaction_find_one(TransactionPid, Topology, Collection, Selector, Projector) ->
  case mc_transaction:is_in_transaction(TransactionPid) of
    true ->
      mongoc:transaction_query(Topology,
        fun(Conf = #{pool := Worker}) ->
          SessionOptions = mc_transaction:get_session_options(TransactionPid),
          EnhancedSelector = add_transaction_context(Selector, SessionOptions),
          Query = mongoc:find_one_query(Conf, Collection, EnhancedSelector, Projector, 0),
          mc_worker_api:find_one(Worker, Query)
        end, get_transaction_read_preference(TransactionPid));
    false ->
      {error, not_in_transaction}
  end.

%% @doc Count documents within a transaction context
-spec transaction_count(pid(), atom() | pid(), collection(), selector()) ->
  transaction_result(integer()).
transaction_count(TransactionPid, Topology, Collection, Selector) ->
  case mc_transaction:is_in_transaction(TransactionPid) of
    true ->
      mongoc:transaction_query(Topology,
        fun(Conf = #{pool := Worker}) ->
          SessionOptions = mc_transaction:get_session_options(TransactionPid),
          EnhancedSelector = add_transaction_context(Selector, SessionOptions),
          Query = mongoc:count_query(Conf, Collection, EnhancedSelector, 0),
          mc_worker_api:count(Worker, Query)
        end, get_transaction_read_preference(TransactionPid));
    false ->
      {error, not_in_transaction}
  end.

%%%===================================================================
%%% Transaction Utilities
%%%===================================================================

%% @doc Get transaction statistics
-spec get_transaction_stats(pid()) -> {ok, map()} | {error, term()}.
get_transaction_stats(TransactionPid) ->
  try
    {ok, State} = mc_transaction:get_transaction_state(TransactionPid),
    Stats = #{
      state => State,
      is_active => mc_transaction:is_in_transaction(TransactionPid),
      start_time => get_transaction_start_time(TransactionPid),
      duration_ms => get_transaction_duration(TransactionPid)
    },
    {ok, Stats}
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Check if transaction is active
-spec is_transaction_active(pid()) -> boolean().
is_transaction_active(TransactionPid) ->
  mc_transaction:is_in_transaction(TransactionPid).

%%%===================================================================
%%% Internal Helper Functions
%%%===================================================================

%% @doc Add transaction context to documents/selectors
-spec add_transaction_context(term(), map()) -> term().
add_transaction_context(Data, SessionOptions) when is_map(Data) ->
  maps:merge(Data, SessionOptions);
add_transaction_context(Data, SessionOptions) when is_list(Data) ->
  case is_bson_document_list(Data) of
    true ->
      [maps:merge(Doc, SessionOptions) || Doc <- Data];
    false ->
      Data
  end;
add_transaction_context(Data, _SessionOptions) ->
  Data.

%% @doc Check if list contains BSON documents
-spec is_bson_document_list(list()) -> boolean().
is_bson_document_list([]) ->
  false;
is_bson_document_list([H|_]) when is_map(H) ->
  true;
is_bson_document_list(_) ->
  false.

%% @doc Get transaction read preference
-spec get_transaction_read_preference(pid()) -> map().
get_transaction_read_preference(TransactionPid) ->
  try
    case mc_transaction:get_transaction_options(TransactionPid) of
      {ok, Options} ->
        case Options#transaction_options.read_preference of
          undefined -> #{<<"mode">> => <<"primary">>};
          ReadPref -> ReadPref
        end;
      _ ->
        #{<<"mode">> => <<"primary">>}
    end
  catch
    _:_ ->
      #{<<"mode">> => <<"primary">>}
  end.

%% @doc Get transaction start time
-spec get_transaction_start_time(pid()) -> integer() | undefined.
get_transaction_start_time(TransactionPid) ->
  try
    mc_transaction:get_start_time(TransactionPid)
  catch
    _:_ -> undefined
  end.

%% @doc Get transaction duration in milliseconds
-spec get_transaction_duration(pid()) -> integer() | undefined.
get_transaction_duration(TransactionPid) ->
  try
    case get_transaction_start_time(TransactionPid) of
      undefined -> undefined;
      StartTime -> erlang:system_time(millisecond) - StartTime
    end
  catch
    _:_ -> undefined
  end.

%%%===================================================================
%%% MongoDB 4.x+ Features Implementation
%%%===================================================================

%% @doc Execute aggregation pipeline with connection pooling
-spec aggregate(atom() | pid(), collection(), [map()]) -> transaction_result({ok, cursor()} | []).
aggregate(Topology, Collection, Pipeline) ->
  aggregate(Topology, Collection, Pipeline, #{}).

-spec aggregate(atom() | pid(), collection(), [map()], map()) -> transaction_result({ok, cursor()} | []).
aggregate(Topology, Collection, Pipeline, Options) ->
  mongoc:transaction_query(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:aggregate(Worker, Collection, Pipeline, Options)
    end, #{}).

%% @doc Watch changes on entire deployment with connection pooling
-spec watch(atom() | pid(), [map()]) -> transaction_result({ok, pid()} | {error, term()}).
watch(Topology, Pipeline) ->
  watch(Topology, Pipeline, #{}).

-spec watch(atom() | pid(), [map()], map()) -> transaction_result({ok, pid()} | {error, term()}).
watch(Topology, Pipeline, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:watch(Worker, Pipeline, Options)
    end, #{}).

%% @doc Watch changes on specific collection with connection pooling
-spec watch_collection(atom() | pid(), binary(), binary(), [map()]) -> transaction_result({ok, pid()} | {error, term()}).
watch_collection(Topology, Database, Collection, Pipeline) ->
  watch_collection(Topology, Database, Collection, Pipeline, #{}).

-spec watch_collection(atom() | pid(), binary(), binary(), [map()], map()) -> transaction_result({ok, pid()} | {error, term()}).
watch_collection(Topology, Database, Collection, Pipeline, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:watch_collection(Worker, Database, Collection, Pipeline, Options)
    end, #{}).

%% @doc Watch changes on specific database with connection pooling
-spec watch_database(atom() | pid(), binary(), [map()]) -> transaction_result({ok, pid()} | {error, term()}).
watch_database(Topology, Database, Pipeline) ->
  watch_database(Topology, Database, Pipeline, #{}).

-spec watch_database(atom() | pid(), binary(), [map()], map()) -> transaction_result({ok, pid()} | {error, term()}).
watch_database(Topology, Database, Pipeline, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:watch_database(Worker, Database, Pipeline, Options)
    end, #{}).

%% @doc Retryable insert with connection pooling
-spec retryable_insert(atom() | pid(), collection(), list() | map() | bson:document()) ->
  transaction_result({{boolean(), map()}, list()}).
retryable_insert(Topology, Collection, Documents) ->
  retryable_insert(Topology, Collection, Documents, #{}).

-spec retryable_insert(atom() | pid(), collection(), list() | map() | bson:document(), map()) ->
  transaction_result({{boolean(), map()}, list()}).
retryable_insert(Topology, Collection, Documents, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:retryable_insert(Worker, Collection, Documents, Options)
    end, #{}).

%% @doc Retryable update with connection pooling
-spec retryable_update(atom() | pid(), collection(), selector(), map(), map()) ->
  transaction_result({boolean(), map()}).
retryable_update(Topology, Collection, Selector, Update, Options) ->
  retryable_update(Topology, Collection, Selector, Update, Options, #{}).

-spec retryable_update(atom() | pid(), collection(), selector(), map(), map(), map()) ->
  transaction_result({boolean(), map()}).
retryable_update(Topology, Collection, Selector, Update, UpdateOptions, RetryOptions) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      CombinedOptions = maps:merge(UpdateOptions, RetryOptions),
      mc_worker_api:retryable_update(Worker, Collection, Selector, Update, CombinedOptions)
    end, #{}).

%% @doc Retryable delete with connection pooling
-spec retryable_delete(atom() | pid(), collection(), selector()) ->
  transaction_result({boolean(), map()}).
retryable_delete(Topology, Collection, Selector) ->
  retryable_delete(Topology, Collection, Selector, #{}).

-spec retryable_delete(atom() | pid(), collection(), selector(), map()) ->
  transaction_result({boolean(), map()}).
retryable_delete(Topology, Collection, Selector, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:retryable_delete(Worker, Collection, Selector, Options)
    end, #{}).

%%%===================================================================
%%% Modern MongoDB Features Implementation
%%%===================================================================

%% @doc Execute bulk write operations with connection pooling
-spec bulk_write(atom() | pid(), collection(), [map()]) -> transaction_result({ok, map()} | {error, term()}).
bulk_write(Topology, Collection, Operations) ->
  bulk_write(Topology, Collection, Operations, #{}).

-spec bulk_write(atom() | pid(), collection(), [map()], map()) -> transaction_result({ok, map()} | {error, term()}).
bulk_write(Topology, Collection, Operations, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:bulk_write(Worker, Collection, Operations, Options)
    end, #{}).

%% @doc Create index with modern options and connection pooling
-spec create_index_with_options(atom() | pid(), collection(), map(), map()) -> transaction_result({ok, map()} | {error, term()}).
create_index_with_options(Topology, Collection, IndexSpec, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:create_index_with_options(Worker, Collection, IndexSpec, Options)
    end, #{}).

%% @doc Drop index with connection pooling
-spec drop_index(atom() | pid(), collection(), binary() | map()) -> transaction_result({ok, map()} | {error, term()}).
drop_index(Topology, Collection, IndexSpec) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:drop_index(Worker, Collection, IndexSpec)
    end, #{}).

%% @doc List indexes with connection pooling
-spec list_indexes(atom() | pid(), collection()) -> transaction_result({ok, [map()]} | {error, term()}).
list_indexes(Topology, Collection) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:list_indexes(Worker, Collection)
    end, #{}).

%% @doc Find with index hint and connection pooling
-spec find_with_hint(atom() | pid(), collection(), map(), binary() | map()) -> transaction_result({ok, pid()} | {error, term()}).
find_with_hint(Topology, Collection, Filter, Hint) ->
  find_with_hint(Topology, Collection, Filter, Hint, #{}).

-spec find_with_hint(atom() | pid(), collection(), map(), binary() | map(), map()) -> transaction_result({ok, pid()} | {error, term()}).
find_with_hint(Topology, Collection, Filter, Hint, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:find_with_hint(Worker, Collection, Filter, Hint, Options)
    end, #{}).

%% @doc Update with index hint and connection pooling
-spec update_with_hint(atom() | pid(), collection(), map(), map(), binary() | map()) -> transaction_result({ok, map()} | {error, term()}).
update_with_hint(Topology, Collection, Filter, Update, Hint) ->
  update_with_hint(Topology, Collection, Filter, Update, Hint, #{}).

-spec update_with_hint(atom() | pid(), collection(), map(), map(), binary() | map(), map()) -> transaction_result({ok, map()} | {error, term()}).
update_with_hint(Topology, Collection, Filter, Update, Hint, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:update_with_hint(Worker, Collection, Filter, Update, Hint, Options)
    end, #{}).

%% @doc Delete with index hint and connection pooling
-spec delete_with_hint(atom() | pid(), collection(), map(), binary() | map()) -> transaction_result({ok, map()} | {error, term()}).
delete_with_hint(Topology, Collection, Filter, Hint) ->
  delete_with_hint(Topology, Collection, Filter, Hint, #{}).

-spec delete_with_hint(atom() | pid(), collection(), map(), binary() | map(), map()) -> transaction_result({ok, map()} | {error, term()}).
delete_with_hint(Topology, Collection, Filter, Hint, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:delete_with_hint(Worker, Collection, Filter, Hint, Options)
    end, #{}).

%% @doc Explain query with connection pooling
-spec explain_query(atom() | pid(), collection(), map()) -> transaction_result({ok, map()} | {error, term()}).
explain_query(Topology, Collection, Query) ->
  explain_query(Topology, Collection, Query, #{}).

-spec explain_query(atom() | pid(), collection(), map(), map()) -> transaction_result({ok, map()} | {error, term()}).
explain_query(Topology, Collection, Query, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:explain_query(Worker, Collection, Query, Options)
    end, #{}).

%% @doc Get collection statistics with connection pooling
-spec get_collection_stats(atom() | pid(), collection()) -> transaction_result({ok, map()} | {error, term()}).
get_collection_stats(Topology, Collection) ->
  get_collection_stats(Topology, Collection, #{}).

-spec get_collection_stats(atom() | pid(), collection(), map()) -> transaction_result({ok, map()} | {error, term()}).
get_collection_stats(Topology, Collection, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:get_collection_stats(Worker, Collection, Options)
    end, #{}).

%% @doc Validate collection with connection pooling
-spec validate_collection(atom() | pid(), collection()) -> transaction_result({ok, map()} | {error, term()}).
validate_collection(Topology, Collection) ->
  validate_collection(Topology, Collection, #{}).

-spec validate_collection(atom() | pid(), collection(), map()) -> transaction_result({ok, map()} | {error, term()}).
validate_collection(Topology, Collection, Options) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:validate_collection(Worker, Collection, Options)
    end, #{}).

%% @doc Compact collection with connection pooling
-spec compact_collection(atom() | pid(), collection()) -> transaction_result({ok, map()} | {error, term()}).
compact_collection(Topology, Collection) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:compact_collection(Worker, Collection)
    end, #{}).

%% @doc Reindex collection with connection pooling
-spec reindex_collection(atom() | pid(), collection()) -> transaction_result({ok, map()} | {error, term()}).
reindex_collection(Topology, Collection) ->
  mongoc:transaction(Topology,
    fun(#{pool := Worker}) ->
      mc_worker_api:reindex_collection(Worker, Collection)
    end, #{}).
