%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% Modern MongoDB features support for MongoDB 4.x+
%%% Includes index hints, bulk operations optimization, and other modern features
%%% @end
%%%-------------------------------------------------------------------
-module(mc_modern_features).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
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

%% Types
-type bulk_operation() :: #{
  type := insert | update | delete | replace,
  document => map(),
  filter => map(),
  update => map(),
  replacement => map(),
  upsert => boolean(),
  multi => boolean(),
  collation => map(),
  array_filters => [map()],
  hint => binary() | map()
}.

-type bulk_write_options() :: #{
  ordered => boolean(),
  write_concern => map(),
  bypass_document_validation => boolean(),
  comment => binary(),
  'let' => map()
}.

-type index_options() :: #{
  unique => boolean(),
  sparse => boolean(),
  partial_filter_expression => map(),
  expire_after_seconds => integer(),
  collation => map(),
  background => boolean(),
  name => binary(),
  comment => binary(),
  commit_quorum => integer() | binary(),
  clustered => boolean()
}.

-export_type([bulk_operation/0, bulk_write_options/0, index_options/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Execute bulk write operations
-spec bulk_write(pid(), binary(), [bulk_operation()]) -> {ok, map()} | {error, term()}.
bulk_write(Connection, Collection, Operations) ->
  bulk_write(Connection, Collection, Operations, #{}).

-spec bulk_write(pid(), binary(), [bulk_operation()], bulk_write_options()) -> {ok, map()} | {error, term()}.
bulk_write(Connection, Collection, Operations, Options) ->
  Command = build_bulk_write_command(Collection, Operations, Options),
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc Create index with modern options
-spec create_index_with_options(pid(), binary(), map(), index_options()) -> {ok, map()} | {error, term()}.
create_index_with_options(Connection, Collection, IndexSpec, Options) ->
  Command = build_create_index_command(Collection, IndexSpec, Options),
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc Drop index by name or specification
-spec drop_index(pid(), binary(), binary() | map()) -> {ok, map()} | {error, term()}.
drop_index(Connection, Collection, IndexSpec) ->
  Command = #{
    <<"dropIndexes">> => Collection,
    <<"index">> => IndexSpec
  },
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc List all indexes on collection
-spec list_indexes(pid(), binary()) -> {ok, [map()]} | {error, term()}.
list_indexes(Connection, Collection) ->
  Command = #{<<"listIndexes">> => Collection},
  case mc_worker_api:command(Connection, Command) of
    {true, #{<<"cursor">> := #{<<"firstBatch">> := Indexes}}} ->
      {ok, Indexes};
    {true, Result} ->
      {ok, [Result]};
    {false, Error} ->
      {error, Error};
    Error ->
      {error, Error}
  end.

%% @doc Find with index hint
-spec find_with_hint(pid(), binary(), map(), binary() | map()) -> {ok, pid()} | {error, term()}.
find_with_hint(Connection, Collection, Filter, Hint) ->
  find_with_hint(Connection, Collection, Filter, Hint, #{}).

-spec find_with_hint(pid(), binary(), map(), binary() | map(), map()) -> {ok, pid()} | {error, term()}.
find_with_hint(Connection, Collection, Filter, Hint, Options) ->
  Command = build_find_command(Collection, Filter, Options#{hint => Hint}),
  case mc_worker_api:command(Connection, Command) of
    {true, #{<<"cursor">> := CursorInfo}} ->
      create_cursor_from_response(Connection, CursorInfo);
    {false, Error} ->
      {error, Error};
    Error ->
      {error, Error}
  end.

%% @doc Update with index hint
-spec update_with_hint(pid(), binary(), map(), map(), binary() | map()) -> {ok, map()} | {error, term()}.
update_with_hint(Connection, Collection, Filter, Update, Hint) ->
  update_with_hint(Connection, Collection, Filter, Update, Hint, #{}).

-spec update_with_hint(pid(), binary(), map(), map(), binary() | map(), map()) -> {ok, map()} | {error, term()}.
update_with_hint(Connection, Collection, Filter, Update, Hint, Options) ->
  Command = build_update_command(Collection, Filter, Update, Options#{hint => Hint}),
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc Delete with index hint
-spec delete_with_hint(pid(), binary(), map(), binary() | map()) -> {ok, map()} | {error, term()}.
delete_with_hint(Connection, Collection, Filter, Hint) ->
  delete_with_hint(Connection, Collection, Filter, Hint, #{}).

-spec delete_with_hint(pid(), binary(), map(), binary() | map(), map()) -> {ok, map()} | {error, term()}.
delete_with_hint(Connection, Collection, Filter, Hint, Options) ->
  Command = build_delete_command(Collection, Filter, Options#{hint => Hint}),
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc Explain query execution plan
-spec explain_query(pid(), binary(), map()) -> {ok, map()} | {error, term()}.
explain_query(Connection, Collection, Query) ->
  explain_query(Connection, Collection, Query, #{}).

-spec explain_query(pid(), binary(), map(), map()) -> {ok, map()} | {error, term()}.
explain_query(Connection, Collection, Query, Options) ->
  Verbosity = maps:get(verbosity, Options, <<"executionStats">>),
  Command = #{
    <<"explain">> => build_find_command(Collection, Query, Options),
    <<"verbosity">> => Verbosity
  },
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc Get collection statistics
-spec get_collection_stats(pid(), binary()) -> {ok, map()} | {error, term()}.
get_collection_stats(Connection, Collection) ->
  get_collection_stats(Connection, Collection, #{}).

-spec get_collection_stats(pid(), binary(), map()) -> {ok, map()} | {error, term()}.
get_collection_stats(Connection, Collection, Options) ->
  Command = maps:merge(#{<<"collStats">> => Collection}, Options),
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc Validate collection
-spec validate_collection(pid(), binary()) -> {ok, map()} | {error, term()}.
validate_collection(Connection, Collection) ->
  validate_collection(Connection, Collection, #{}).

-spec validate_collection(pid(), binary(), map()) -> {ok, map()} | {error, term()}.
validate_collection(Connection, Collection, Options) ->
  Command = maps:merge(#{<<"validate">> => Collection}, Options),
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc Compact collection (reclaim disk space)
-spec compact_collection(pid(), binary()) -> {ok, map()} | {error, term()}.
compact_collection(Connection, Collection) ->
  Command = #{<<"compact">> => Collection},
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%% @doc Reindex collection
-spec reindex_collection(pid(), binary()) -> {ok, map()} | {error, term()}.
reindex_collection(Connection, Collection) ->
  Command = #{<<"reIndex">> => Collection},
  case mc_worker_api:command(Connection, Command) of
    {true, Result} -> {ok, Result};
    {false, Error} -> {error, Error};
    Error -> {error, Error}
  end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
build_bulk_write_command(Collection, Operations, Options) ->
  BaseCommand = #{
    <<"bulkWrite">> => Collection,
    <<"ops">> => [convert_bulk_operation(Op) || Op <- Operations],
    <<"ordered">> => maps:get(ordered, Options, true)
  },
  
  % Add optional parameters
  lists:foldl(fun({Key, CommandKey}, Acc) ->
    case maps:get(Key, Options, undefined) of
      undefined -> Acc;
      Value -> maps:put(CommandKey, Value, Acc)
    end
  end, BaseCommand, [
    {write_concern, <<"writeConcern">>},
    {bypass_document_validation, <<"bypassDocumentValidation">>},
    {comment, <<"comment">>},
    {'let', <<"let">>}
  ]).

%% @private
convert_bulk_operation(#{type := insert, document := Doc} = Op) ->
  BaseOp = #{<<"insertOne">> => #{<<"document">> => Doc}},
  add_bulk_operation_options(BaseOp, Op);

convert_bulk_operation(#{type := update, filter := Filter, update := Update} = Op) ->
  UpdateOp = case maps:get(multi, Op, false) of
    true -> <<"updateMany">>;
    false -> <<"updateOne">>
  end,
  BaseOp = #{UpdateOp => #{
    <<"filter">> => Filter,
    <<"update">> => Update,
    <<"upsert">> => maps:get(upsert, Op, false)
  }},
  add_bulk_operation_options(BaseOp, Op);

convert_bulk_operation(#{type := replace, filter := Filter, replacement := Replacement} = Op) ->
  BaseOp = #{<<"replaceOne">> => #{
    <<"filter">> => Filter,
    <<"replacement">> => Replacement,
    <<"upsert">> => maps:get(upsert, Op, false)
  }},
  add_bulk_operation_options(BaseOp, Op);

convert_bulk_operation(#{type := delete, filter := Filter} = Op) ->
  DeleteOp = case maps:get(multi, Op, false) of
    true -> <<"deleteMany">>;
    false -> <<"deleteOne">>
  end,
  BaseOp = #{DeleteOp => #{<<"filter">> => Filter}},
  add_bulk_operation_options(BaseOp, Op).

%% @private
add_bulk_operation_options(BaseOp, Options) ->
  [OpType] = maps:keys(BaseOp),
  OpSpec = maps:get(OpType, BaseOp),
  
  UpdatedOpSpec = lists:foldl(fun({Key, SpecKey}, Acc) ->
    case maps:get(Key, Options, undefined) of
      undefined -> Acc;
      Value -> maps:put(SpecKey, Value, Acc)
    end
  end, OpSpec, [
    {collation, <<"collation">>},
    {array_filters, <<"arrayFilters">>},
    {hint, <<"hint">>}
  ]),
  
  #{OpType => UpdatedOpSpec}.

%% @private
build_create_index_command(Collection, IndexSpec, Options) ->
  IndexDoc = maps:merge(#{
    <<"key">> => IndexSpec,
    <<"name">> => generate_index_name(IndexSpec)
  }, Options),
  
  #{
    <<"createIndexes">> => Collection,
    <<"indexes">> => [IndexDoc]
  }.

%% @private
generate_index_name(IndexSpec) ->
  Keys = maps:keys(IndexSpec),
  NameParts = [<<Key/binary, "_", (integer_to_binary(maps:get(Key, IndexSpec)))/binary>> || Key <- Keys],
  iolist_to_binary(lists:join(<<"_">>, NameParts)).

%% @private
build_find_command(Collection, Filter, Options) ->
  BaseCommand = #{
    <<"find">> => Collection,
    <<"filter">> => Filter
  },
  
  % Add optional parameters
  lists:foldl(fun({Key, CommandKey}, Acc) ->
    case maps:get(Key, Options, undefined) of
      undefined -> Acc;
      Value -> maps:put(CommandKey, Value, Acc)
    end
  end, BaseCommand, [
    {projection, <<"projection">>},
    {sort, <<"sort">>},
    {limit, <<"limit">>},
    {skip, <<"skip">>},
    {hint, <<"hint">>},
    {collation, <<"collation">>},
    {comment, <<"comment">>},
    {max_time_ms, <<"maxTimeMS">>}
  ]).

%% @private
build_update_command(Collection, Filter, Update, Options) ->
  BaseCommand = #{
    <<"update">> => Collection,
    <<"updates">> => [#{
      <<"q">> => Filter,
      <<"u">> => Update,
      <<"multi">> => maps:get(multi, Options, false),
      <<"upsert">> => maps:get(upsert, Options, false)
    }]
  },
  
  % Add hint if specified
  case maps:get(hint, Options, undefined) of
    undefined -> BaseCommand;
    Hint ->
      Updates = maps:get(<<"updates">>, BaseCommand),
      [UpdateSpec] = Updates,
      UpdatedSpec = maps:put(<<"hint">>, Hint, UpdateSpec),
      maps:put(<<"updates">>, [UpdatedSpec], BaseCommand)
  end.

%% @private
build_delete_command(Collection, Filter, Options) ->
  BaseCommand = #{
    <<"delete">> => Collection,
    <<"deletes">> => [#{
      <<"q">> => Filter,
      <<"limit">> => maps:get(limit, Options, 1)
    }]
  },
  
  % Add hint if specified
  case maps:get(hint, Options, undefined) of
    undefined -> BaseCommand;
    Hint ->
      Deletes = maps:get(<<"deletes">>, BaseCommand),
      [DeleteSpec] = Deletes,
      UpdatedSpec = maps:put(<<"hint">>, Hint, DeleteSpec),
      maps:put(<<"deletes">>, [UpdatedSpec], BaseCommand)
  end.

%% @private
create_cursor_from_response(Connection, CursorInfo) ->
  try
    CursorId = maps:get(<<"id">>, CursorInfo),
    Namespace = maps:get(<<"ns">>, CursorInfo),
    FirstBatch = maps:get(<<"firstBatch">>, CursorInfo, []),
    
    % Parse namespace
    [_Database, Collection] = binary:split(Namespace, <<".">>),
    
    % Create cursor process
    mc_cursor:start(Connection, Collection, CursorId, 101, FirstBatch)
  catch
    _:Reason ->
      {error, {cursor_creation_failed, Reason}}
  end.
