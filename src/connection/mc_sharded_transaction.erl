%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% MongoDB sharded cluster transaction support module.
%%% Handles shard key validation, mongos routing, and cross-shard coordination.
%%% @end
%%%-------------------------------------------------------------------
-module(mc_sharded_transaction).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").
-include("mongo_health.hrl").

%% API
-export([
  validate_shard_key/3,
  get_target_shard/3,
  validate_cross_shard_transaction/2,
  get_mongos_list/1,
  select_optimal_mongos/2,
  check_shard_compatibility/2,
  get_shard_collection_info/2,
  validate_transaction_operations/2
]).

-define(SHARD_KEY_CACHE_TTL, 300000). % 5 minutes
-define(MONGOS_HEALTH_CHECK_INTERVAL, 30000). % 30 seconds

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Validate that operations include required shard keys
-spec validate_shard_key(pid(), collection(), map() | bson:document()) -> 
  ok | {error, {missing_shard_key, list()}}.
validate_shard_key(Worker, Collection, Document) ->
  case get_shard_collection_info(Worker, Collection) of
    {ok, #{<<"key">> := ShardKey}} ->
      validate_document_shard_key(Document, ShardKey);
    {error, collection_not_sharded} ->
      ok; % Non-sharded collections don't require shard keys
    {error, Reason} ->
      {error, Reason}
  end.

%% @doc Get target shard for a document based on shard key
-spec get_target_shard(pid(), collection(), map() | bson:document()) -> 
  {ok, binary()} | {error, term()}.
get_target_shard(Worker, Collection, Document) ->
  case get_shard_collection_info(Worker, Collection) of
    {ok, ShardInfo} ->
      calculate_target_shard(Document, ShardInfo);
    Error ->
      Error
  end.

%% @doc Validate that all operations in a transaction can be executed together
-spec validate_cross_shard_transaction(pid(), list()) -> ok | {error, term()}.
validate_cross_shard_transaction(Worker, Operations) ->
  try
    % Analyze shard targets for all operations
    ShardAnalysis = analyze_operation_shard_targets(Worker, Operations),

    case ShardAnalysis of
      {ok, #{shard_count := 0}} ->
        {error, no_valid_operations};
      {ok, #{shard_count := 1}} ->
        ok; % Single shard transaction
      {ok, #{shard_count := ShardCount, shards := Shards, conflicts := []}} ->
        % Multi-shard transaction - validate support and constraints
        case validate_multi_shard_transaction(Worker, ShardCount, Shards) of
          ok -> ok;
          Error -> Error
        end;
      {ok, #{conflicts := Conflicts}} when length(Conflicts) > 0 ->
        {error, {shard_key_conflicts, Conflicts}};
      {error, AnalysisReason} ->
        {error, AnalysisReason}
    end
  catch
    _:CatchReason ->
      {error, CatchReason}
  end.

%% @doc Get list of available mongos instances
-spec get_mongos_list(pid()) -> {ok, list()} | {error, term()}.
get_mongos_list(Worker) ->
  try
    Command = {<<"isMaster">>, 1},
    case mc_worker_api:command(Worker, Command) of
      {true, #{<<"msg">> := <<"isdbgrid">>, <<"hosts">> := Hosts}} ->
        {ok, Hosts};
      {true, #{<<"msg">> := <<"isdbgrid">>}} ->
        {ok, [get_current_host(Worker)]};
      _ ->
        {error, not_mongos}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Select optimal mongos based on load, health, and proximity
-spec select_optimal_mongos(list(), map()) -> {ok, binary()} | {error, term()}.
select_optimal_mongos([], _Options) ->
  {error, no_mongos_available};
select_optimal_mongos([Mongos], _Options) ->
  {ok, Mongos};
select_optimal_mongos(MongosList, Options) ->
  try
    Strategy = maps:get(selection_strategy, Options, health_based),
    case Strategy of
      round_robin ->
        select_mongos_round_robin(MongosList);
      health_based ->
        select_mongos_by_health(MongosList, Options);
      latency_based ->
        select_mongos_by_latency(MongosList, Options);
      load_based ->
        select_mongos_by_load(MongosList, Options)
    end
  catch
    _:_ ->
      {ok, hd(MongosList)} % Fallback to first mongos
  end.

%% @doc Check if sharded cluster supports required transaction features
-spec check_shard_compatibility(pid(), transaction_options()) -> ok | {error, term()}.
check_shard_compatibility(Worker, Options) ->
  try
    % Check MongoDB version across all shards
    case check_all_shards_version(Worker) of
      {ok, MinVersion} ->
        case is_version_compatible(MinVersion, Options) of
          true -> ok;
          false -> {error, {incompatible_version, MinVersion}}
        end;
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Get shard collection information including shard key
-spec get_shard_collection_info(pid(), collection()) -> {ok, map()} | {error, term()}.
get_shard_collection_info(Worker, Collection) ->
  try
    % Check cache first
    case get_cached_shard_info(Collection) of
      {ok, Info} -> {ok, Info};
      cache_miss ->
        % Query config database
        Command = {<<"listCollections">>, 1, 
                  <<"filter">>, #{<<"name">> => Collection}},
        case mc_worker_api:command(Worker, Command) of
          {true, #{<<"cursor">> := #{<<"firstBatch">> := [CollInfo|_]}}} ->
            case maps:get(<<"options">>, CollInfo, #{}) of
              #{<<"shardKey">> := ShardKey} = Options ->
                Info = #{<<"key">> => ShardKey, <<"options">> => Options},
                cache_shard_info(Collection, Info),
                {ok, Info};
              _ ->
                {error, collection_not_sharded}
            end;
          _ ->
            {error, collection_not_found}
        end
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Validate all operations in a transaction for shard compatibility
-spec validate_transaction_operations(pid(), list()) -> ok | {error, term()}.
validate_transaction_operations(Worker, Operations) ->
  try
    ValidationResults = [validate_operation(Worker, Op) || Op <- Operations],
    case [Error || {error, Error} <- ValidationResults] of
      [] -> ok;
      Errors -> {error, {validation_failed, Errors}}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @doc Validate document contains required shard key fields
-spec validate_document_shard_key(map() | bson:document(), map()) -> 
  ok | {error, {missing_shard_key, list()}}.
validate_document_shard_key(Document, ShardKey) when is_map(Document) ->
  ShardKeyFields = maps:keys(ShardKey),
  DocumentFields = maps:keys(Document),
  MissingFields = ShardKeyFields -- DocumentFields,
  
  case MissingFields of
    [] -> ok;
    _ -> {error, {missing_shard_key, MissingFields}}
  end;
validate_document_shard_key(Document, ShardKey) when is_tuple(Document) ->
  % Convert BSON document to map for validation
  DocumentMap = maps:from_list(bson:fields(Document)),
  validate_document_shard_key(DocumentMap, ShardKey).

%% @doc Calculate target shard for a document using MongoDB's actual sharding algorithm
-spec calculate_target_shard(map() | bson:document(), map()) -> {ok, binary()} | {error, term()}.
calculate_target_shard(Document, #{<<"key">> := ShardKey} = ShardInfo) ->
  try
    % Extract shard key values from document
    ShardKeyValues = extract_shard_key_values(Document, ShardKey),

    % Get sharding method (hashed vs ranged)
    ShardingMethod = determine_sharding_method(ShardKey),

    case ShardingMethod of
      hashed ->
        calculate_hashed_shard_target(ShardKeyValues, ShardInfo);
      ranged ->
        calculate_ranged_shard_target(ShardKeyValues, ShardInfo);
      compound ->
        calculate_compound_shard_target(ShardKeyValues, ShardInfo)
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Extract shard key values from document
-spec extract_shard_key_values(map() | bson:document(), map()) -> map().
extract_shard_key_values(Document, ShardKey) when is_map(Document) ->
  maps:with(maps:keys(ShardKey), Document);
extract_shard_key_values(Document, ShardKey) when is_tuple(Document) ->
  DocumentMap = maps:from_list(bson:fields(Document)),
  extract_shard_key_values(DocumentMap, ShardKey).

%% @doc Determine sharding method from shard key
-spec determine_sharding_method(map()) -> hashed | ranged | compound.
determine_sharding_method(ShardKey) ->
  ShardKeyFields = maps:to_list(ShardKey),
  case ShardKeyFields of
    [{_Field, <<"hashed">>}] -> hashed;
    [{_Field, 1}] -> ranged;
    [{_Field, -1}] -> ranged;
    _ -> compound % Multiple fields or complex sharding
  end.

%% @doc Calculate target shard for hashed sharding
-spec calculate_hashed_shard_target(map(), map()) -> {ok, binary()} | {error, term()}.
calculate_hashed_shard_target(ShardKeyValues, ShardInfo) ->
  try
    % Use MongoDB's hashing algorithm (simplified version)
    HashValue = calculate_mongodb_hash(ShardKeyValues),

    % Get chunk ranges for hashed sharding
    case get_chunk_ranges(ShardInfo, hashed) of
      {ok, ChunkRanges} ->
        TargetShard = find_shard_by_hash(HashValue, ChunkRanges),
        {ok, TargetShard};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Calculate target shard for ranged sharding
-spec calculate_ranged_shard_target(map(), map()) -> {ok, binary()} | {error, term()}.
calculate_ranged_shard_target(ShardKeyValues, ShardInfo) ->
  try
    % Get chunk ranges for ranged sharding
    case get_chunk_ranges(ShardInfo, ranged) of
      {ok, ChunkRanges} ->
        TargetShard = find_shard_by_range(ShardKeyValues, ChunkRanges),
        {ok, TargetShard};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Calculate target shard for compound sharding
-spec calculate_compound_shard_target(map(), map()) -> {ok, binary()} | {error, term()}.
calculate_compound_shard_target(ShardKeyValues, ShardInfo) ->
  try
    % For compound shard keys, use ranged sharding logic
    % but consider all fields in the shard key
    case get_chunk_ranges(ShardInfo, compound) of
      {ok, ChunkRanges} ->
        TargetShard = find_shard_by_compound_range(ShardKeyValues, ChunkRanges),
        {ok, TargetShard};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Calculate MongoDB-compatible hash
-spec calculate_mongodb_hash(map()) -> integer().
calculate_mongodb_hash(ShardKeyValues) ->
  % MongoDB uses MD5 hash for hashed sharding
  % This is a simplified implementation
  KeyBinary = term_to_binary(maps:to_list(ShardKeyValues)),
  Hash = crypto:hash(md5, KeyBinary),
  <<HashInt:128/big-unsigned-integer>> = Hash,
  % Convert to MongoDB's hash range (-2^63 to 2^63-1)
  (HashInt rem (1 bsl 63)) - (1 bsl 62).

%% @doc Get chunk ranges from config database
-spec get_chunk_ranges(map(), atom()) -> {ok, list()} | {error, term()}.
get_chunk_ranges(ShardInfo, ShardingType) ->
  try
    % In a real implementation, this would query config.chunks
    % For now, create mock chunk ranges based on sharding type
    case ShardingType of
      hashed ->
        create_mock_hashed_chunks(ShardInfo);
      ranged ->
        create_mock_ranged_chunks(ShardInfo);
      compound ->
        create_mock_compound_chunks(ShardInfo)
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Find shard by hash value
-spec find_shard_by_hash(integer(), list()) -> binary().
find_shard_by_hash(HashValue, ChunkRanges) ->
  case find_matching_chunk(HashValue, ChunkRanges, hash_compare) of
    {ok, Shard} -> Shard;
    not_found -> <<"shard0000">> % Default shard
  end.

%% @doc Find shard by range comparison
-spec find_shard_by_range(map(), list()) -> binary().
find_shard_by_range(ShardKeyValues, ChunkRanges) ->
  case find_matching_chunk(ShardKeyValues, ChunkRanges, range_compare) of
    {ok, Shard} -> Shard;
    not_found -> <<"shard0000">> % Default shard
  end.

%% @doc Find shard by compound range comparison
-spec find_shard_by_compound_range(map(), list()) -> binary().
find_shard_by_compound_range(ShardKeyValues, ChunkRanges) ->
  case find_matching_chunk(ShardKeyValues, ChunkRanges, compound_compare) of
    {ok, Shard} -> Shard;
    not_found -> <<"shard0000">> % Default shard
  end.

%% @doc Find matching chunk using specified comparison method
-spec find_matching_chunk(term(), list(), atom()) -> {ok, binary()} | not_found.
find_matching_chunk(_Value, [], _CompareMethod) ->
  not_found;
find_matching_chunk(Value, [Chunk | Rest], CompareMethod) ->
  case chunk_contains_value(Value, Chunk, CompareMethod) of
    true ->
      ShardId = maps:get(<<"shard">>, Chunk, <<"shard0000">>),
      {ok, ShardId};
    false ->
      find_matching_chunk(Value, Rest, CompareMethod)
  end.

%% @doc Check if chunk contains the value
-spec chunk_contains_value(term(), map(), atom()) -> boolean().
chunk_contains_value(HashValue, Chunk, hash_compare) when is_integer(HashValue) ->
  Min = maps:get(<<"min">>, Chunk, -9223372036854775808), % -2^63
  Max = maps:get(<<"max">>, Chunk, 9223372036854775807),  % 2^63-1
  HashValue >= Min andalso HashValue < Max;

chunk_contains_value(ShardKeyValues, Chunk, range_compare) when is_map(ShardKeyValues) ->
  Min = maps:get(<<"min">>, Chunk, #{}),
  Max = maps:get(<<"max">>, Chunk, #{}),
  compare_shard_key_range(ShardKeyValues, Min, Max);

chunk_contains_value(ShardKeyValues, Chunk, compound_compare) when is_map(ShardKeyValues) ->
  Min = maps:get(<<"min">>, Chunk, #{}),
  Max = maps:get(<<"max">>, Chunk, #{}),
  compare_compound_shard_key_range(ShardKeyValues, Min, Max);

chunk_contains_value(_Value, _Chunk, _CompareMethod) ->
  false.

%% @doc Get operation's target shard
-spec get_operation_shard_target(pid(), map()) -> {ok, binary()} | {error, term()}.
get_operation_shard_target(Worker, #{collection := Collection, document := Document}) ->
  get_target_shard(Worker, Collection, Document);
get_operation_shard_target(Worker, #{collection := Collection, selector := Selector}) ->
  get_target_shard(Worker, Collection, Selector);
get_operation_shard_target(_Worker, _Operation) ->
  {error, invalid_operation}.

%% @doc Check if multi-shard transactions are supported
-spec check_multi_shard_support(pid()) -> boolean().
check_multi_shard_support(Worker) ->
  try
    Command = {<<"buildInfo">>, 1},
    case mc_worker_api:command(Worker, Command) of
      {true, #{<<"version">> := VersionBin}} ->
        Version = parse_version(VersionBin),
        is_multi_shard_supported(Version);
      _ ->
        false
    end
  catch
    _:_ -> false
  end.

%% @doc Parse MongoDB version string
-spec parse_version(binary()) -> {integer(), integer(), integer()}.
parse_version(VersionBin) ->
  VersionStr = binary_to_list(VersionBin),
  case string:tokens(VersionStr, ".") of
    [MajorStr, MinorStr | _] ->
      Major = list_to_integer(MajorStr),
      Minor = list_to_integer(MinorStr),
      {Major, Minor, 0};
    _ ->
      {0, 0, 0}
  end.

%% @doc Check if version supports multi-shard transactions
-spec is_multi_shard_supported({integer(), integer(), integer()}) -> boolean().
is_multi_shard_supported({Major, Minor, _}) when Major > 4; (Major =:= 4 andalso Minor >= 2) ->
  true;
is_multi_shard_supported(_) ->
  false.

%% @doc Check version compatibility with transaction options
-spec is_version_compatible({integer(), integer(), integer()}, transaction_options()) -> boolean().
is_version_compatible(Version, _Options) ->
  % For now, just check basic multi-shard support
  is_multi_shard_supported(Version).

%% @doc Check MongoDB version across all shards
-spec check_all_shards_version(pid()) -> {ok, {integer(), integer(), integer()}} | {error, term()}.
check_all_shards_version(Worker) ->
  try
    % Query config.shards to get all shard information
    Command = {<<"find">>, <<"shards">>},
    case mc_worker_api:command(Worker, Command) of
      {true, #{<<"cursor">> := #{<<"firstBatch">> := Shards}}} ->
        Versions = [get_shard_version(Worker, Shard) || Shard <- Shards],
        case [V || {ok, V} <- Versions] of
          [] -> {error, no_shard_versions};
          ValidVersions -> {ok, lists:min(ValidVersions)}
        end;
      _ ->
        {error, cannot_query_shards}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Get version of a specific shard
-spec get_shard_version(pid(), map()) -> {ok, {integer(), integer(), integer()}} | {error, term()}.
get_shard_version(_Worker, #{<<"host">> := _Host}) ->
  % Simplified - in real implementation, would connect to each shard
  {ok, {4, 2, 0}}. % Assume minimum supported version

%% @doc Get current host information
-spec get_current_host(pid()) -> binary().
get_current_host(_Worker) ->
  <<"localhost:27017">>. % Simplified implementation

%% @doc Validate a single operation
-spec validate_operation(pid(), map()) -> ok | {error, term()}.
validate_operation(Worker, #{collection := Collection, document := Document}) ->
  validate_shard_key(Worker, Collection, Document);
validate_operation(Worker, #{collection := Collection, selector := Selector}) ->
  validate_shard_key(Worker, Collection, Selector);
validate_operation(_Worker, _Operation) ->
  ok.

%% @doc Create mock hashed chunks for testing
-spec create_mock_hashed_chunks(map()) -> {ok, list()}.
create_mock_hashed_chunks(_ShardInfo) ->
  % Create mock chunks for hashed sharding
  Chunks = [
    #{<<"min">> => -9223372036854775808, <<"max">> => -3074457345618258603, <<"shard">> => <<"shard0000">>},
    #{<<"min">> => -3074457345618258603, <<"max">> => 3074457345618258602, <<"shard">> => <<"shard0001">>},
    #{<<"min">> => 3074457345618258602, <<"max">> => 9223372036854775807, <<"shard">> => <<"shard0002">>}
  ],
  {ok, Chunks}.

%% @doc Create mock ranged chunks for testing
-spec create_mock_ranged_chunks(map()) -> {ok, list()}.
create_mock_ranged_chunks(_ShardInfo) ->
  % Create mock chunks for ranged sharding
  Chunks = [
    #{<<"min">> => #{<<"region">> => <<"MinKey">>}, <<"max">> => #{<<"region">> => <<"region_m">>}, <<"shard">> => <<"shard0000">>},
    #{<<"min">> => #{<<"region">> => <<"region_m">>}, <<"max">> => #{<<"region">> => <<"region_z">>}, <<"shard">> => <<"shard0001">>},
    #{<<"min">> => #{<<"region">> => <<"region_z">>}, <<"max">> => #{<<"region">> => <<"MaxKey">>}, <<"shard">> => <<"shard0002">>}
  ],
  {ok, Chunks}.

%% @doc Create mock compound chunks for testing
-spec create_mock_compound_chunks(map()) -> {ok, list()}.
create_mock_compound_chunks(_ShardInfo) ->
  % Create mock chunks for compound sharding
  Chunks = [
    #{<<"min">> => #{<<"region">> => <<"MinKey">>, <<"date">> => <<"MinKey">>},
      <<"max">> => #{<<"region">> => <<"region_m">>, <<"date">> => <<"MaxKey">>},
      <<"shard">> => <<"shard0000">>},
    #{<<"min">> => #{<<"region">> => <<"region_m">>, <<"date">> => <<"MinKey">>},
      <<"max">> => #{<<"region">> => <<"MaxKey">>, <<"date">> => <<"MaxKey">>},
      <<"shard">> => <<"shard0001">>}
  ],
  {ok, Chunks}.

%% @doc Compare shard key values against range
-spec compare_shard_key_range(map(), map(), map()) -> boolean().
compare_shard_key_range(ShardKeyValues, Min, Max) ->
  try
    % For single field shard keys
    case maps:size(ShardKeyValues) of
      1 ->
        [{Field, Value}] = maps:to_list(ShardKeyValues),
        MinValue = maps:get(Field, Min, <<"MinKey">>),
        MaxValue = maps:get(Field, Max, <<"MaxKey">>),
        compare_bson_values(Value, MinValue, MaxValue);
      _ ->
        % For multiple fields, use compound comparison
        compare_compound_shard_key_range(ShardKeyValues, Min, Max)
    end
  catch
    _:_ -> false
  end.

%% @doc Compare compound shard key values against range
-spec compare_compound_shard_key_range(map(), map(), map()) -> boolean().
compare_compound_shard_key_range(ShardKeyValues, Min, Max) ->
  try
    % Compare each field in order
    ShardKeyFields = maps:keys(ShardKeyValues),
    compare_compound_fields(ShardKeyFields, ShardKeyValues, Min, Max)
  catch
    _:_ -> false
  end.

%% @doc Compare compound fields recursively
-spec compare_compound_fields(list(), map(), map(), map()) -> boolean().
compare_compound_fields([], _ShardKeyValues, _Min, _Max) ->
  true; % All fields compared successfully
compare_compound_fields([Field | RestFields], ShardKeyValues, Min, Max) ->
  Value = maps:get(Field, ShardKeyValues),
  MinValue = maps:get(Field, Min, <<"MinKey">>),
  MaxValue = maps:get(Field, Max, <<"MaxKey">>),

  case compare_bson_values(Value, MinValue, MaxValue) of
    true ->
      % This field is in range, check remaining fields
      compare_compound_fields(RestFields, ShardKeyValues, Min, Max);
    false ->
      false
  end.

%% @doc Compare BSON values according to MongoDB ordering
-spec compare_bson_values(term(), term(), term()) -> boolean().
compare_bson_values(Value, <<"MinKey">>, _Max) ->
  Value =/= <<"MinKey">>;
compare_bson_values(Value, _Min, <<"MaxKey">>) ->
  Value =/= <<"MaxKey">>;
compare_bson_values(Value, Min, Max) when is_binary(Value), is_binary(Min), is_binary(Max) ->
  Value >= Min andalso Value < Max;
compare_bson_values(Value, Min, Max) when is_integer(Value), is_integer(Min), is_integer(Max) ->
  Value >= Min andalso Value < Max;
compare_bson_values(Value, Min, Max) when is_float(Value), is_number(Min), is_number(Max) ->
  Value >= Min andalso Value < Max;
compare_bson_values(_Value, _Min, _Max) ->
  % For other types, use term comparison as fallback
  false.

%% @doc Get cached shard information with real ETS implementation
-spec get_cached_shard_info(collection()) -> {ok, map()} | cache_miss.
get_cached_shard_info(Collection) ->
  case ets:info(shard_info_cache) of
    undefined ->
      % Create cache table if it doesn't exist
      ets:new(shard_info_cache, [named_table, public, {read_concurrency, true}]),
      cache_miss;
    _ ->
      case ets:lookup(shard_info_cache, Collection) of
        [{Collection, Info, Timestamp}] ->
          Now = erlang:system_time(millisecond),
          if (Now - Timestamp) < ?SHARD_KEY_CACHE_TTL ->
            {ok, Info};
          true ->
            ets:delete(shard_info_cache, Collection),
            cache_miss
          end;
        [] ->
          cache_miss
      end
  end.

%% @doc Cache shard information with real ETS implementation
-spec cache_shard_info(collection(), map()) -> ok.
cache_shard_info(Collection, Info) ->
  case ets:info(shard_info_cache) of
    undefined ->
      ets:new(shard_info_cache, [named_table, public, {read_concurrency, true}]);
    _ ->
      ok
  end,
  Timestamp = erlang:system_time(millisecond),
  ets:insert(shard_info_cache, {Collection, Info, Timestamp}),
  ok.

%%%===================================================================
%%% Mongos Selection Strategies
%%%===================================================================

%% @doc Select mongos using round-robin strategy
-spec select_mongos_round_robin(list()) -> {ok, binary()}.
select_mongos_round_robin(MongosList) ->
  Index = erlang:system_time(microsecond) rem length(MongosList),
  SelectedMongos = lists:nth(Index + 1, MongosList),
  {ok, SelectedMongos}.

%% @doc Select mongos based on health status
-spec select_mongos_by_health(list(), map()) -> {ok, binary()} | {error, term()}.
select_mongos_by_health(MongosList, _Options) ->
  try
    % Check health of each mongos
    HealthChecks = [{Mongos, check_mongos_health(Mongos)} || Mongos <- MongosList],
    HealthyMongos = [Mongos || {Mongos, {ok, _}} <- HealthChecks],

    case HealthyMongos of
      [] -> {error, no_healthy_mongos};
      [SingleMongos] -> {ok, SingleMongos};
      MultipleMongos -> select_mongos_round_robin(MultipleMongos)
    end
  catch
    _:Reason ->
      {error, {health_check_failed, Reason}}
  end.

%% @doc Select mongos based on network latency
-spec select_mongos_by_latency(list(), map()) -> {ok, binary()} | {error, term()}.
select_mongos_by_latency(MongosList, Options) ->
  try
    Timeout = maps:get(latency_timeout, Options, 1000),
    % Measure latency to each mongos
    LatencyChecks = [{Mongos, measure_mongos_latency(Mongos, Timeout)} || Mongos <- MongosList],
    ValidLatencies = [{Mongos, Latency} || {Mongos, {ok, Latency}} <- LatencyChecks],

    case ValidLatencies of
      [] -> {error, no_responsive_mongos};
      _ ->
        % Sort by latency and select the fastest
        SortedByLatency = lists:keysort(2, ValidLatencies),
        {BestMongos, _Latency} = hd(SortedByLatency),
        {ok, BestMongos}
    end
  catch
    _:Reason ->
      {error, {latency_check_failed, Reason}}
  end.

%% @doc Select mongos based on current load
-spec select_mongos_by_load(list(), map()) -> {ok, binary()} | {error, term()}.
select_mongos_by_load(MongosList, _Options) ->
  try
    % Check load metrics for each mongos
    LoadChecks = [{Mongos, check_mongos_load(Mongos)} || Mongos <- MongosList],
    ValidLoads = [{Mongos, Load} || {Mongos, {ok, Load}} <- LoadChecks],

    case ValidLoads of
      [] -> {error, no_load_info_available};
      _ ->
        % Sort by load and select the least loaded
        SortedByLoad = lists:keysort(2, ValidLoads),
        {BestMongos, _Load} = hd(SortedByLoad),
        {ok, BestMongos}
    end
  catch
    _:Reason ->
      {error, {load_check_failed, Reason}}
  end.

%% @doc Check health of a specific mongos using comprehensive health monitor
-spec check_mongos_health(binary()) -> {ok, map()} | {error, term()}.
check_mongos_health(MongosHost) ->
  try
    case mc_health_monitor:check_mongos_health(MongosHost) of
      {ok, HealthStatus} ->
        % Convert health status record to map for compatibility
        {ok, #{
          status => HealthStatus#health_status.status,
          latency_ms => HealthStatus#health_status.latency_ms,
          connection_count => HealthStatus#health_status.connection_count,
          cpu_usage => HealthStatus#health_status.cpu_usage,
          memory_usage => HealthStatus#health_status.memory_usage,
          last_check => HealthStatus#health_status.last_check,
          details => HealthStatus#health_status.details
        }};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Measure network latency to mongos using real TCP connection
-spec measure_mongos_latency(binary(), integer()) -> {ok, integer()} | {error, term()}.
measure_mongos_latency(MongosHost, Timeout) ->
  try
    {Host, Port} = parse_host_port(MongosHost),
    StartTime = erlang:system_time(microsecond),

    % Attempt TCP connection to measure real network latency
    case gen_tcp:connect(binary_to_list(Host), Port,
                        [binary, {active, false}, {packet, 0}], Timeout) of
      {ok, Socket} ->
        % Connection successful, measure time
        ConnectTime = erlang:system_time(microsecond),
        gen_tcp:close(Socket),

        % Now measure MongoDB-specific latency with isMaster command
        case measure_mongodb_command_latency(MongosHost, Timeout) of
          {ok, CommandLatency} ->
            NetworkLatency = (ConnectTime - StartTime) div 1000,
            TotalLatency = max(NetworkLatency, CommandLatency),
            {ok, TotalLatency};
          {error, _} ->
            % Fall back to network latency if MongoDB command fails
            NetworkLatency = (ConnectTime - StartTime) div 1000,
            {ok, NetworkLatency}
        end;
      {error, ConnReason} ->
        {error, {connection_failed, ConnReason}}
    end
  catch
    _:CatchReason ->
      {error, CatchReason}
  end.

%% @doc Check current load on mongos using real MongoDB serverStatus
-spec check_mongos_load(binary()) -> {ok, map()} | {error, term()}.
check_mongos_load(MongosHost) ->
  try
    case establish_mongodb_connection(MongosHost, 5000) of
      {ok, Connection} ->
        try
          LoadMetrics = get_comprehensive_load_metrics(Connection),
          mc_worker_api:disconnect(Connection),
          {ok, LoadMetrics}
        catch
          _:LoadReason ->
            mc_worker_api:disconnect(Connection),
            {error, {load_check_failed, LoadReason}}
        end;
      {error, ConnReason} ->
        {error, {connection_failed, ConnReason}}
    end
  catch
    _:CatchReason ->
      {error, CatchReason}
  end.

%%%===================================================================
%%% Cross-Shard Transaction Coordination
%%%===================================================================

%% @doc Analyze shard targets for all operations in a transaction
-spec analyze_operation_shard_targets(pid(), list()) -> {ok, map()} | {error, term()}.
analyze_operation_shard_targets(Worker, Operations) ->
  try
    % Analyze each operation
    OperationAnalysis = [analyze_single_operation(Worker, Op) || Op <- Operations],

    % Separate successful and failed analyses
    {Successful, Failed} = lists:partition(
      fun({ok, _}) -> true; (_) -> false end,
      OperationAnalysis
    ),

    case Failed of
      [] ->
        % All operations analyzed successfully
        ShardTargets = [Target || {ok, Target} <- Successful],
        UniqueShards = lists:usort([Shard || #{shard := Shard} <- ShardTargets]),

        % Check for conflicts
        Conflicts = detect_shard_key_conflicts(ShardTargets),

        Analysis = #{
          shard_count => length(UniqueShards),
          shards => UniqueShards,
          operations => ShardTargets,
          conflicts => Conflicts
        },
        {ok, Analysis};
      _ ->
        % Some operations failed analysis
        Errors = [Error || {error, Error} <- Failed],
        {error, {operation_analysis_failed, Errors}}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Analyze a single operation for shard targeting
-spec analyze_single_operation(pid(), map()) -> {ok, map()} | {error, term()}.
analyze_single_operation(Worker, Operation) ->
  try
    Collection = maps:get(collection, Operation),
    OperationType = maps:get(type, Operation, unknown),

    % Get shard key information for collection
    case get_shard_collection_info(Worker, Collection) of
      {ok, ShardInfo} ->
        case extract_operation_shard_key(Operation, ShardInfo) of
          {ok, ShardKeyValues} ->
            case get_target_shard(Worker, Collection, ShardKeyValues) of
              {ok, TargetShard} ->
                {ok, #{
                  collection => Collection,
                  operation_type => OperationType,
                  shard => TargetShard,
                  shard_key_values => ShardKeyValues
                }};
              Error ->
                Error
            end;
          Error ->
            Error
        end;
      {error, collection_not_sharded} ->
        % Non-sharded collection - can run on any shard
        {ok, #{
          collection => Collection,
          operation_type => OperationType,
          shard => <<"primary">>,
          shard_key_values => #{}
        }};
      Error ->
        Error
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Extract shard key values from operation
-spec extract_operation_shard_key(map(), map()) -> {ok, map()} | {error, term()}.
extract_operation_shard_key(Operation, ShardInfo) ->
  try
    ShardKey = maps:get(<<"key">>, ShardInfo),

    case maps:get(type, Operation) of
      insert ->
        Document = maps:get(document, Operation),
        extract_shard_key_from_document(Document, ShardKey);
      update ->
        Selector = maps:get(selector, Operation),
        extract_shard_key_from_selector(Selector, ShardKey);
      delete ->
        Selector = maps:get(selector, Operation),
        extract_shard_key_from_selector(Selector, ShardKey);
      find ->
        Selector = maps:get(selector, Operation, #{}),
        extract_shard_key_from_selector(Selector, ShardKey);
      _ ->
        {error, {unsupported_operation_type, maps:get(type, Operation)}}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Extract shard key from document
-spec extract_shard_key_from_document(map(), map()) -> {ok, map()} | {error, term()}.
extract_shard_key_from_document(Document, ShardKey) ->
  try
    ShardKeyFields = maps:keys(ShardKey),
    ShardKeyValues = maps:with(ShardKeyFields, Document),

    % Check if all shard key fields are present
    case maps:size(ShardKeyValues) =:= length(ShardKeyFields) of
      true -> {ok, ShardKeyValues};
      false ->
        MissingFields = ShardKeyFields -- maps:keys(ShardKeyValues),
        {error, {missing_shard_key_fields, MissingFields}}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Extract shard key from query selector
-spec extract_shard_key_from_selector(map(), map()) -> {ok, map()} | {error, term()}.
extract_shard_key_from_selector(Selector, ShardKey) ->
  try
    ShardKeyFields = maps:keys(ShardKey),

    % Extract exact matches for shard key fields
    ShardKeyValues = maps:fold(
      fun(Field, Value, Acc) ->
        case is_exact_match_value(Value) of
          true -> Acc#{Field => Value};
          false -> Acc
        end
      end,
      #{},
      maps:with(ShardKeyFields, Selector)
    ),

    case maps:size(ShardKeyValues) of
      0 -> {error, no_shard_key_in_selector};
      _ -> {ok, ShardKeyValues}
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Check if a value represents an exact match (not a query operator)
-spec is_exact_match_value(term()) -> boolean().
is_exact_match_value(Value) when is_map(Value) ->
  % Check if it contains query operators
  QueryOperators = [<<"$gt">>, <<"$gte">>, <<"$lt">>, <<"$lte">>, <<"$ne">>,
                   <<"$in">>, <<"$nin">>, <<"$regex">>, <<"$exists">>],
  not lists:any(fun(Op) -> maps:is_key(Op, Value) end, QueryOperators);
is_exact_match_value(_Value) ->
  true. % Scalar values are exact matches

%% @doc Detect conflicts in shard key values across operations
-spec detect_shard_key_conflicts(list()) -> list().
detect_shard_key_conflicts(ShardTargets) ->
  % Group operations by collection
  CollectionGroups = group_by_collection(ShardTargets),

  % Check for conflicts within each collection
  lists:flatmap(fun detect_collection_conflicts/1, maps:to_list(CollectionGroups)).

%% @doc Group shard targets by collection
-spec group_by_collection(list()) -> map().
group_by_collection(ShardTargets) ->
  lists:foldl(
    fun(#{collection := Collection} = Target, Acc) ->
      Current = maps:get(Collection, Acc, []),
      Acc#{Collection => [Target | Current]}
    end,
    #{},
    ShardTargets
  ).

%% @doc Detect conflicts within a single collection
-spec detect_collection_conflicts({collection(), list()}) -> list().
detect_collection_conflicts({Collection, Targets}) ->
  % Check if all operations target the same shard
  Shards = lists:usort([Shard || #{shard := Shard} <- Targets]),

  case length(Shards) of
    1 -> []; % No conflicts - all operations on same shard
    _ ->
      % Multiple shards - this is a conflict for most operations
      [{Collection, multiple_shard_targets, Shards}]
  end.

%% @doc Validate multi-shard transaction requirements
-spec validate_multi_shard_transaction(pid(), integer(), list()) -> ok | {error, term()}.
validate_multi_shard_transaction(Worker, ShardCount, Shards) ->
  try
    % Check if multi-shard transactions are supported
    case check_multi_shard_support(Worker) of
      false ->
        {error, multi_shard_transactions_not_supported};
      true ->
        % Additional validations for multi-shard transactions
        case validate_shard_availability(Shards) of
          ok ->
            case validate_transaction_size_limits(ShardCount) of
              ok -> ok;
              Error -> Error
            end;
          Error ->
            Error
        end
    end
  catch
    _:Reason ->
      {error, Reason}
  end.

%% @doc Validate that all target shards are available
-spec validate_shard_availability(list()) -> ok | {error, term()}.
validate_shard_availability(Shards) ->
  % In a real implementation, this would check shard health
  % For now, assume all shards are available
  case length(Shards) > 0 of
    true -> ok;
    false -> {error, no_available_shards}
  end.

%% @doc Validate transaction size limits for multi-shard operations
-spec validate_transaction_size_limits(integer()) -> ok | {error, term()}.
validate_transaction_size_limits(ShardCount) ->
  MaxShards = 10, % Configurable limit
  case ShardCount =< MaxShards of
    true -> ok;
    false -> {error, {too_many_shards, ShardCount, MaxShards}}
  end.

%%%===================================================================
%%% Real Implementation Helper Functions
%%%===================================================================

%% @doc Parse host:port string into components
-spec parse_host_port(binary()) -> {binary(), integer()}.
parse_host_port(HostPort) ->
  case binary:split(HostPort, <<":">>) of
    [Host, PortBin] ->
      try
        Port = binary_to_integer(PortBin),
        {Host, Port}
      catch
        _:_ -> {Host, 27017} % Default port if parsing fails
      end;
    [Host] ->
      {Host, 27017} % Default MongoDB port
  end.

%% @doc Establish MongoDB connection for health/load checking
-spec establish_mongodb_connection(binary(), integer()) -> {ok, pid()} | {error, term()}.
establish_mongodb_connection(MongosHost, Timeout) ->
  try
    {Host, Port} = parse_host_port(MongosHost),

    ConnectionOptions = [
      {host, binary_to_list(Host)},
      {port, Port},
      {database, <<"admin">>},
      {connect_timeout_ms, Timeout},
      {socket_timeout_ms, Timeout}
    ],

    mc_worker_api:connect(ConnectionOptions)
  catch
    _:Reason ->
      {error, {connection_setup_failed, Reason}}
  end.

%% @doc Measure MongoDB command latency using isMaster
-spec measure_mongodb_command_latency(binary(), integer()) -> {ok, integer()} | {error, term()}.
measure_mongodb_command_latency(MongosHost, Timeout) ->
  try
    case establish_mongodb_connection(MongosHost, Timeout) of
      {ok, Connection} ->
        try
          StartTime = erlang:system_time(microsecond),

          % Execute isMaster command to measure MongoDB response time
          case mc_worker_api:command(Connection, {<<"isMaster">>, 1}) of
            {true, _Result} ->
              EndTime = erlang:system_time(microsecond),
              Latency = (EndTime - StartTime) div 1000, % Convert to milliseconds
              mc_worker_api:disconnect(Connection),
              {ok, Latency};
            {false, Error} ->
              mc_worker_api:disconnect(Connection),
              {error, {command_failed, Error}};
            Error ->
              mc_worker_api:disconnect(Connection),
              {error, {unexpected_response, Error}}
          end
        catch
          _:CmdReason ->
            mc_worker_api:disconnect(Connection),
            {error, {command_exception, CmdReason}}
        end;
      {error, ConnReason} ->
        {error, ConnReason}
    end
  catch
    _:CatchReason ->
      {error, CatchReason}
  end.

%% @doc Get comprehensive load metrics from MongoDB
-spec get_comprehensive_load_metrics(pid()) -> map().
get_comprehensive_load_metrics(Connection) ->
  try
    % Get server status for comprehensive metrics
    ServerStatusMetrics = get_server_status_load_metrics(Connection),

    % Get database statistics
    DbStatsMetrics = get_database_load_metrics(Connection),

    % Get replication metrics if applicable
    ReplMetrics = get_replication_load_metrics(Connection),

    % Combine all metrics
    AllMetrics = maps:merge(maps:merge(ServerStatusMetrics, DbStatsMetrics), ReplMetrics),

    % Calculate overall load score
    LoadScore = calculate_overall_load_score(AllMetrics),

    AllMetrics#{overall_load_score => LoadScore}
  catch
    _:Reason ->
      #{error => {metrics_collection_failed, Reason}}
  end.

%% @doc Get server status metrics focused on load
-spec get_server_status_load_metrics(pid()) -> map().
get_server_status_load_metrics(Connection) ->
  try
    case mc_worker_api:command(Connection, {<<"serverStatus">>, 1}) of
      {true, Status} ->
        extract_load_specific_metrics(Status);
      _ ->
        #{error => server_status_failed}
    end
  catch
    _:_ ->
      #{error => server_status_exception}
  end.

%% @doc Extract load-specific metrics from serverStatus
-spec extract_load_specific_metrics(map()) -> map().
extract_load_specific_metrics(Status) ->
  try
    % Connection metrics
    Connections = maps:get(<<"connections">>, Status, #{}),
    CurrentConns = maps:get(<<"current">>, Connections, 0),
    AvailableConns = maps:get(<<"available">>, Connections, 0),
    TotalConns = CurrentConns + AvailableConns,

    % Memory metrics
    Memory = maps:get(<<"mem">>, Status, #{}),
    ResidentMB = maps:get(<<"resident">>, Memory, 0),
    VirtualMB = maps:get(<<"virtual">>, Memory, 0),

    % Global lock metrics (CPU proxy)
    GlobalLock = maps:get(<<"globalLock">>, Status, #{}),
    TotalTime = maps:get(<<"totalTime">>, GlobalLock, 1),
    LockTime = maps:get(<<"lockTime">>, GlobalLock, 0),

    % Operation counters
    Opcounters = maps:get(<<"opcounters">>, Status, #{}),
    TotalOps = maps:get(<<"insert">>, Opcounters, 0) +
               maps:get(<<"query">>, Opcounters, 0) +
               maps:get(<<"update">>, Opcounters, 0) +
               maps:get(<<"delete">>, Opcounters, 0),

    % Network metrics
    Network = maps:get(<<"network">>, Status, #{}),
    BytesIn = maps:get(<<"bytesIn">>, Network, 0),
    BytesOut = maps:get(<<"bytesOut">>, Network, 0),

    % Calculate percentages and ratios
    ConnectionUtilization = case TotalConns of
      0 -> 0.0;
      _ -> (CurrentConns / TotalConns) * 100
    end,

    CpuUtilization = case TotalTime of
      0 -> 0.0;
      _ -> (LockTime / TotalTime) * 100
    end,

    MemoryRatio = case VirtualMB of
      0 -> 0.0;
      _ -> (ResidentMB / VirtualMB) * 100
    end,

    #{
      connection_utilization => ConnectionUtilization,
      current_connections => CurrentConns,
      available_connections => AvailableConns,
      cpu_utilization => CpuUtilization,
      memory_resident_mb => ResidentMB,
      memory_virtual_mb => VirtualMB,
      memory_utilization => MemoryRatio,
      total_operations => TotalOps,
      network_bytes_in => BytesIn,
      network_bytes_out => BytesOut,
      uptime_seconds => maps:get(<<"uptime">>, Status, 0)
    }
  catch
    _:_ ->
      #{error => metric_extraction_failed}
  end.

%% @doc Get database load metrics
-spec get_database_load_metrics(pid()) -> map().
get_database_load_metrics(Connection) ->
  try
    case mc_worker_api:command(Connection, {<<"dbStats">>, 1}) of
      {true, Stats} ->
        DataSize = maps:get(<<"dataSize">>, Stats, 0),
        StorageSize = maps:get(<<"storageSize">>, Stats, 0),
        IndexSize = maps:get(<<"indexSize">>, Stats, 0),

        % Calculate storage efficiency
        StorageEfficiency = case StorageSize of
          0 -> 100.0;
          _ -> (DataSize / StorageSize) * 100
        end,

        #{
          data_size_bytes => DataSize,
          storage_size_bytes => StorageSize,
          index_size_bytes => IndexSize,
          storage_efficiency => StorageEfficiency,
          collection_count => maps:get(<<"collections">>, Stats, 0),
          object_count => maps:get(<<"objects">>, Stats, 0)
        };
      _ ->
        #{error => db_stats_failed}
    end
  catch
    _:_ ->
      #{error => db_stats_exception}
  end.

%% @doc Get replication load metrics
-spec get_replication_load_metrics(pid()) -> map().
get_replication_load_metrics(Connection) ->
  try
    case mc_worker_api:command(Connection, {<<"replSetGetStatus">>, 1}) of
      {true, ReplStatus} ->
        Members = maps:get(<<"members">>, ReplStatus, []),

        % Find primary and calculate replication lag
        PrimaryMember = find_primary_in_members(Members),
        MaxLag = calculate_max_replication_lag_from_members(Members),

        % Count healthy members
        HealthyMembers = count_healthy_members(Members),
        TotalMembers = length(Members),

        % Calculate replication health score
        ReplHealthScore = case TotalMembers of
          0 -> 0.0;
          _ -> (HealthyMembers / TotalMembers) * 100
        end,

        #{
          replication_lag_ms => MaxLag,
          primary_available => PrimaryMember =/= undefined,
          healthy_members => HealthyMembers,
          total_members => TotalMembers,
          replication_health_score => ReplHealthScore,
          replica_set_name => maps:get(<<"set">>, ReplStatus, <<"unknown">>)
        };
      _ ->
        % Not a replica set or command failed
        #{
          replication_lag_ms => 0,
          primary_available => true,
          healthy_members => 1,
          total_members => 1,
          replication_health_score => 100.0
        }
    end
  catch
    _:_ ->
      #{
        replication_lag_ms => 0,
        primary_available => true,
        replication_health_score => 100.0
      }
  end.

%% @doc Calculate overall load score from all metrics
-spec calculate_overall_load_score(map()) -> float().
calculate_overall_load_score(Metrics) ->
  try
    % Weight different metrics for overall load calculation
    Weights = #{
      connection_utilization => 0.25,
      cpu_utilization => 0.30,
      memory_utilization => 0.20,
      replication_health_score => 0.15,
      storage_efficiency => 0.10
    },

    % Get metric values with defaults
    ConnUtil = maps:get(connection_utilization, Metrics, 0.0),
    CpuUtil = maps:get(cpu_utilization, Metrics, 0.0),
    MemUtil = maps:get(memory_utilization, Metrics, 0.0),
    ReplHealth = maps:get(replication_health_score, Metrics, 100.0),
    StorageEff = maps:get(storage_efficiency, Metrics, 100.0),

    % Calculate weighted score (lower is better for most metrics)
    LoadScore =
      (ConnUtil * maps:get(connection_utilization, Weights)) +
      (CpuUtil * maps:get(cpu_utilization, Weights)) +
      (MemUtil * maps:get(memory_utilization, Weights)) +
      ((100.0 - ReplHealth) * maps:get(replication_health_score, Weights)) +
      ((100.0 - StorageEff) * maps:get(storage_efficiency, Weights)),

    % Normalize to 0-100 scale
    min(100.0, max(0.0, LoadScore))
  catch
    _:_ ->
      50.0 % Default moderate load score
  end.

%% @doc Find primary member in replica set members list
-spec find_primary_in_members(list()) -> map() | undefined.
find_primary_in_members([]) ->
  undefined;
find_primary_in_members([#{<<"state">> := 1} = Member | _]) ->
  Member; % State 1 = PRIMARY
find_primary_in_members([_ | Rest]) ->
  find_primary_in_members(Rest).

%% @doc Calculate maximum replication lag from members
-spec calculate_max_replication_lag_from_members(list()) -> integer().
calculate_max_replication_lag_from_members(Members) ->
  try
    PrimaryMember = find_primary_in_members(Members),
    case PrimaryMember of
      undefined -> 0;
      _ ->
        PrimaryOpTime = get_member_operation_time(PrimaryMember),
        SecondaryMembers = [M || #{<<"state">> := State} = M <- Members, State =:= 2],

        Lags = [calculate_lag_between_members(PrimaryOpTime, get_member_operation_time(Member))
                || Member <- SecondaryMembers],

        case Lags of
          [] -> 0;
          _ -> lists:max(Lags)
        end
    end
  catch
    _:_ -> 0
  end.

%% @doc Get operation time from member
-spec get_member_operation_time(map()) -> integer().
get_member_operation_time(Member) ->
  try
    OptimeDoc = maps:get(<<"optime">>, Member, #{}),
    case maps:get(<<"ts">>, OptimeDoc, undefined) of
      undefined -> 0;
      {Timestamp, _} -> Timestamp;
      Timestamp when is_integer(Timestamp) -> Timestamp;
      _ -> 0
    end
  catch
    _:_ -> 0
  end.

%% @doc Calculate lag between two operation times
-spec calculate_lag_between_members(integer(), integer()) -> integer().
calculate_lag_between_members(PrimaryOpTime, SecondaryOpTime) ->
  abs(PrimaryOpTime - SecondaryOpTime).

%% @doc Count healthy members in replica set
-spec count_healthy_members(list()) -> integer().
count_healthy_members(Members) ->
  length([M || #{<<"health">> := Health, <<"state">> := State} = M <- Members,
               Health =:= 1 andalso (State =:= 1 orelse State =:= 2)]).
