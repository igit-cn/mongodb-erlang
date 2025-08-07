%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% Enhanced MongoDB Aggregation Pipeline support for MongoDB 4.x+
%%% Includes new operators and features introduced in MongoDB 4.x and later
%%% @end
%%%-------------------------------------------------------------------
-module(mc_aggregation).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% API
-export([
  aggregate/3, aggregate/4,
  pipeline/0,
  add_stage/2,
  match/2,
  project/2,
  group/2,
  sort/2,
  limit/2,
  skip/2,
  unwind/2, unwind/3,
  lookup/2,
  facet/2,
  bucket/2,
  bucket_auto/2,
  count/2,
  add_fields/2,
  replace_root/2,
  sample/2,
  out/2,
  merge/2,
  % MongoDB 4.x+ specific stages
  set/2,
  unset/2,
  replace_with/2,
  % MongoDB 4.x+ operators
  convert/1,
  to_bool/1,
  to_date/1,
  to_decimal/1,
  to_double/1,
  to_int/1,
  to_long/1,
  to_object_id/1,
  to_string/1,
  % Array operators
  array_elem_at/2,
  array_to_object/1,
  object_to_array/1,
  % Date operators
  date_from_parts/1,
  date_to_parts/1,
  iso_date_from_parts/1,
  iso_date_to_parts/1,
  % String operators
  ltrim/1, ltrim/2,
  rtrim/1, rtrim/2,
  trim/1, trim/2,
  regex_find/2,
  regex_find_all/2,
  regex_match/2,
  % MongoDB 5.0+ operators
  tsIncrement/1,
  tsSecond/1,
  % MongoDB 4.4+ operators
  first/1,
  last/1,
  push/1,
  push/2,
  add_to_set/1,
  merge_objects/1,
  % Conditional operators
  switch/1,
  'cond'/1,
  if_null/2,
  % Math operators
  round/1,
  round/2,
  trunc/1,
  trunc/2,
  % Utility functions
  build_pipeline/1,
  validate_pipeline/1,
  optimize_pipeline/1
]).

%% Types
-type pipeline() :: [map()].
-type stage() :: map().
-type aggregation_options() :: #{
  allow_disk_use => boolean(),
  max_time_ms => integer(),
  batch_size => integer(),
  read_concern => map(),
  read_preference => map(),
  collation => map(),
  hint => binary() | map(),
  comment => binary(),
  'let' => map()
}.

-export_type([pipeline/0, stage/0, aggregation_options/0]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Execute aggregation pipeline
-spec aggregate(pid(), binary(), pipeline()) -> {ok, pid()} | {error, term()}.
aggregate(Connection, Collection, Pipeline) ->
  aggregate(Connection, Collection, Pipeline, #{}).

-spec aggregate(pid(), binary(), pipeline(), aggregation_options()) -> {ok, pid()} | {error, term()}.
aggregate(Connection, Collection, Pipeline, Options) ->
  Command = build_aggregate_command(Collection, Pipeline, Options),
  case mc_worker_api:command(Connection, Command) of
    {true, #{<<"cursor">> := CursorInfo}} ->
      create_cursor_from_response(Connection, CursorInfo);
    {false, Error} ->
      {error, Error};
    Error ->
      {error, Error}
  end.

%% @doc Create empty pipeline
-spec pipeline() -> pipeline().
pipeline() ->
  [].

%% @doc Add stage to pipeline
-spec add_stage(pipeline(), stage()) -> pipeline().
add_stage(Pipeline, Stage) ->
  Pipeline ++ [Stage].

%% @doc Add $match stage
-spec match(pipeline(), map()) -> pipeline().
match(Pipeline, Conditions) ->
  add_stage(Pipeline, #{<<"$match">> => Conditions}).

%% @doc Add $project stage
-spec project(pipeline(), map()) -> pipeline().
project(Pipeline, Projection) ->
  add_stage(Pipeline, #{<<"$project">> => Projection}).

%% @doc Add $group stage
-spec group(pipeline(), map()) -> pipeline().
group(Pipeline, GroupSpec) ->
  add_stage(Pipeline, #{<<"$group">> => GroupSpec}).

%% @doc Add $sort stage
-spec sort(pipeline(), map()) -> pipeline().
sort(Pipeline, SortSpec) ->
  add_stage(Pipeline, #{<<"$sort">> => SortSpec}).

%% @doc Add $limit stage
-spec limit(pipeline(), integer()) -> pipeline().
limit(Pipeline, Count) ->
  add_stage(Pipeline, #{<<"$limit">> => Count}).

%% @doc Add $skip stage
-spec skip(pipeline(), integer()) -> pipeline().
skip(Pipeline, Count) ->
  add_stage(Pipeline, #{<<"$skip">> => Count}).

%% @doc Add $unwind stage
-spec unwind(pipeline(), binary()) -> pipeline().
unwind(Pipeline, Path) ->
  add_stage(Pipeline, #{<<"$unwind">> => Path}).

-spec unwind(pipeline(), binary(), map()) -> pipeline().
unwind(Pipeline, Path, Options) ->
  UnwindSpec = maps:merge(#{<<"path">> => Path}, Options),
  add_stage(Pipeline, #{<<"$unwind">> => UnwindSpec}).

%% @doc Add $lookup stage
-spec lookup(pipeline(), map()) -> pipeline().
lookup(Pipeline, LookupSpec) ->
  add_stage(Pipeline, #{<<"$lookup">> => LookupSpec}).

%% @doc Add $facet stage
-spec facet(pipeline(), map()) -> pipeline().
facet(Pipeline, FacetSpec) ->
  add_stage(Pipeline, #{<<"$facet">> => FacetSpec}).

%% @doc Add $bucket stage
-spec bucket(pipeline(), map()) -> pipeline().
bucket(Pipeline, BucketSpec) ->
  add_stage(Pipeline, #{<<"$bucket">> => BucketSpec}).

%% @doc Add $bucketAuto stage
-spec bucket_auto(pipeline(), map()) -> pipeline().
bucket_auto(Pipeline, BucketAutoSpec) ->
  add_stage(Pipeline, #{<<"$bucketAuto">> => BucketAutoSpec}).

%% @doc Add $count stage
-spec count(pipeline(), binary()) -> pipeline().
count(Pipeline, CountName) ->
  add_stage(Pipeline, #{<<"$count">> => CountName}).

%% @doc Add $addFields stage
-spec add_fields(pipeline(), map()) -> pipeline().
add_fields(Pipeline, Fields) ->
  add_stage(Pipeline, #{<<"$addFields">> => Fields}).

%% @doc Add $replaceRoot stage
-spec replace_root(pipeline(), map()) -> pipeline().
replace_root(Pipeline, ReplaceSpec) ->
  add_stage(Pipeline, #{<<"$replaceRoot">> => ReplaceSpec}).

%% @doc Add $sample stage
-spec sample(pipeline(), integer()) -> pipeline().
sample(Pipeline, Size) ->
  add_stage(Pipeline, #{<<"$sample">> => #{<<"size">> => Size}}).

%% @doc Add $out stage
-spec out(pipeline(), binary()) -> pipeline().
out(Pipeline, Collection) ->
  add_stage(Pipeline, #{<<"$out">> => Collection}).

%% @doc Add $merge stage (MongoDB 4.2+)
-spec merge(pipeline(), map()) -> pipeline().
merge(Pipeline, MergeSpec) ->
  add_stage(Pipeline, #{<<"$merge">> => MergeSpec}).

%% MongoDB 4.x+ specific stages

%% @doc Add $set stage (MongoDB 4.2+, alias for $addFields)
-spec set(pipeline(), map()) -> pipeline().
set(Pipeline, Fields) ->
  add_stage(Pipeline, #{<<"$set">> => Fields}).

%% @doc Add $unset stage (MongoDB 4.2+)
-spec unset(pipeline(), binary() | [binary()]) -> pipeline().
unset(Pipeline, Fields) ->
  add_stage(Pipeline, #{<<"$unset">> => Fields}).

%% @doc Add $replaceWith stage (MongoDB 4.2+)
-spec replace_with(pipeline(), map()) -> pipeline().
replace_with(Pipeline, Expression) ->
  add_stage(Pipeline, #{<<"$replaceWith">> => Expression}).

%% MongoDB 4.x+ operators

%% @doc $convert operator
-spec convert(map()) -> map().
convert(ConvertSpec) ->
  #{<<"$convert">> => ConvertSpec}.

%% @doc $toBool operator
-spec to_bool(term()) -> map().
to_bool(Expression) ->
  #{<<"$toBool">> => Expression}.

%% @doc $toDate operator
-spec to_date(term()) -> map().
to_date(Expression) ->
  #{<<"$toDate">> => Expression}.

%% @doc $toDecimal operator
-spec to_decimal(term()) -> map().
to_decimal(Expression) ->
  #{<<"$toDecimal">> => Expression}.

%% @doc $toDouble operator
-spec to_double(term()) -> map().
to_double(Expression) ->
  #{<<"$toDouble">> => Expression}.

%% @doc $toInt operator
-spec to_int(term()) -> map().
to_int(Expression) ->
  #{<<"$toInt">> => Expression}.

%% @doc $toLong operator
-spec to_long(term()) -> map().
to_long(Expression) ->
  #{<<"$toLong">> => Expression}.

%% @doc $toObjectId operator
-spec to_object_id(term()) -> map().
to_object_id(Expression) ->
  #{<<"$toObjectId">> => Expression}.

%% @doc $toString operator
-spec to_string(term()) -> map().
to_string(Expression) ->
  #{<<"$toString">> => Expression}.

%% Array operators

%% @doc $arrayElemAt operator
-spec array_elem_at(term(), term()) -> map().
array_elem_at(Array, Index) ->
  #{<<"$arrayElemAt">> => [Array, Index]}.

%% @doc $arrayToObject operator
-spec array_to_object(term()) -> map().
array_to_object(Expression) ->
  #{<<"$arrayToObject">> => Expression}.

%% @doc $objectToArray operator
-spec object_to_array(term()) -> map().
object_to_array(Expression) ->
  #{<<"$objectToArray">> => Expression}.

%% Date operators

%% @doc $dateFromParts operator
-spec date_from_parts(map()) -> map().
date_from_parts(DateParts) ->
  #{<<"$dateFromParts">> => DateParts}.

%% @doc $dateToParts operator
-spec date_to_parts(term()) -> map().
date_to_parts(Expression) ->
  #{<<"$dateToParts">> => Expression}.

%% @doc $isoDateFromParts operator
-spec iso_date_from_parts(map()) -> map().
iso_date_from_parts(DateParts) ->
  #{<<"$isoDateFromParts">> => DateParts}.

%% @doc $isoDateToParts operator
-spec iso_date_to_parts(term()) -> map().
iso_date_to_parts(Expression) ->
  #{<<"$isoDateToParts">> => Expression}.

%% String operators

%% @doc $ltrim operator
-spec ltrim(term()) -> map().
ltrim(Expression) ->
  #{<<"$ltrim">> => #{<<"input">> => Expression}}.

-spec ltrim(term(), term()) -> map().
ltrim(Expression, Chars) ->
  #{<<"$ltrim">> => #{<<"input">> => Expression, <<"chars">> => Chars}}.

%% @doc $rtrim operator
-spec rtrim(term()) -> map().
rtrim(Expression) ->
  #{<<"$rtrim">> => #{<<"input">> => Expression}}.

-spec rtrim(term(), term()) -> map().
rtrim(Expression, Chars) ->
  #{<<"$rtrim">> => #{<<"input">> => Expression, <<"chars">> => Chars}}.

%% @doc $trim operator
-spec trim(term()) -> map().
trim(Expression) ->
  #{<<"$trim">> => #{<<"input">> => Expression}}.

-spec trim(term(), term()) -> map().
trim(Expression, Chars) ->
  #{<<"$trim">> => #{<<"input">> => Expression, <<"chars">> => Chars}}.

%% @doc $regexFind operator
-spec regex_find(term(), term()) -> map().
regex_find(Input, Regex) ->
  #{<<"$regexFind">> => #{<<"input">> => Input, <<"regex">> => Regex}}.

%% @doc $regexFindAll operator
-spec regex_find_all(term(), term()) -> map().
regex_find_all(Input, Regex) ->
  #{<<"$regexFindAll">> => #{<<"input">> => Input, <<"regex">> => Regex}}.

%% @doc $regexMatch operator
-spec regex_match(term(), term()) -> map().
regex_match(Input, Regex) ->
  #{<<"$regexMatch">> => #{<<"input">> => Input, <<"regex">> => Regex}}.

%% MongoDB 5.0+ operators

%% @doc $tsIncrement operator
-spec tsIncrement(term()) -> map().
tsIncrement(Expression) ->
  #{<<"$tsIncrement">> => Expression}.

%% @doc $tsSecond operator
-spec tsSecond(term()) -> map().
tsSecond(Expression) ->
  #{<<"$tsSecond">> => Expression}.

%% MongoDB 4.4+ operators

%% @doc $first operator
-spec first(term()) -> map().
first(Expression) ->
  #{<<"$first">> => Expression}.

%% @doc $last operator
-spec last(term()) -> map().
last(Expression) ->
  #{<<"$last">> => Expression}.

%% @doc $push operator
-spec push(term()) -> map().
push(Expression) ->
  #{<<"$push">> => Expression}.

-spec push(term(), map()) -> map().
push(Expression, Options) ->
  #{<<"$push">> => maps:merge(#{<<"$each">> => Expression}, Options)}.

%% @doc $addToSet operator
-spec add_to_set(term()) -> map().
add_to_set(Expression) ->
  #{<<"$addToSet">> => Expression}.

%% @doc $mergeObjects operator
-spec merge_objects(term()) -> map().
merge_objects(Expression) ->
  #{<<"$mergeObjects">> => Expression}.

%% Conditional operators

%% @doc $switch operator
-spec switch(map()) -> map().
switch(SwitchSpec) ->
  #{<<"$switch">> => SwitchSpec}.

%% @doc $cond operator
-spec 'cond'([term()]) -> map().
'cond'([If, Then, Else]) ->
  #{<<"$cond">> => [If, Then, Else]}.

%% @doc $ifNull operator
-spec if_null(term(), term()) -> map().
if_null(Expression, Replacement) ->
  #{<<"$ifNull">> => [Expression, Replacement]}.

%% Math operators

%% @doc $round operator
-spec round(term()) -> map().
round(Expression) ->
  #{<<"$round">> => Expression}.

-spec round(term(), term()) -> map().
round(Expression, Place) ->
  #{<<"$round">> => [Expression, Place]}.

%% @doc $trunc operator
-spec trunc(term()) -> map().
trunc(Expression) ->
  #{<<"$trunc">> => Expression}.

-spec trunc(term(), term()) -> map().
trunc(Expression, Place) ->
  #{<<"$trunc">> => [Expression, Place]}.

%% Utility functions

%% @doc Build pipeline from list of operations
-spec build_pipeline([{atom(), term()}]) -> pipeline().
build_pipeline(Operations) ->
  lists:foldl(fun({Op, Args}, Acc) ->
    case Op of
      match -> match(Acc, Args);
      project -> project(Acc, Args);
      group -> group(Acc, Args);
      sort -> sort(Acc, Args);
      limit -> limit(Acc, Args);
      skip -> skip(Acc, Args);
      unwind -> unwind(Acc, Args);
      lookup -> lookup(Acc, Args);
      facet -> facet(Acc, Args);
      bucket -> bucket(Acc, Args);
      bucket_auto -> bucket_auto(Acc, Args);
      count -> count(Acc, Args);
      add_fields -> add_fields(Acc, Args);
      replace_root -> replace_root(Acc, Args);
      sample -> sample(Acc, Args);
      out -> out(Acc, Args);
      merge -> merge(Acc, Args);
      set -> set(Acc, Args);
      unset -> unset(Acc, Args);
      replace_with -> replace_with(Acc, Args);
      _ -> add_stage(Acc, #{atom_to_binary(Op, utf8) => Args})
    end
  end, pipeline(), Operations).

%% @doc Validate pipeline stages
-spec validate_pipeline(pipeline()) -> {ok, pipeline()} | {error, term()}.
validate_pipeline(Pipeline) ->
  try
    ValidatedStages = [validate_stage(Stage) || Stage <- Pipeline],
    {ok, ValidatedStages}
  catch
    _:Reason ->
      {error, {invalid_pipeline, Reason}}
  end.

%% @doc Optimize pipeline for better performance
-spec optimize_pipeline(pipeline()) -> pipeline().
optimize_pipeline(Pipeline) ->
  % Basic optimizations
  Pipeline1 = move_match_stages_early(Pipeline),
  Pipeline2 = combine_consecutive_matches(Pipeline1),
  Pipeline3 = remove_unnecessary_projects(Pipeline2),
  Pipeline3.

%% @private
validate_stage(Stage) when is_map(Stage) ->
  case maps:size(Stage) of
    1 -> Stage;
    _ -> throw({invalid_stage, multiple_operators, Stage})
  end;
validate_stage(Stage) ->
  throw({invalid_stage, not_map, Stage}).

%% @private
move_match_stages_early(Pipeline) ->
  {MatchStages, OtherStages} = lists:partition(fun(Stage) ->
    maps:is_key(<<"$match">>, Stage)
  end, Pipeline),
  MatchStages ++ OtherStages.

%% @private
combine_consecutive_matches([#{<<"$match">> := Match1}, #{<<"$match">> := Match2} | Rest]) ->
  CombinedMatch = maps:merge(Match1, Match2),
  combine_consecutive_matches([#{<<"$match">> => CombinedMatch} | Rest]);
combine_consecutive_matches([Stage | Rest]) ->
  [Stage | combine_consecutive_matches(Rest)];
combine_consecutive_matches([]) ->
  [].

%% @private
remove_unnecessary_projects(Pipeline) ->
  % Remove $project stages that don't actually change the document structure
  lists:filter(fun(#{<<"$project">> := Projection}) ->
    not is_identity_projection(Projection);
    (_) -> true
  end, Pipeline).

%% @private
is_identity_projection(Projection) ->
  % Check if projection is just {_id: 1} or similar identity projections
  maps:fold(fun
    (<<"_id">>, 1, Acc) -> Acc;
    (<<"_id">>, true, Acc) -> Acc;
    (_, 1, _) -> false;
    (_, true, _) -> false;
    (_, _, _) -> false
  end, true, Projection).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
build_aggregate_command(Collection, Pipeline, Options) ->
  BaseCommand = #{
    <<"aggregate">> => Collection,
    <<"pipeline">> => Pipeline,
    <<"cursor">> => build_cursor_options(Options)
  },
  
  % Add optional parameters
  lists:foldl(fun({Key, CommandKey}, Acc) ->
    case maps:get(Key, Options, undefined) of
      undefined -> Acc;
      Value -> maps:put(CommandKey, Value, Acc)
    end
  end, BaseCommand, [
    {allow_disk_use, <<"allowDiskUse">>},
    {max_time_ms, <<"maxTimeMS">>},
    {read_concern, <<"readConcern">>},
    {collation, <<"collation">>},
    {hint, <<"hint">>},
    {comment, <<"comment">>},
    {'let', <<"let">>}
  ]).

%% @private
build_cursor_options(Options) ->
  BatchSize = maps:get(batch_size, Options, 101),
  #{<<"batchSize">> => BatchSize}.

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
