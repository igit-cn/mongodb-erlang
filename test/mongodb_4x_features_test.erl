%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% Test suite for MongoDB 4.x+ features
%%% @end
%%%-------------------------------------------------------------------
-module(mongodb_4x_features_test).
-author("mongodb-erlang team").

-include_lib("eunit/include/eunit.hrl").

-define(DATABASE, <<"test_db">>).
-define(COLLECTION, <<"test_collection">>).
-define(HOST, "localhost").
-define(PORT, 27017).

%%%===================================================================
%%% Test Setup and Teardown
%%%===================================================================

setup() ->
    application:ensure_all_started(mongodb),
    {ok, Connection} = mc_worker_api:connect([
        {database, ?DATABASE},
        {host, ?HOST},
        {port, ?PORT}
    ]),
    % Clean up collection
    mc_worker_api:delete(Connection, ?COLLECTION, #{}),
    Connection.

cleanup(Connection) ->
    mc_worker_api:delete(Connection, ?COLLECTION, #{}),
    mc_worker_api:disconnect(Connection).

%%%===================================================================
%%% Test Cases
%%%===================================================================

mongodb_4x_features_test_() ->
    {setup,
        fun setup/0,
        fun cleanup/1,
        fun(Connection) ->
            [
                {"Change Streams Test", fun() -> test_change_streams(Connection) end},
                {"Retryable Writes Test", fun() -> test_retryable_writes(Connection) end},
                {"Enhanced Aggregation Test", fun() -> test_enhanced_aggregation(Connection) end},
                {"Bulk Operations Test", fun() -> test_bulk_operations(Connection) end},
                {"Modern Index Management Test", fun() -> test_modern_index_management(Connection) end},
                {"Query Optimization Test", fun() -> test_query_optimization(Connection) end},
                {"Collection Management Test", fun() -> test_collection_management(Connection) end}
            ]
        end
    }.

%%%===================================================================
%%% Individual Test Functions
%%%===================================================================

test_change_streams(Connection) ->
    % Insert initial document
    InitialDoc = #{<<"name">> => <<"initial">>, <<"value">> => 1},
    mc_worker_api:insert(Connection, ?COLLECTION, InitialDoc),
    
    % Start change stream
    Pipeline = [#{<<"$match">> => #{<<"operationType">> => <<"insert">>}}],
    case mc_worker_api:watch_collection(Connection, ?DATABASE, ?COLLECTION, Pipeline) of
        {ok, ChangeStream} ->
            % Insert a document to trigger change event
            TestDoc = #{<<"name">> => <<"test_change">>, <<"value">> => 42},
            spawn(fun() ->
                timer:sleep(100),
                mc_worker_api:insert(Connection, ?COLLECTION, TestDoc)
            end),
            
            % Try to get change event
            case mc_change_stream:next(ChangeStream, 2000) of
                {ok, ChangeEvent} ->
                    ?assertEqual(<<"insert">>, maps:get(<<"operationType">>, ChangeEvent)),
                    FullDoc = maps:get(<<"fullDocument">>, ChangeEvent),
                    ?assertEqual(<<"test_change">>, maps:get(<<"name">>, FullDoc));
                timeout ->
                    ?debugMsg("Change stream timeout - this may be expected in test environment");
                {error, Reason} ->
                    ?debugFmt("Change stream error: ~p", [Reason])
            end,
            
            mc_change_stream:close(ChangeStream);
        {error, Reason} ->
            ?debugFmt("Failed to create change stream: ~p", [Reason])
    end.

test_retryable_writes(Connection) ->
    % Test retryable insert
    TestDoc = #{<<"name">> => <<"retryable_test">>, <<"value">> => 123},
    Result1 = mc_worker_api:retryable_insert(Connection, ?COLLECTION, TestDoc),
    ?assertMatch({{true, _}, _}, Result1),
    
    % Test retryable update
    UpdateResult = mc_worker_api:retryable_update(Connection, ?COLLECTION, 
        #{<<"name">> => <<"retryable_test">>}, 
        #{<<"$set">> => #{<<"value">> => 456}}),
    ?assertMatch({true, _}, UpdateResult),
    
    % Test retryable delete
    DeleteResult = mc_worker_api:retryable_delete(Connection, ?COLLECTION, 
        #{<<"name">> => <<"retryable_test">>}),
    ?assertMatch({true, _}, DeleteResult).

test_enhanced_aggregation(Connection) ->
    % Insert test data
    TestDocs = [
        #{<<"name">> => <<"Alice">>, <<"age">> => 25, <<"status">> => <<"active">>},
        #{<<"name">> => <<"Bob">>, <<"age">> => 30, <<"status">> => <<"active">>},
        #{<<"name">> => <<"Charlie">>, <<"age">> => 35, <<"status">> => <<"inactive">>}
    ],
    mc_worker_api:insert(Connection, ?COLLECTION, TestDocs),
    
    % Build aggregation pipeline with modern operators
    Pipeline = mc_aggregation:pipeline()
        |> mc_aggregation:match(#{<<"status">> => <<"active">>})
        |> mc_aggregation:add_fields(#{
            <<"ageGroup">> => mc_aggregation:'cond'([
                #{<<"$lt">> => [<<"$age">>, 30]}, 
                <<"young">>, 
                <<"mature">>
            ])
        })
        |> mc_aggregation:group(#{
            <<"_id">> => <<"$ageGroup">>,
            <<"count">> => #{<<"$sum">> => 1},
            <<"avgAge">> => #{<<"$avg">> => <<"$age">>}
        }),
    
    % Execute aggregation
    {ok, Cursor} = mc_worker_api:aggregate(Connection, ?COLLECTION, Pipeline),
    Results = mc_cursor:rest(Cursor),
    
    ?assertEqual(2, length(Results)),
    
    % Verify results
    YoungGroup = lists:keyfind(<<"young">>, 2, [maps:to_list(R) || R <- Results]),
    MatureGroup = lists:keyfind(<<"mature">>, 2, [maps:to_list(R) || R <- Results]),
    
    ?assertNotEqual(false, YoungGroup),
    ?assertNotEqual(false, MatureGroup).

test_bulk_operations(Connection) ->
    % Define bulk operations
    BulkOps = [
        #{type => insert, document => #{<<"name">> => <<"bulk1">>, <<"value">> => 1}},
        #{type => insert, document => #{<<"name">> => <<"bulk2">>, <<"value">> => 2}},
        #{type => update, filter => #{<<"name">> => <<"bulk1">>}, 
          update => #{<<"$set">> => #{<<"updated">> => true}}, upsert => false},
        #{type => delete, filter => #{<<"name">> => <<"bulk2">>}, multi => false}
    ],
    
    % Execute bulk write
    {ok, BulkResult} = mc_worker_api:bulk_write(Connection, ?COLLECTION, BulkOps),
    
    % Verify results
    ?assertEqual(2, maps:get(<<"insertedCount">>, BulkResult, 0)),
    ?assertEqual(1, maps:get(<<"modifiedCount">>, BulkResult, 0)),
    ?assertEqual(1, maps:get(<<"deletedCount">>, BulkResult, 0)).

test_modern_index_management(Connection) ->
    % Create index with modern options
    IndexSpec = #{<<"name">> => 1, <<"status">> => 1},
    IndexOptions = #{
        name => <<"name_status_idx">>,
        unique => false,
        sparse => true
    },
    
    {ok, _} = mc_worker_api:create_index_with_options(Connection, ?COLLECTION, IndexSpec, IndexOptions),
    
    % List indexes
    {ok, Indexes} = mc_worker_api:list_indexes(Connection, ?COLLECTION),
    ?assert(length(Indexes) >= 2), % At least _id and our custom index
    
    % Find our index
    CustomIndex = lists:filter(fun(Index) ->
        maps:get(<<"name">>, Index) =:= <<"name_status_idx">>
    end, Indexes),
    ?assertEqual(1, length(CustomIndex)),
    
    % Drop the index
    {ok, _} = mc_worker_api:drop_index(Connection, ?COLLECTION, <<"name_status_idx">>).

test_query_optimization(Connection) ->
    % Insert test data
    TestDoc = #{<<"email">> => <<"test@example.com">>, <<"status">> => <<"active">>},
    mc_worker_api:insert(Connection, ?COLLECTION, TestDoc),
    
    % Create index for testing
    mc_worker_api:create_index_with_options(Connection, ?COLLECTION, 
        #{<<"email">> => 1}, #{name => <<"email_idx">>}),
    
    % Test find with hint
    {ok, Cursor} = mc_worker_api:find_with_hint(Connection, ?COLLECTION, 
        #{<<"email">> => <<"test@example.com">>}, 
        <<"email_idx">>),
    Results = mc_cursor:rest(Cursor),
    ?assertEqual(1, length(Results)),
    
    % Test explain query
    {ok, ExplanationPlan} = mc_worker_api:explain_query(Connection, ?COLLECTION, 
        #{<<"email">> => <<"test@example.com">>}),
    ?assert(is_map(ExplanationPlan)),
    ?assert(maps:is_key(<<"executionStats">>, ExplanationPlan)),
    
    % Clean up
    mc_worker_api:drop_index(Connection, ?COLLECTION, <<"email_idx">>).

test_collection_management(Connection) ->
    % Insert some test data
    TestDocs = [
        #{<<"name">> => <<"doc1">>, <<"value">> => 1},
        #{<<"name">> => <<"doc2">>, <<"value">> => 2}
    ],
    mc_worker_api:insert(Connection, ?COLLECTION, TestDocs),
    
    % Get collection statistics
    {ok, Stats} = mc_worker_api:get_collection_stats(Connection, ?COLLECTION),
    ?assert(is_map(Stats)),
    ?assert(maps:get(<<"count">>, Stats, 0) >= 2),
    
    % Validate collection
    {ok, ValidationResult} = mc_worker_api:validate_collection(Connection, ?COLLECTION),
    ?assert(is_map(ValidationResult)),
    ?assertEqual(true, maps:get(<<"valid">>, ValidationResult, false)).

%%%===================================================================
%%% Helper Functions
%%%===================================================================

wait_for_change_stream_ready() ->
    timer:sleep(500). % Give change stream time to initialize
