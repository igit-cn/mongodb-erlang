%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% Advanced MongoDB transaction examples demonstrating enhanced features.
%%% Shows usage across single node, replica set, and sharded cluster deployments.
%%% @end
%%%-------------------------------------------------------------------
-module(advanced_transaction_example).
-author("mongodb-erlang team").

-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

-export([
  run_single_node_example/0,
  run_replica_set_example/0,
  run_sharded_cluster_example/0,
  run_causal_consistency_example/0,
  run_retry_mechanism_example/0,
  run_monitoring_example/0,
  bank_transfer_example/0,
  inventory_management_example/0
]).

-define(TEST_DB, <<"advanced_transactions">>).
-define(ACCOUNTS_COLLECTION, <<"accounts">>).
-define(INVENTORY_COLLECTION, <<"inventory">>).
-define(ORDERS_COLLECTION, <<"orders">>).

%%%===================================================================
%%% Single Node Examples
%%%===================================================================

%% @doc Demonstrate single node transaction with enhanced features
run_single_node_example() ->
  io:format("=== Single Node Transaction Example ===~n"),
  
  % Connect to single MongoDB instance
  {ok, Connection} = mc_worker_api:connect([
    {database, ?TEST_DB},
    {host, "localhost"},
    {port, 27017}
  ]),
  
  % Start session with causal consistency
  {ok, SessionPid} = mc_worker_api:start_session(Connection, #{
    causal_consistency => true,
    timeout => 60000
  }),
  
  % Enable causal consistency
  ok = mc_causal_consistency:enable_causal_consistency(SessionPid),
  
  % Start transaction with enhanced options
  TransactionOptions = #transaction_options{
    read_concern = #{<<"level">> => <<"majority">>},
    write_concern = #{<<"w">> => 1, <<"j">> => true},
    max_commit_time_ms = 30000
  },
  
  Result = mc_worker_api:with_transaction(SessionPid, Connection,
    fun(TransactionPid) ->
      % Insert test data
      TestDoc = #{<<"_id">> => 1, <<"name">> => <<"single_node_test">>, <<"value">> => 100},
      ok = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, insert, [TestDoc]),
      
      % Update the document
      ok = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, update,
        [#{<<"_id">> => 1}, #{<<"$inc">> => #{<<"value">> => 50}}]),
      
      % Read the updated document
      UpdatedDoc = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, find_one,
        [#{<<"_id">> => 1}]),
      
      io:format("Updated document: ~p~n", [UpdatedDoc]),
      <<"single_node_success">>
    end, TransactionOptions),
  
  io:format("Single node transaction result: ~p~n", [Result]),
  
  % Cleanup
  ok = mc_worker_api:end_session(SessionPid),
  mc_worker_api:disconnect(Connection).

%%%===================================================================
%%% Replica Set Examples
%%%===================================================================

%% @doc Demonstrate replica set transaction with failover handling
run_replica_set_example() ->
  io:format("=== Replica Set Transaction Example ===~n"),
  
  % Connect to replica set
  {ok, Topology} = mongo_api:connect(
    replica_set,
    ["localhost:27017", "localhost:27018", "localhost:27019"],
    [{name, replica_topology}],
    [{database, ?TEST_DB}]
  ),
  
  % Start session
  {ok, SessionPid} = mongo_api:start_session(Topology),
  
  % Transaction with replica set specific options
  TransactionOptions = #transaction_options{
    read_concern = #{<<"level">> => <<"majority">>},
    write_concern = #{<<"w">> => <<"majority">>, <<"j">> => true},
    read_preference = #{<<"mode">> => <<"primary">>}
  },
  
  Result = mongo_api:with_transaction(SessionPid, Topology,
    fun(_TransactionPid) ->
      % Multi-collection transaction
      Account1 = #{<<"_id">> => <<"acc1">>, <<"balance">> => 1000},
      Account2 = #{<<"_id">> => <<"acc2">>, <<"balance">> => 500},
      
      % Insert accounts
      mongo_api:insert(Topology, ?ACCOUNTS_COLLECTION, Account1),
      mongo_api:insert(Topology, ?ACCOUNTS_COLLECTION, Account2),
      
      % Transfer money between accounts
      mongo_api:update(Topology, ?ACCOUNTS_COLLECTION, 
        #{<<"_id">> => <<"acc1">>}, 
        #{<<"$inc">> => #{<<"balance">> => -200}}, 
        #{multi => false}),
      
      mongo_api:update(Topology, ?ACCOUNTS_COLLECTION,
        #{<<"_id">> => <<"acc2">>},
        #{<<"$inc">> => #{<<"balance">> => 200}},
        #{multi => false}),
      
      <<"replica_set_success">>
    end, TransactionOptions),
  
  io:format("Replica set transaction result: ~p~n", [Result]),
  
  % Cleanup
  ok = mongo_api:end_session(SessionPid),
  mongo_api:disconnect(Topology).

%%%===================================================================
%%% Sharded Cluster Examples
%%%===================================================================

%% @doc Demonstrate sharded cluster transaction with shard key validation
run_sharded_cluster_example() ->
  io:format("=== Sharded Cluster Transaction Example ===~n"),
  
  % Connect to sharded cluster
  {ok, Topology} = mongo_api:connect(
    sharded,
    ["mongos1:27017", "mongos2:27017"],
    [{name, sharded_topology}],
    [{database, ?TEST_DB}]
  ),
  
  % Start session
  {ok, SessionPid} = mongo_api:start_session(Topology),
  
  % Get worker for shard key validation
  {ok, Worker} = mongoc:transaction(Topology, fun(#{pool := W}) -> {ok, W} end, #{}),
  
  % Validate shard key before transaction
  TestDoc = #{<<"shard_key">> => <<"region_1">>, <<"_id">> => 1, <<"data">> => <<"test">>},
  case mc_sharded_transaction:validate_shard_key(Worker, ?INVENTORY_COLLECTION, TestDoc) of
    ok ->
      io:format("Shard key validation passed~n");
    {error, Reason} ->
      io:format("Shard key validation failed: ~p~n", [Reason])
  end,
  
  % Cross-shard transaction
  TransactionOptions = #transaction_options{
    read_concern = #{<<"level">> => <<"snapshot">>},
    write_concern = #{<<"w">> => <<"majority">>, <<"j">> => true}
  },
  
  Result = mongo_api:with_transaction(SessionPid, Topology,
    fun(_TransactionPid) ->
      % Insert documents with shard keys
      Doc1 = #{<<"shard_key">> => <<"region_1">>, <<"_id">> => 1, <<"inventory">> => 100},
      Doc2 = #{<<"shard_key">> => <<"region_2">>, <<"_id">> => 2, <<"inventory">> => 200},
      
      mongo_api:insert(Topology, ?INVENTORY_COLLECTION, Doc1),
      mongo_api:insert(Topology, ?INVENTORY_COLLECTION, Doc2),
      
      % Update inventory across shards
      mongo_api:update(Topology, ?INVENTORY_COLLECTION,
        #{<<"shard_key">> => <<"region_1">>, <<"_id">> => 1},
        #{<<"$inc">> => #{<<"inventory">> => -10}},
        #{multi => false}),
      
      <<"sharded_success">>
    end, TransactionOptions),
  
  io:format("Sharded cluster transaction result: ~p~n", [Result]),
  
  % Cleanup
  ok = mongo_api:end_session(SessionPid),
  mongo_api:disconnect(Topology).

%%%===================================================================
%%% Causal Consistency Examples
%%%===================================================================

%% @doc Demonstrate causal consistency across operations
run_causal_consistency_example() ->
  io:format("=== Causal Consistency Example ===~n"),
  
  {ok, Connection} = mc_worker_api:connect([{database, ?TEST_DB}]),
  
  % Create two sessions with causal consistency
  {ok, Session1} = mc_worker_api:start_session(Connection, #{causal_consistency => true}),
  {ok, Session2} = mc_worker_api:start_session(Connection, #{causal_consistency => true}),
  
  % Enable causal consistency
  ok = mc_causal_consistency:enable_causal_consistency(Session1),
  ok = mc_causal_consistency:enable_causal_consistency(Session2),
  
  % Write in session 1
  Result1 = mc_worker_api:with_transaction(Session1, Connection,
    fun(TransactionPid) ->
      TestDoc = #{<<"_id">> => <<"causal_test">>, <<"value">> => 1, <<"timestamp">> => erlang:system_time()},
      ok = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, insert, [TestDoc]),
      <<"write_complete">>
    end),
  
  io:format("Session 1 write result: ~p~n", [Result1]),
  
  % Sync cluster time between sessions
  mc_causal_consistency:sync_cluster_time([Session1, Session2]),
  
  % Read in session 2 (should see the write from session 1)
  Result2 = mc_worker_api:with_transaction(Session2, Connection,
    fun(TransactionPid) ->
      Doc = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, find_one,
        [#{<<"_id">> => <<"causal_test">>}]),
      case Doc of
        undefined -> <<"not_found">>;
        _ -> <<"found">>
      end
    end),
  
  io:format("Session 2 read result: ~p~n", [Result2]),
  
  % Cleanup
  ok = mc_worker_api:end_session(Session1),
  ok = mc_worker_api:end_session(Session2),
  mc_worker_api:disconnect(Connection).

%%%===================================================================
%%% Retry Mechanism Examples
%%%===================================================================

%% @doc Demonstrate intelligent retry mechanism
run_retry_mechanism_example() ->
  io:format("=== Retry Mechanism Example ===~n"),
  
  {ok, Connection} = mc_worker_api:connect([{database, ?TEST_DB}]),
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Function that might fail and need retry
  TransactionFun = fun() ->
    mc_worker_api:with_transaction(SessionPid, Connection,
      fun(TransactionPid) ->
        % Simulate potential write conflict
        TestDoc = #{<<"_id">> => <<"retry_test">>, <<"counter">> => 1},
        ok = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, insert, [TestDoc]),
        
        % Increment counter (might conflict with concurrent transactions)
        ok = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, update,
          [#{<<"_id">> => <<"retry_test">>}, #{<<"$inc">> => #{<<"counter">> => 1}}]),
        
        <<"retry_success">>
      end)
  end,
  
  % Execute with retry mechanism
  RetryOptions = #{
    strategy => exponential,
    base_backoff => 100,
    max_backoff => 2000
  },
  
  Result = mc_transaction_retry:execute_with_retry(TransactionFun, RetryOptions, 3),
  io:format("Retry mechanism result: ~p~n", [Result]),
  
  % Cleanup
  ok = mc_worker_api:end_session(SessionPid),
  mc_worker_api:disconnect(Connection).

%%%===================================================================
%%% Monitoring Examples
%%%===================================================================

%% @doc Demonstrate transaction monitoring
run_monitoring_example() ->
  io:format("=== Transaction Monitoring Example ===~n"),
  
  % Start transaction monitor
  {ok, _MonitorPid} = mc_transaction_monitor:start_link([{detailed_logging, true}]),
  
  {ok, Connection} = mc_worker_api:connect([{database, ?TEST_DB}]),
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Start transaction
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection),
  
  % Register transaction for monitoring
  mc_transaction_monitor:register_transaction(TransactionPid, #{
    session_id => <<"monitor_session">>,
    topology_type => single,
    operations_count => 0
  }),
  
  % Perform operations
  TestDoc = #{<<"_id">> => <<"monitor_test">>, <<"value">> => 42},
  ok = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, insert, [TestDoc]),
  
  % Update transaction state
  mc_transaction_monitor:update_transaction_state(TransactionPid, in_progress),
  
  % Get transaction metrics
  {ok, TxnMetrics} = mc_transaction_monitor:get_transaction_metrics(TransactionPid),
  io:format("Transaction metrics: ~p~n", [TxnMetrics]),
  
  % Commit transaction
  ok = mc_worker_api:commit_transaction(TransactionPid),
  
  % Unregister transaction
  mc_transaction_monitor:unregister_transaction(TransactionPid),
  
  % Get overall metrics
  {ok, OverallMetrics} = mc_transaction_monitor:get_transaction_metrics(),
  io:format("Overall metrics: ~p~n", [OverallMetrics]),
  
  % Cleanup
  ok = mc_worker_api:end_session(SessionPid),
  mc_worker_api:disconnect(Connection),
  mc_transaction_monitor:stop().

%%%===================================================================
%%% Real-world Examples
%%%===================================================================

%% @doc Bank transfer example with full error handling
bank_transfer_example() ->
  io:format("=== Bank Transfer Example ===~n"),
  
  {ok, Connection} = mc_worker_api:connect([{database, ?TEST_DB}]),
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Setup accounts
  Account1 = #{<<"_id">> => <<"alice">>, <<"balance">> => 1000},
  Account2 = #{<<"_id">> => <<"bob">>, <<"balance">> => 500},
  
  mc_worker_api:insert(Connection, ?ACCOUNTS_COLLECTION, Account1),
  mc_worker_api:insert(Connection, ?ACCOUNTS_COLLECTION, Account2),
  
  % Transfer function with validation
  TransferAmount = 200,
  
  TransferFun = fun() ->
    mc_worker_api:with_transaction(SessionPid, Connection,
      fun(TransactionPid) ->
        % Check source account balance
        SourceAccount = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, find_one,
          [#{<<"_id">> => <<"alice">>}]),
        
        case maps:get(<<"balance">>, SourceAccount, 0) of
          Balance when Balance >= TransferAmount ->
            % Debit source account
            ok = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, update,
              [#{<<"_id">> => <<"alice">>}, #{<<"$inc">> => #{<<"balance">> => -TransferAmount}}]),
            
            % Credit destination account
            ok = mc_worker_api:in_transaction(TransactionPid, ?ACCOUNTS_COLLECTION, update,
              [#{<<"_id">> => <<"bob">>}, #{<<"$inc">> => #{<<"balance">> => TransferAmount}}]),
            
            <<"transfer_successful">>;
          _ ->
            throw({insufficient_funds, SourceAccount})
        end
      end)
  end,
  
  % Execute transfer with retry
  Result = try
    mc_transaction_retry:execute_with_retry(TransferFun, #{strategy => exponential}, 3)
  catch
    throw:{insufficient_funds, Account} ->
      {error, {insufficient_funds, Account}}
  end,
  
  io:format("Bank transfer result: ~p~n", [Result]),
  
  % Cleanup
  ok = mc_worker_api:end_session(SessionPid),
  mc_worker_api:disconnect(Connection).

%% @doc Inventory management example
inventory_management_example() ->
  io:format("=== Inventory Management Example ===~n"),
  
  {ok, Connection} = mc_worker_api:connect([{database, ?TEST_DB}]),
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Setup inventory
  Items = [
    #{<<"_id">> => <<"item1">>, <<"stock">> => 100, <<"reserved">> => 0},
    #{<<"_id">> => <<"item2">>, <<"stock">> => 50, <<"reserved">> => 0}
  ],
  
  mc_worker_api:insert(Connection, ?INVENTORY_COLLECTION, Items),
  
  % Order processing function
  OrderItems = [
    #{<<"item_id">> => <<"item1">>, <<"quantity">> => 5},
    #{<<"item_id">> => <<"item2">>, <<"quantity">> => 3}
  ],
  
  OrderFun = fun() ->
    mc_worker_api:with_transaction(SessionPid, Connection,
      fun(TransactionPid) ->
        % Reserve inventory for each item
        ReservationResults = [begin
          Item = mc_worker_api:in_transaction(TransactionPid, ?INVENTORY_COLLECTION, find_one,
            [#{<<"_id">> => ItemId}]),
          
          Stock = maps:get(<<"stock">>, Item, 0),
          Reserved = maps:get(<<"reserved">>, Item, 0),
          Available = Stock - Reserved,
          
          if Available >= Quantity ->
            % Reserve the quantity
            ok = mc_worker_api:in_transaction(TransactionPid, ?INVENTORY_COLLECTION, update,
              [#{<<"_id">> => ItemId}, #{<<"$inc">> => #{<<"reserved">> => Quantity}}]),
            {ItemId, Quantity, reserved};
          true ->
            throw({insufficient_stock, ItemId, Available, Quantity})
          end
        end || #{<<"item_id">> := ItemId, <<"quantity">> := Quantity} <- OrderItems],
        
        % Create order record
        Order = #{
          <<"_id">> => <<"order_" ++ integer_to_list(erlang:system_time())>>,
          <<"items">> => OrderItems,
          <<"status">> => <<"confirmed">>,
          <<"timestamp">> => erlang:system_time()
        },
        
        ok = mc_worker_api:in_transaction(TransactionPid, ?ORDERS_COLLECTION, insert, [Order]),
        
        {<<"order_successful">>, ReservationResults}
      end)
  end,
  
  % Execute order with retry
  Result = try
    mc_transaction_retry:execute_with_retry(OrderFun, #{strategy => linear}, 3)
  catch
    throw:{insufficient_stock, ItemId, Available, Requested} ->
      {error, {insufficient_stock, ItemId, Available, Requested}}
  end,
  
  io:format("Inventory management result: ~p~n", [Result]),
  
  % Cleanup
  ok = mc_worker_api:end_session(SessionPid),
  mc_worker_api:disconnect(Connection).
