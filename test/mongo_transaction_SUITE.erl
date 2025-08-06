%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% Common Test suite for MongoDB transaction functionality.
%%% Tests transaction support across different MongoDB topologies.
%%% @end
%%%-------------------------------------------------------------------
-module(mongo_transaction_SUITE).
-author("mongodb-erlang team").

-include_lib("common_test/include/ct.hrl").
-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

%% CT callbacks
-export([
  all/0,
  groups/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_group/2,
  end_per_group/2,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Test cases
-export([
  test_session_management/1,
  test_transaction_lifecycle/1,
  test_transaction_isolation/1,
  test_transaction_retry/1,
  test_concurrent_transactions/1,
  test_transaction_timeout/1,
  test_transaction_error_handling/1,
  test_multi_document_transaction/1,
  test_cross_collection_transaction/1,
  test_transaction_read_concern/1,
  test_transaction_write_concern/1
]).

-define(TEST_DB, <<"test_transactions">>).
-define(TEST_COLLECTION1, <<"test_collection1">>).
-define(TEST_COLLECTION2, <<"test_collection2">>).

%%%===================================================================
%%% CT Callbacks
%%%===================================================================

all() ->
  [
    {group, single_node},
    {group, replica_set}
  ].

groups() ->
  [
    {single_node, [parallel], [
      test_session_management,
      test_transaction_lifecycle,
      test_transaction_timeout,
      test_transaction_error_handling,
      test_multi_document_transaction,
      test_cross_collection_transaction
    ]},
    {replica_set, [parallel], [
      test_session_management,
      test_transaction_lifecycle,
      test_transaction_isolation,
      test_transaction_retry,
      test_concurrent_transactions,
      test_transaction_read_concern,
      test_transaction_write_concern
    ]}
  ].

init_per_suite(Config) ->
  application:ensure_all_started(mongodb),
  Config.

end_per_suite(_Config) ->
  application:stop(mongodb),
  ok.

init_per_group(single_node, Config) ->
  case connect_single_node() of
    {ok, Connection} ->
      [{connection, Connection}, {topology, single} | Config];
    {error, Reason} ->
      ct:skip("Cannot connect to single node MongoDB: ~p", [Reason])
  end;

init_per_group(replica_set, Config) ->
  case connect_replica_set() of
    {ok, Topology} ->
      [{topology_pid, Topology}, {topology, replica_set} | Config];
    {error, Reason} ->
      ct:skip("Cannot connect to replica set MongoDB: ~p", [Reason])
  end.

end_per_group(single_node, Config) ->
  Connection = ?config(connection, Config),
  mc_worker_api:disconnect(Connection),
  ok;

end_per_group(replica_set, Config) ->
  Topology = ?config(topology_pid, Config),
  mongo_api:disconnect(Topology),
  ok.

init_per_testcase(_TestCase, Config) ->
  cleanup_test_data(Config),
  Config.

end_per_testcase(_TestCase, Config) ->
  cleanup_test_data(Config),
  ok.

%%%===================================================================
%%% Helper Functions
%%%===================================================================

connect_single_node() ->
  try
    mc_worker_api:connect([
      {database, ?TEST_DB},
      {host, "localhost"},
      {port, 27017}
    ])
  catch
    _:Reason ->
      {error, Reason}
  end.

connect_replica_set() ->
  try
    mongo_api:connect(
      replica_set,
      ["localhost:27017", "localhost:27018", "localhost:27019"],
      [{name, test_topology}],
      [{database, ?TEST_DB}]
    )
  catch
    _:Reason ->
      {error, Reason}
  end.

cleanup_test_data(Config) ->
  case ?config(topology, Config) of
    single ->
      Connection = ?config(connection, Config),
      mc_worker_api:delete(Connection, ?TEST_COLLECTION1, #{}),
      mc_worker_api:delete(Connection, ?TEST_COLLECTION2, #{});
    replica_set ->
      Topology = ?config(topology_pid, Config),
      mongo_api:delete(Topology, ?TEST_COLLECTION1, #{}),
      mongo_api:delete(Topology, ?TEST_COLLECTION2, #{})
  end.

get_api_module(Config) ->
  case ?config(topology, Config) of
    single -> mc_worker_api;
    replica_set -> mongo_api
  end.

get_connection(Config) ->
  case ?config(topology, Config) of
    single -> ?config(connection, Config);
    replica_set -> ?config(topology_pid, Config)
  end.

%%%===================================================================
%%% Test Cases
%%%===================================================================

test_session_management(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % Test session creation
  {ok, SessionPid} = API:start_session(Connection),
  ct:pal("Created session: ~p", [SessionPid]),
  
  % Test session validity
  true = mc_session:is_session_valid(SessionPid),
  
  % Test session ID
  {ok, SessionId} = mc_session:get_session_id(SessionPid),
  true = is_binary(SessionId),
  true = byte_size(SessionId) > 0,
  
  % Test session options
  SessionOptions = mc_session:get_session_options(SessionPid),
  true = is_map(SessionOptions),
  true = maps:is_key(<<"lsid">>, SessionOptions),
  
  % End session
  ok = API:end_session(SessionPid),
  
  % Session should be invalid after ending
  false = mc_session:is_session_valid(SessionPid).

test_transaction_lifecycle(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % Create session
  {ok, SessionPid} = API:start_session(Connection),
  
  % Start transaction
  {ok, TransactionPid} = API:start_transaction(SessionPid, Connection),
  ct:pal("Started transaction: ~p", [TransactionPid]),
  
  % Check transaction state
  {ok, in_progress} = mc_transaction:get_transaction_state(TransactionPid),
  true = mc_transaction:is_in_transaction(TransactionPid),
  
  % Insert test document
  TestDoc = #{<<"test_field">> => <<"lifecycle_test">>, <<"value">> => 1},
  case API of
    mc_worker_api ->
      ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION1, insert, [TestDoc]);
    mongo_api ->
      % For mongo_api, we need to use the transaction context differently
      {ok, Worker} = mc_transaction:get_worker(TransactionPid),
      mc_worker_api:insert(Worker, ?TEST_COLLECTION1, TestDoc)
  end,
  
  % Commit transaction
  ok = API:commit_transaction(TransactionPid),
  
  % Check final state
  {ok, committed} = mc_transaction:get_transaction_state(TransactionPid),
  false = mc_transaction:is_in_transaction(TransactionPid),
  
  % Verify data was committed
  Result = case API of
    mc_worker_api ->
      mc_worker_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"test_field">> => <<"lifecycle_test">>});
    mongo_api ->
      mongo_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"test_field">> => <<"lifecycle_test">>}, #{})
  end,
  #{<<"test_field">> := <<"lifecycle_test">>} = Result,
  
  % Clean up
  ok = API:end_session(SessionPid).

test_transaction_isolation(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % This test only makes sense for replica sets
  case ?config(topology, Config) of
    single ->
      ct:skip("Transaction isolation test requires replica set");
    replica_set ->
      % Create two sessions
      {ok, Session1} = API:start_session(Connection),
      {ok, Session2} = API:start_session(Connection),
      
      % Start two transactions
      {ok, Transaction1} = API:start_transaction(Session1, Connection),
      {ok, Transaction2} = API:start_transaction(Session2, Connection),
      
      % Insert document in first transaction
      TestDoc = #{<<"_id">> => 1, <<"value">> => <<"isolation_test">>},
      {ok, Worker1} = mc_transaction:get_worker(Transaction1),
      mc_worker_api:insert(Worker1, ?TEST_COLLECTION1, TestDoc),
      
      % Try to read from second transaction (should not see uncommitted data)
      {ok, Worker2} = mc_transaction:get_worker(Transaction2),
      undefined = mc_worker_api:find_one(Worker2, ?TEST_COLLECTION1, #{<<"_id">> => 1}),
      
      % Commit first transaction
      ok = API:commit_transaction(Transaction1),
      
      % Now second transaction should see the data
      Result = mc_worker_api:find_one(Worker2, ?TEST_COLLECTION1, #{<<"_id">> => 1}),
      #{<<"value">> := <<"isolation_test">>} = Result,
      
      % Clean up
      ok = API:abort_transaction(Transaction2),
      ok = API:end_session(Session1),
      ok = API:end_session(Session2)
  end.

test_transaction_retry(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % Test with_transaction automatic retry
  {ok, SessionPid} = API:start_session(Connection),
  
  TestDoc = #{<<"test_field">> => <<"retry_test">>, <<"value">> => 2},
  
  Result = API:with_transaction(SessionPid, Connection, 
    fun(_TransactionPid) ->
      case API of
        mc_worker_api ->
          mc_worker_api:insert(Connection, ?TEST_COLLECTION1, TestDoc);
        mongo_api ->
          mongo_api:insert(Connection, ?TEST_COLLECTION1, TestDoc)
      end,
      <<"retry_success">>
    end),
  
  {ok, <<"retry_success">>} = Result,
  
  % Verify data was committed
  Doc = case API of
    mc_worker_api ->
      mc_worker_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"test_field">> => <<"retry_test">>});
    mongo_api ->
      mongo_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"test_field">> => <<"retry_test">>}, #{})
  end,
  #{<<"test_field">> := <<"retry_test">>} = Doc,
  
  % Clean up
  ok = API:end_session(SessionPid).

test_concurrent_transactions(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % This test only makes sense for replica sets
  case ?config(topology, Config) of
    single ->
      ct:skip("Concurrent transactions test requires replica set");
    replica_set ->
      % Create multiple sessions and run concurrent transactions
      NumTransactions = 5,
      Sessions = [begin
        {ok, SessionPid} = API:start_session(Connection),
        SessionPid
      end || _ <- lists:seq(1, NumTransactions)],
      
      % Run concurrent transactions
      Parent = self(),
      Pids = [spawn_link(fun() ->
        TestDoc = #{<<"_id">> => N, <<"value">> => N * 10},
        Result = API:with_transaction(SessionPid, Connection,
          fun(_TransactionPid) ->
            case API of
              mc_worker_api ->
                mc_worker_api:insert(Connection, ?TEST_COLLECTION1, TestDoc);
              mongo_api ->
                mongo_api:insert(Connection, ?TEST_COLLECTION1, TestDoc)
            end,
            N
          end),
        Parent ! {transaction_result, N, Result}
      end) || {N, SessionPid} <- lists:zip(lists:seq(1, NumTransactions), Sessions)],
      
      % Wait for all transactions to complete
      Results = [receive
        {transaction_result, N, Result} -> {N, Result}
      end || N <- lists:seq(1, NumTransactions)],
      
      % Verify all transactions succeeded
      [{N, {ok, N}} = Result || {N, Result} <- Results],
      
      % Verify all data was committed
      [begin
        Doc = case API of
          mc_worker_api ->
            mc_worker_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"_id">> => N});
          mongo_api ->
            mongo_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"_id">> => N}, #{})
        end,
        #{<<"value">> := Value} = Doc,
        Value = N * 10
      end || N <- lists:seq(1, NumTransactions)],
      
      % Clean up
      [ok = API:end_session(SessionPid) || SessionPid <- Sessions],
      [unlink(Pid) || Pid <- Pids]
  end.

test_transaction_timeout(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % Create session
  {ok, SessionPid} = API:start_session(Connection),
  
  % Start transaction with short timeout
  Options = #transaction_options{max_commit_time_ms = 100},
  {ok, TransactionPid} = API:start_transaction(SessionPid, Connection, Options),
  
  % Wait for timeout
  timer:sleep(200),
  
  % Transaction should be aborted
  {ok, aborted} = mc_transaction:get_transaction_state(TransactionPid),
  
  % Clean up
  ok = API:end_session(SessionPid).

test_transaction_error_handling(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % Test invalid session
  {ok, SessionPid} = API:start_session(Connection),
  ok = API:end_session(SessionPid),
  
  {error, _} = API:start_transaction(SessionPid, Connection),
  
  % Test double commit
  {ok, SessionPid2} = API:start_session(Connection),
  {ok, TransactionPid} = API:start_transaction(SessionPid2, Connection),
  
  ok = API:commit_transaction(TransactionPid),
  {error, _} = API:commit_transaction(TransactionPid),
  
  % Clean up
  ok = API:end_session(SessionPid2).

test_multi_document_transaction(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % Create session and transaction
  {ok, SessionPid} = API:start_session(Connection),
  {ok, TransactionPid} = API:start_transaction(SessionPid, Connection),
  
  % Insert multiple documents
  Docs = [#{<<"_id">> => N, <<"value">> => N * 2} || N <- lists:seq(1, 10)],
  
  case API of
    mc_worker_api ->
      ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION1, insert, [Docs]);
    mongo_api ->
      {ok, Worker} = mc_transaction:get_worker(TransactionPid),
      mc_worker_api:insert(Worker, ?TEST_COLLECTION1, Docs)
  end,
  
  % Update some documents
  case API of
    mc_worker_api ->
      ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION1, update,
        [#{<<"_id">> => #{<<"$lte">> => 5}}, #{<<"$inc">> => #{<<"value">> => 100}}]);
    mongo_api ->
      {ok, Worker2} = mc_transaction:get_worker(TransactionPid),
      mc_worker_api:update(Worker2, ?TEST_COLLECTION1, 
        #{<<"_id">> => #{<<"$lte">> => 5}}, #{<<"$inc">> => #{<<"value">> => 100}}, false, true)
  end,
  
  % Commit transaction
  ok = API:commit_transaction(TransactionPid),
  
  % Verify results
  Count = case API of
    mc_worker_api ->
      mc_worker_api:count(Connection, ?TEST_COLLECTION1, #{});
    mongo_api ->
      mongo_api:count(Connection, ?TEST_COLLECTION1, #{}, 0)
  end,
  10 = Count,
  
  % Check updated documents
  UpdatedDoc = case API of
    mc_worker_api ->
      mc_worker_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"_id">> => 1});
    mongo_api ->
      mongo_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"_id">> => 1}, #{})
  end,
  #{<<"value">> := 102} = UpdatedDoc, % 1*2 + 100 = 102
  
  % Clean up
  ok = API:end_session(SessionPid).

test_cross_collection_transaction(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % Create session and transaction
  {ok, SessionPid} = API:start_session(Connection),
  {ok, TransactionPid} = API:start_transaction(SessionPid, Connection),
  
  % Insert into first collection
  Doc1 = #{<<"_id">> => 1, <<"collection">> => <<"first">>},
  case API of
    mc_worker_api ->
      ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION1, insert, [Doc1]);
    mongo_api ->
      {ok, Worker} = mc_transaction:get_worker(TransactionPid),
      mc_worker_api:insert(Worker, ?TEST_COLLECTION1, Doc1)
  end,
  
  % Insert into second collection
  Doc2 = #{<<"_id">> => 1, <<"collection">> => <<"second">>},
  case API of
    mc_worker_api ->
      ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION2, insert, [Doc2]);
    mongo_api ->
      {ok, Worker2} = mc_transaction:get_worker(TransactionPid),
      mc_worker_api:insert(Worker2, ?TEST_COLLECTION2, Doc2)
  end,
  
  % Commit transaction
  ok = API:commit_transaction(TransactionPid),
  
  % Verify both documents exist
  Result1 = case API of
    mc_worker_api ->
      mc_worker_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"_id">> => 1});
    mongo_api ->
      mongo_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"_id">> => 1}, #{})
  end,
  #{<<"collection">> := <<"first">>} = Result1,
  
  Result2 = case API of
    mc_worker_api ->
      mc_worker_api:find_one(Connection, ?TEST_COLLECTION2, #{<<"_id">> => 1});
    mongo_api ->
      mongo_api:find_one(Connection, ?TEST_COLLECTION2, #{<<"_id">> => 1}, #{})
  end,
  #{<<"collection">> := <<"second">>} = Result2,
  
  % Clean up
  ok = API:end_session(SessionPid).

test_transaction_read_concern(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % This test only makes sense for replica sets
  case ?config(topology, Config) of
    single ->
      ct:skip("Read concern test requires replica set");
    replica_set ->
      % Create session with read concern
      {ok, SessionPid} = API:start_session(Connection),
      
      Options = #transaction_options{
        read_concern = #{<<"level">> => <<"majority">>}
      },
      {ok, TransactionPid} = API:start_transaction(SessionPid, Connection, Options),
      
      % Perform read operation
      {ok, Worker} = mc_transaction:get_worker(TransactionPid),
      Result = mc_worker_api:find_one(Worker, ?TEST_COLLECTION1, #{}),
      undefined = Result, % Should be undefined since collection is empty
      
      % Clean up
      ok = API:abort_transaction(TransactionPid),
      ok = API:end_session(SessionPid)
  end.

test_transaction_write_concern(Config) ->
  API = get_api_module(Config),
  Connection = get_connection(Config),
  
  % This test only makes sense for replica sets
  case ?config(topology, Config) of
    single ->
      ct:skip("Write concern test requires replica set");
    replica_set ->
      % Create session with write concern
      {ok, SessionPid} = API:start_session(Connection),
      
      Options = #transaction_options{
        write_concern = #{<<"w">> => <<"majority">>, <<"j">> => true}
      },
      {ok, TransactionPid} = API:start_transaction(SessionPid, Connection, Options),
      
      % Perform write operation
      TestDoc = #{<<"test_field">> => <<"write_concern_test">>},
      {ok, Worker} = mc_transaction:get_worker(TransactionPid),
      mc_worker_api:insert(Worker, ?TEST_COLLECTION1, TestDoc),
      
      % Commit with write concern
      ok = API:commit_transaction(TransactionPid),
      
      % Verify data was written
      Result = case API of
        mc_worker_api ->
          mc_worker_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"test_field">> => <<"write_concern_test">>});
        mongo_api ->
          mongo_api:find_one(Connection, ?TEST_COLLECTION1, #{<<"test_field">> => <<"write_concern_test">>}, #{})
      end,
      #{<<"test_field">> := <<"write_concern_test">>} = Result,
      
      % Clean up
      ok = API:end_session(SessionPid)
  end.
