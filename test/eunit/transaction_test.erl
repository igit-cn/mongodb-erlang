%%%-------------------------------------------------------------------
%%% @author mongodb-erlang team
%%% @copyright (C) 2024, mongodb-erlang
%%% @doc
%%% Unit tests for MongoDB transaction functionality.
%%% Tests session management, transaction lifecycle, and error handling.
%%% @end
%%%-------------------------------------------------------------------
-module(transaction_test).
-author("mongodb-erlang team").

-include_lib("eunit/include/eunit.hrl").
-include("mongo_types.hrl").
-include("mongo_protocol.hrl").

-define(TEST_DB, <<"test_transactions">>).
-define(TEST_COLLECTION, <<"test_collection">>).

%%%===================================================================
%%% Test Setup and Teardown
%%%===================================================================

transaction_test_() ->
  {setup,
    fun setup/0,
    fun cleanup/1,
    fun(Connection) ->
      [
        {"Session Management Tests", session_tests(Connection)},
        {"Transaction Lifecycle Tests", transaction_lifecycle_tests(Connection)},
        {"Transaction Error Handling Tests", transaction_error_tests(Connection)},
        {"Transaction API Tests", transaction_api_tests(Connection)}
      ]
    end
  }.

setup() ->
  application:ensure_all_started(mongodb),
  {ok, Connection} = mc_worker_api:connect([
    {database, ?TEST_DB},
    {host, "localhost"},
    {port, 27017}
  ]),
  
  % Clean up test collection
  mc_worker_api:delete(Connection, ?TEST_COLLECTION, #{}),
  Connection.

cleanup(Connection) ->
  % Clean up test collection
  mc_worker_api:delete(Connection, ?TEST_COLLECTION, #{}),
  mc_worker_api:disconnect(Connection),
  application:stop(mongodb).

%%%===================================================================
%%% Session Management Tests
%%%===================================================================

session_tests(Connection) ->
  [
    ?_test(test_session_creation(Connection)),
    ?_test(test_session_lifecycle(Connection)),
    ?_test(test_session_options(Connection)),
    ?_test(test_session_timeout(Connection))
  ].

test_session_creation(Connection) ->
  % Test basic session creation
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  ?assert(is_pid(SessionPid)),
  ?assert(mc_session:is_session_valid(SessionPid)),
  
  % Test session ID retrieval
  {ok, SessionId} = mc_session:get_session_id(SessionPid),
  ?assert(is_binary(SessionId)),
  ?assert(byte_size(SessionId) > 0),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

test_session_lifecycle(Connection) ->
  % Create session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  ?assert(mc_session:is_session_valid(SessionPid)),
  
  % End session
  ok = mc_worker_api:end_session(SessionPid),
  
  % Session should no longer be valid
  ?assertNot(mc_session:is_session_valid(SessionPid)).

test_session_options(Connection) ->
  % Test session with custom options
  Options = #{timeout => 60000, implicit => false},
  {ok, SessionPid} = mc_worker_api:start_session(Connection, Options),
  
  % Get session options
  SessionOptions = mc_session:get_session_options(SessionPid),
  ?assert(is_map(SessionOptions)),
  ?assert(maps:is_key(<<"lsid">>, SessionOptions)),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

test_session_timeout(Connection) ->
  % Test session with short timeout
  Options = #{timeout => 100}, % 100ms timeout
  {ok, SessionPid} = mc_worker_api:start_session(Connection, Options),
  
  % Wait for timeout
  timer:sleep(200),
  
  % Session should be invalid after timeout
  ?assertNot(mc_session:is_session_valid(SessionPid)).

%%%===================================================================
%%% Transaction Lifecycle Tests
%%%===================================================================

transaction_lifecycle_tests(Connection) ->
  [
    ?_test(test_transaction_creation(Connection)),
    ?_test(test_transaction_commit(Connection)),
    ?_test(test_transaction_abort(Connection)),
    ?_test(test_transaction_state_tracking(Connection))
  ].

test_transaction_creation(Connection) ->
  % Create session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Start transaction
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection),
  ?assert(is_pid(TransactionPid)),
  ?assert(mc_transaction:is_in_transaction(TransactionPid)),
  
  % Check transaction state
  {ok, State} = mc_transaction:get_transaction_state(TransactionPid),
  ?assertEqual(in_progress, State),
  
  % Clean up
  ok = mc_worker_api:abort_transaction(TransactionPid),
  ok = mc_worker_api:end_session(SessionPid).

test_transaction_commit(Connection) ->
  % Create session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Start transaction
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection),
  
  % Perform some operations within transaction
  TestDoc = #{<<"test_field">> => <<"commit_test">>, <<"value">> => 1},
  ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION, insert, [TestDoc]),
  
  % Commit transaction
  ok = mc_worker_api:commit_transaction(TransactionPid),
  
  % Check transaction state
  {ok, State} = mc_transaction:get_transaction_state(TransactionPid),
  ?assertEqual(committed, State),
  
  % Verify data was committed
  Result = mc_worker_api:find_one(Connection, ?TEST_COLLECTION, #{<<"test_field">> => <<"commit_test">>}),
  ?assertMatch(#{<<"test_field">> := <<"commit_test">>}, Result),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

test_transaction_abort(Connection) ->
  % Create session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Start transaction
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection),
  
  % Perform some operations within transaction
  TestDoc = #{<<"test_field">> => <<"abort_test">>, <<"value">> => 2},
  ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION, insert, [TestDoc]),
  
  % Abort transaction
  ok = mc_worker_api:abort_transaction(TransactionPid),
  
  % Check transaction state
  {ok, State} = mc_transaction:get_transaction_state(TransactionPid),
  ?assertEqual(aborted, State),
  
  % Verify data was not committed
  Result = mc_worker_api:find_one(Connection, ?TEST_COLLECTION, #{<<"test_field">> => <<"abort_test">>}),
  ?assertEqual(undefined, Result),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

test_transaction_state_tracking(Connection) ->
  % Create session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Start transaction
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection),
  
  % Check initial state
  {ok, State1} = mc_transaction:get_transaction_state(TransactionPid),
  ?assertEqual(in_progress, State1),
  ?assert(mc_transaction:is_in_transaction(TransactionPid)),
  
  % Commit transaction
  ok = mc_worker_api:commit_transaction(TransactionPid),
  
  % Check final state
  {ok, State2} = mc_transaction:get_transaction_state(TransactionPid),
  ?assertEqual(committed, State2),
  ?assertNot(mc_transaction:is_in_transaction(TransactionPid)),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

%%%===================================================================
%%% Transaction Error Handling Tests
%%%===================================================================

transaction_error_tests(Connection) ->
  [
    ?_test(test_invalid_session_transaction(Connection)),
    ?_test(test_double_commit(Connection)),
    ?_test(test_commit_after_abort(Connection)),
    ?_test(test_transaction_timeout(Connection))
  ].

test_invalid_session_transaction(Connection) ->
  % Create and end session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  ok = mc_worker_api:end_session(SessionPid),
  
  % Try to start transaction with invalid session
  Result = mc_worker_api:start_transaction(SessionPid, Connection),
  ?assertMatch({error, _}, Result).

test_double_commit(Connection) ->
  % Create session and transaction
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection),
  
  % First commit should succeed
  ok = mc_worker_api:commit_transaction(TransactionPid),
  
  % Second commit should fail
  Result = mc_worker_api:commit_transaction(TransactionPid),
  ?assertMatch({error, _}, Result),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

test_commit_after_abort(Connection) ->
  % Create session and transaction
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection),
  
  % Abort transaction
  ok = mc_worker_api:abort_transaction(TransactionPid),
  
  % Commit should fail
  Result = mc_worker_api:commit_transaction(TransactionPid),
  ?assertMatch({error, _}, Result),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

test_transaction_timeout(Connection) ->
  % Create session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Start transaction with short timeout
  Options = #transaction_options{max_commit_time_ms = 100},
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection, Options),
  
  % Wait for timeout
  timer:sleep(200),
  
  % Transaction should be aborted
  {ok, State} = mc_transaction:get_transaction_state(TransactionPid),
  ?assertEqual(aborted, State),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

%%%===================================================================
%%% Transaction API Tests
%%%===================================================================

transaction_api_tests(Connection) ->
  [
    ?_test(test_with_transaction_success(Connection)),
    ?_test(test_with_transaction_failure(Connection)),
    ?_test(test_transaction_operations(Connection))
  ].

test_with_transaction_success(Connection) ->
  % Create session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Execute transaction with automatic commit
  TestDoc = #{<<"test_field">> => <<"with_transaction_success">>, <<"value">> => 3},
  
  Result = mc_worker_api:with_transaction(SessionPid, Connection, 
    fun(TransactionPid) ->
      mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION, insert, [TestDoc]),
      <<"success">>
    end),
  
  ?assertMatch({ok, <<"success">>}, Result),
  
  % Verify data was committed
  Doc = mc_worker_api:find_one(Connection, ?TEST_COLLECTION, #{<<"test_field">> => <<"with_transaction_success">>}),
  ?assertMatch(#{<<"test_field">> := <<"with_transaction_success">>}, Doc),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

test_with_transaction_failure(Connection) ->
  % Create session
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  
  % Execute transaction that throws an error
  TestDoc = #{<<"test_field">> => <<"with_transaction_failure">>, <<"value">> => 4},
  
  Result = try
    mc_worker_api:with_transaction(SessionPid, Connection, 
      fun(TransactionPid) ->
        mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION, insert, [TestDoc]),
        throw(test_error)
      end)
  catch
    throw:test_error -> {error, test_error}
  end,
  
  ?assertMatch({error, test_error}, Result),
  
  % Verify data was not committed
  Doc = mc_worker_api:find_one(Connection, ?TEST_COLLECTION, #{<<"test_field">> => <<"with_transaction_failure">>}),
  ?assertEqual(undefined, Doc),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).

test_transaction_operations(Connection) ->
  % Create session and transaction
  {ok, SessionPid} = mc_worker_api:start_session(Connection),
  {ok, TransactionPid} = mc_worker_api:start_transaction(SessionPid, Connection),
  
  % Test various operations within transaction
  TestDoc1 = #{<<"_id">> => 1, <<"name">> => <<"doc1">>, <<"value">> => 10},
  TestDoc2 = #{<<"_id">> => 2, <<"name">> => <<"doc2">>, <<"value">> => 20},
  
  % Insert documents
  ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION, insert, [[TestDoc1, TestDoc2]]),
  
  % Update document
  ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION, update, 
    [#{<<"_id">> => 1}, #{<<"$set">> => #{<<"value">> => 15}}]),
  
  % Find document
  Result = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION, find_one, 
    [#{<<"_id">> => 1}]),
  ?assertMatch(#{<<"value">> := 15}, Result),
  
  % Delete document
  ok = mc_worker_api:in_transaction(TransactionPid, ?TEST_COLLECTION, delete, 
    [#{<<"_id">> => 2}]),
  
  % Commit transaction
  ok = mc_worker_api:commit_transaction(TransactionPid),
  
  % Verify final state
  Doc1 = mc_worker_api:find_one(Connection, ?TEST_COLLECTION, #{<<"_id">> => 1}),
  ?assertMatch(#{<<"value">> := 15}, Doc1),
  
  Doc2 = mc_worker_api:find_one(Connection, ?TEST_COLLECTION, #{<<"_id">> => 2}),
  ?assertEqual(undefined, Doc2),
  
  % Clean up
  ok = mc_worker_api:end_session(SessionPid).
