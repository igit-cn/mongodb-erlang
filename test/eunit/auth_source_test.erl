%%%-------------------------------------------------------------------
%%% @author test
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% Test auth_source parameter support
%%% @end
%%% Created : 16. Jul 2024
%%%-------------------------------------------------------------------
-module(auth_source_test).
-author("test").

-include("mongo_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Test auth_source in connection state
auth_source_connection_test() ->
    % Test that auth_source is properly stored in connection state
    Options = [{database, <<"test">>}, {auth_source, <<"myauth">>}],
    ConnState = mc_worker:form_state(Options),
    ?assertEqual(<<"myauth">>, ConnState#conn_state.auth_source).

%% Test auth_source default value
auth_source_default_test() ->
    % Test that auth_source defaults to "admin"
    Options = [{database, <<"test">>}],
    ConnState = mc_worker:form_state(Options),
    ?assertEqual(<<"admin">>, ConnState#conn_state.auth_source).

%% Test auth_source with different databases
auth_source_different_databases_test() ->
    % Test auth_source with different database values
    TestDatabases = [<<"admin">>, <<"mydb">>, <<"authdb">>, <<"$external">>],
    lists:foreach(fun(AuthDB) ->
        Options = [{database, <<"test">>}, {auth_source, AuthDB}],
        ConnState = mc_worker:form_state(Options),
        ?assertEqual(AuthDB, ConnState#conn_state.auth_source)
    end, TestDatabases).

%% Test auth_source different from database
auth_source_different_from_database_test() ->
    % Test that auth_source can be different from the main database
    Options = [{database, <<"myapp">>}, {auth_source, <<"admin">>}],
    ConnState = mc_worker:form_state(Options),
    ?assertEqual(<<"myapp">>, ConnState#conn_state.database),
    ?assertEqual(<<"admin">>, ConnState#conn_state.auth_source).

%% Test auth_source with external authentication
auth_source_external_test() ->
    % Test auth_source with external authentication (e.g., LDAP, Kerberos)
    Options = [{database, <<"test">>}, {auth_source, <<"$external">>}],
    ConnState = mc_worker:form_state(Options),
    ?assertEqual(<<"$external">>, ConnState#conn_state.auth_source).
