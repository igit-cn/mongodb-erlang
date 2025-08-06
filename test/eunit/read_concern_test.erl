%%%-------------------------------------------------------------------
%%% @author test
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% Test read concern level support
%%% @end
%%% Created : 16. Jul 2024
%%%-------------------------------------------------------------------
-module(read_concern_test).
-author("test").

-include("mongo_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Test read concern level in connection state
read_concern_connection_test() ->
    % Test that read_concern_level is properly stored in connection state
    Options = [{database, <<"test">>}, {read_concern_level, majority}],
    ConnState = mc_worker:form_state(Options),
    ?assertEqual(majority, ConnState#conn_state.read_concern_level).

%% Test read concern level default value
read_concern_default_test() ->
    % Test that read_concern_level defaults to undefined
    Options = [{database, <<"test">>}],
    ConnState = mc_worker:form_state(Options),
    ?assertEqual(undefined, ConnState#conn_state.read_concern_level).

%% Test read concern level with different values
read_concern_values_test() ->
    TestValues = [local, available, majority, linearizable, snapshot],
    lists:foreach(fun(Level) ->
        Options = [{database, <<"test">>}, {read_concern_level, Level}],
        ConnState = mc_worker:form_state(Options),
        ?assertEqual(Level, ConnState#conn_state.read_concern_level)
    end, TestValues).
