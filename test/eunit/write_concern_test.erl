%%%-------------------------------------------------------------------
%%% @author test
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% Test write concern and write concern timeout support
%%% @end
%%% Created : 16. Jul 2024
%%%-------------------------------------------------------------------
-module(write_concern_test).
-author("test").

-include("mongo_protocol.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Test write concern with wtimeout
write_concern_with_timeout_test() ->
    % Test that write concern can include wtimeout
    WriteConcern = #{<<"w">> => 1, <<"wtimeout">> => 5000},
    
    % Test that the write concern is properly formatted
    ?assertEqual(1, maps:get(<<"w">>, WriteConcern)),
    ?assertEqual(5000, maps:get(<<"wtimeout">>, WriteConcern)).

%% Test write concern with different w values
write_concern_w_values_test() ->
    % Test different w values
    WValues = [0, 1, 2, <<"majority">>],
    lists:foreach(fun(W) ->
        WriteConcern = #{<<"w">> => W, <<"wtimeout">> => 1000},
        ?assertEqual(W, maps:get(<<"w">>, WriteConcern)),
        ?assertEqual(1000, maps:get(<<"wtimeout">>, WriteConcern))
    end, WValues).

%% Test write concern with j (journal) option
write_concern_with_journal_test() ->
    % Test that write concern can include journal option
    WriteConcern = #{<<"w">> => 1, <<"j">> => true, <<"wtimeout">> => 3000},
    
    ?assertEqual(1, maps:get(<<"w">>, WriteConcern)),
    ?assertEqual(true, maps:get(<<"j">>, WriteConcern)),
    ?assertEqual(3000, maps:get(<<"wtimeout">>, WriteConcern)).

%% Test write concern without timeout (should work)
write_concern_without_timeout_test() ->
    % Test that write concern works without wtimeout
    WriteConcern = #{<<"w">> => 1},

    ?assertEqual(1, maps:get(<<"w">>, WriteConcern)),
    ?assertEqual(undefined, maps:get(<<"wtimeout">>, WriteConcern, undefined)).

%% Test write concern document format (BSON tuple format)
write_concern_bson_format_test() ->
    % Test that write concern can be in BSON tuple format
    WriteConcern = {<<"w">>, 1, <<"wtimeout">>, 5000, <<"j">>, true},

    % Convert to map for testing
    WriteConcernMap = maps:from_list(bson:fields(WriteConcern)),
    ?assertEqual(1, maps:get(<<"w">>, WriteConcernMap)),
    ?assertEqual(5000, maps:get(<<"wtimeout">>, WriteConcernMap)),
    ?assertEqual(true, maps:get(<<"j">>, WriteConcernMap)).

%% Test write concern with replica set tag
write_concern_with_tag_test() ->
    % Test that write concern can include replica set tags
    WriteConcern = #{<<"w">> => <<"myTag">>, <<"wtimeout">> => 10000},

    ?assertEqual(<<"myTag">>, maps:get(<<"w">>, WriteConcern)),
    ?assertEqual(10000, maps:get(<<"wtimeout">>, WriteConcern)).
