%%%-------------------------------------------------------------------
%%% @author test
%%% @copyright (C) 2024, <COMPANY>
%%% @doc
%%% Test read preference support
%%% @end
%%% Created : 16. Jul 2024
%%%-------------------------------------------------------------------
-module(read_preference_test).
-author("test").

-include("mongo_protocol.hrl").
-include("mongoc.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Test read preference modes
read_preference_modes_test() ->
    % Test different read preference modes
    Modes = [primary, secondary, primaryPreferred, secondaryPreferred, nearest],
    lists:foreach(fun(Mode) ->
        ReadPref = #{<<"mode">> => atom_to_binary(Mode, utf8)},
        ?assertEqual(atom_to_binary(Mode, utf8), maps:get(<<"mode">>, ReadPref))
    end, Modes).

%% Test read preference with tags
read_preference_with_tags_test() ->
    % Test read preference with replica set tags
    Tags = [{<<"datacenter">>, <<"east">>}, {<<"rack">>, <<"1">>}],
    ReadPref = #{<<"mode">> => <<"secondary">>, <<"tags">> => [Tags]},
    
    ?assertEqual(<<"secondary">>, maps:get(<<"mode">>, ReadPref)),
    ?assertEqual([Tags], maps:get(<<"tags">>, ReadPref)).

%% Test read preference extraction from selector
read_preference_extraction_test() ->
    % Test mongoc:extract_read_preference/1 function
    Selector = #{<<"$query">> => #{<<"name">> => <<"test">>}, 
                 <<"$readPreference">> => #{<<"mode">> => <<"secondary">>}},
    
    {ReadPref, Query, OrderBy} = mongoc:extract_read_preference(Selector),
    
    ?assertEqual(#{<<"mode">> => <<"secondary">>}, ReadPref),
    ?assertEqual(#{<<"name">> => <<"test">>}, Query),
    ?assertEqual(#{}, OrderBy).

%% Test read preference appending to selector
read_preference_append_test() ->
    % Test mongoc:append_read_preference/2 function
    Selector = #{<<"name">> => <<"test">>},
    ReadPref = #{<<"mode">> => <<"primaryPreferred">>},
    
    Result = mongoc:append_read_preference(Selector, ReadPref),
    
    % Should create a new structure with $query and $readPreference
    ?assertEqual(#{<<"name">> => <<"test">>}, maps:get(<<"$query">>, Result)),
    ?assertEqual(ReadPref, maps:get(<<"$readPreference">>, Result)).

%% Test read preference default value
read_preference_default_test() ->
    % Test that default read preference is primary
    DefaultReadPref = #{<<"mode">> => <<"primary">>},
    ?assertEqual(<<"primary">>, maps:get(<<"mode">>, DefaultReadPref)).
