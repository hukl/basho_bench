%% -------------------------------------------------------------------
%%
%% basho_bench_driver_riakc_pb: Driver for riak protocol buffers client
%%
%% Copyright (c) 2009 Basho Techonologies
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(basho_bench_driver_riakc_pb).

-export([new/1,
         run/4,
         mapred_valgen/2]).

-include("basho_bench.hrl").

-record(state, { pid,
                 bucket,
                 r,
                 w,
                 dw,
                 rw,
                 keylist_length}).

-define(ERLANG_MR,
        [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, false},
         {reduce, {modfun, riak_kv_mapreduce, reduce_count_inputs}, none, true}]).
-define(JS_MR,
        [{map, {jsfun, <<"Riak.mapValuesJson">>}, none, false},
         {reduce, {jsfun, <<"Riak.reduceSum">>}, none, true}]).

%% ====================================================================
%% API
%% ====================================================================

new(Id) ->
    %% Make sure the path is setup such that we can get at riak_client
    case code:which(riakc_pb_socket) of
        non_existing ->
            ?FAIL_MSG("~s requires riakc_pb_socket module to be available on code path.\n",
                      [?MODULE]);
        _ ->
            ok
    end,

    Ips  = basho_bench_config:get(riakc_pb_ips, [{127,0,0,1}]),
    Port  = basho_bench_config:get(riakc_pb_port, 8087),
    %% riakc_pb_replies sets defaults for R, W, DW and RW.
    %% Each can be overridden separately
    Replies = basho_bench_config:get(riakc_pb_replies, 2),
    R = basho_bench_config:get(riakc_pb_r, Replies),
    W = basho_bench_config:get(riakc_pb_w, Replies),
    DW = basho_bench_config:get(riakc_pb_dw, Replies),
    RW = basho_bench_config:get(riakc_pb_rw, Replies),
    Bucket  = basho_bench_config:get(riakc_pb_bucket, <<"test">>),
    KeylistLength = basho_bench_config:get(riakc_pb_keylist_length, 1000),

    %% Choose the target node using our ID as a modulus
    Targets = expand_ips(Ips, Port),
    {TargetIp, TargetPort} = lists:nth((Id rem length(Targets)+1), Targets),
    ?INFO("Using target ~p:~p for worker ~p\n", [TargetIp, TargetPort, Id]),
    case riakc_pb_socket:start_link(TargetIp, TargetPort) of
        {ok, Pid} ->
            {ok, #state { pid = Pid,
                          bucket = Bucket,
                          r = R,
                          w = W,
                          dw = DW,
                          rw = RW,
                          keylist_length = KeylistLength
                         }};
        {error, Reason2} ->
            ?FAIL_MSG("Failed to connect riakc_pb_socket to ~p:~p: ~p\n",
                      [TargetIp, TargetPort, Reason2])
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    % error_logger:info_msg("Key: ~p~n", [Key]),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}]) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

run(get_gamestate, KeyGen, _ValueGen, State) ->
    Key = list_to_binary(
         integer_to_list( 100000000000000 + random:uniform(50000000) ) 
    ),
    case riakc_pb_socket:get(State#state.pid, <<"gamestate">>, Key,
                             [{r, State#state.r}]) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

run(get_existing, KeyGen, _ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket, Key,
                             [{r, State#state.r}]) of
        {ok, _} ->
            {ok, State};
        {error, notfound} ->
            {error, {not_found, Key}, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put, KeyGen, ValueGen, State) ->
    Robj0 = riakc_obj:new(State#state.bucket, KeyGen()),
    Robj = riakc_obj:update_value(Robj0, ValueGen()),
    case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                     {dw, State#state.dw}]) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(put_gamestate, _, _, State) ->
    Key = random_gamestate_key(100000000),
    % error_logger:info_msg("Key: ~p~n", [Key]),
    Robj0 = riakc_obj:new(<<"gamestate">>, Key),
    Robj = riakc_obj:update_value(Robj0, generate_gamestate()),
    case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                     {dw, State#state.dw}]) of
        ok ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(update, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}]) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                              {dw, State#state.dw}]) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            Robj0 = riakc_obj:new(State#state.bucket, KeyGen()),
            Robj = riakc_obj:update_value(Robj0, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj, [{w, State#state.w},
                                                             {dw, State#state.dw}]) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end
    end;
run(update_existing, KeyGen, ValueGen, State) ->
    Key = KeyGen(),
    case riakc_pb_socket:get(State#state.pid, State#state.bucket,
                             Key, [{r, State#state.r}]) of
        {ok, Robj} ->
            Robj2 = riakc_obj:update_value(Robj, ValueGen()),
            case riakc_pb_socket:put(State#state.pid, Robj2, [{w, State#state.w},
                                                              {dw, State#state.dw}]) of
                ok ->
                    {ok, State};
                {error, Reason} ->
                    {error, Reason, State}
            end;
        {error, notfound} ->
            {error, {not_found, Key}, State}
    end;
run(delete, KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakc_pb_socket:delete(State#state.pid, State#state.bucket, KeyGen(),
                                [{rw, State#state.rw}]) of
        ok ->
            {ok, State};
        {error, notfound} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
run(listkeys, _KeyGen, _ValueGen, State) ->
    %% Pass on rw
    case riakc_pb_socket:list_keys(State#state.pid, State#state.bucket) of
        {ok, _Keys} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

run(m_values, KeyGen, _ValueGen, State) ->
    case mapred_values( KeyGen, State ) of
      {ok, _Result}     -> {ok, State};
      {error, Reason}   -> {error, Reason, State}
    end;

run(mr_bucket_erlang, _KeyGen, _ValueGen, State) ->
    mapred(State, State#state.bucket, ?ERLANG_MR);
run(mr_bucket_js, _KeyGen, _ValueGen, State) ->
    mapred(State, State#state.bucket, ?JS_MR);
run(mr_keylist_erlang, KeyGen, _ValueGen, State) ->
    Keylist = make_keylist(State#state.bucket, KeyGen,
                           State#state.keylist_length),
    mapred(State, Keylist, ?ERLANG_MR);
run(mr_keylist_js, KeyGen, _ValueGen, State) ->
    Keylist = make_keylist(State#state.bucket, KeyGen,
                          State#state.keylist_length),
    mapred(State, Keylist, ?JS_MR).

%% ====================================================================
%% Key Generators
%% ====================================================================

random_highscore_key(MaxKey) ->
    list_to_binary([
        integer_to_list(100000000 + random:uniform(MaxKey)),
        "_2012-02-01"
      ]).

random_gamestate_key(MaxKey) ->
    list_to_binary(
        integer_to_list( 200000000000000 + random:uniform(MaxKey) )
    ).


%% ====================================================================
%% Value Generators
%% ====================================================================

generate_gamestate() ->
    list_to_binary(
        "{\"low_awards\":{\"award1\":5,\"award2\":3,\"award3\":2,\"award4\":4,\"award5\":0,\"award6\":2,\"award7\":1,\"award8\":0,\"award9\":0,\"award10\":0,\"crown1\":2,\"crown2\":0,\"crown3\":1},\"user_awards\":{\"type1_trophy1\":0,\"type1_trophy2\":0,\"type1_trophy3\":0,\"type1_trophy4\":0,\"type1_trophy5\":0,\"type1_trophy6\":0,\"type1_trophy7\":0,\"type1_trophy8\":0,\"type1_trophy9\":0,\"type1_trophy10\":0,\"type2_trophy1\":0,\"type2_trophy2\":0,\"type2_trophy3\":0,\"type2_trophy4\":0,\"type2_trophy5\":0,\"type2_trophy6\":0,\"type2_trophy7\":0,\"type2_trophy8\":0,\"type2_trophy9\":0,\"type2_trophy10\":0,\"type3_trophy1\":0,\"type3_trophy2\":0,\"type3_trophy3\":0,\"type3_trophy4\":0,\"type3_trophy5\":0,\"type3_trophy6\":0,\"type3_trophy7\":0,\"type3_trophy8\":0,\"type3_trophy9\":0,\"type3_trophy10\":0},\"low_scores\":{\"low_count\":17,\"low_count_week\":5,\"score\":114775,\"created_at\":\"2011-09-22 14:33:18\",\"calendar_week\":\"2011-09-19\",\"low_prem\":0,\"low_time_boosts\":4,\"low_add_timer_boosts\":3,\"low_score_boosts\":4,\"low_fireball_boosts\":0},\"user_days\":{\"days_in_a_row\":2,\"updated_at\":\"2011-09-22 14:33:18\"},\"user_missions\":{\"type\":2,\"mission\":1,\"tasks\":[{\"id\":1,\"it\":1,\"cr\":10,\"tr\":10}],\"state\":2,\"created_at\":\"2011-09-22 14:33:18\",\"updated_at\":\"2011-09-22 14:33:18\"},\"user\":{\"number_of_friends_playing\":8,\"number_of_friends\":240,\"locale\":\"en\",\"session_token\":\"2.1KlOvR_q32GbNbIijTSrHQ__.3600.1295\",\"created_at\":\"2011-09-22 14:33:18\",\"updated_at\":\"2011-09-22 14:33:18\",\"ip_address\":1121752565,\"rounds_played\":322},\"user_user_currency\":{\"gold\":20,\"coins\":500}}"
    ).

generate_highscore() ->
    list_to_binary( integer_to_list( random:uniform(1000000) ) ).

%% ====================================================================
%% Internal functions
%% ====================================================================

expand_ips(Ips, Port) ->
    lists:foldl(fun({Ip,Ports}, Acc) when is_list(Ports) ->
                        Acc ++ lists:map(fun(P) -> {Ip, P} end, Ports);
                   (T={_,_}, Acc) ->
                        [T|Acc];
                   (Ip, Acc) ->
                        [{Ip,Port}|Acc]
                end, [], Ips).

mapred(State, Input, Query) ->
    case riakc_pb_socket:mapred(State#state.pid, Input, Query) of
        {ok, _Result} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

make_keylist(_Bucket, _KeyGen, 0) ->
    [];
make_keylist(Bucket, KeyGen, Count) ->
    [{Bucket, list_to_binary(KeyGen())}
     |make_keylist(Bucket, KeyGen, Count-1)].

mapred_valgen(_Id, MaxRand) ->
    fun() ->
            list_to_binary(integer_to_list(random:uniform(MaxRand)))
    end.

mapred_values( KeyGen, State ) ->
    Keys = generate_key_list( KeyGen ),

    Result = riakc_pb_socket:mapred(
        State#state.pid,
        Keys,
        [{map, {modfun, riak_kv_mapreduce, map_object_value}, none, true}]
    ),
    % error_logger:info_msg("Result ~p~n", [Result]),
    Result.

generate_key_list( KeyGen ) ->
    generate_key_list( 0, KeyGen, [] ).

generate_key_list( 12, _KeyGen, State ) ->
    State;

generate_key_list( Counter, KeyGen, State ) ->
    NewState = [{<<"test">>, KeyGen()}|State],
    generate_key_list( Counter + 1, KeyGen, NewState ).

