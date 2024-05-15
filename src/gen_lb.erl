%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% @doc
%%%   loadbalancer/circuitbreaker
%%%
%%% @copyright Bjorn Jensen-Urstad 2014
%%% @end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%_* Module declaration ===============================================
-module(gen_lb).
-behaviour(gen_server).

%%%_* Exports ==========================================================
%% api
-export([ start_link/2
        , stop/1
        , call/2
        , call/3
        , block/2
        , unblock/2
        ]).

%% gen_server
-export([ init/1
        , terminate/2
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , code_change/3
        ]).

%%%_* Includes =========================================================
-include_lib("kernel/include/logger.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").

%%%_* Behaviour ========================================================
-callback exec(any(), list()) -> {ok, _} | {error, _}.

%%%_* Macros ===========================================================
%% Number of attepts done, will always hit atleast two different servers
-define(ATTEMPTS, 2).
%% Interval on which failures are calculated (milliseconds)
-define(INTERVAL, 600000).
%% Number of errors before node will be taken out of cluster, 0-N
-define(MAX_ERRORS, 2).
%% Number of crashes before node will be taken out of cluster, 0-N
-define(MAX_CRASCHES, 1).
%% Time a blocked node will be kept out of cluster (milliseconds)
-define(BLOCK_TIME, 600000).

%%%_* Code =============================================================
%%%_ * Types -----------------------------------------------------------
-record(r, { attempt      = 1
           , attempts     = throw(attempts)
           , args         = throw(args)
           , from         = throw(from)
           , node         = throw(node)
           , parent_ctx   = throw(parent_ctx)
           }).

-record(s, { cluster_up   = throw(cluster_up)
           , cluster_down = []
           , reqs         = dict:new()
           , tref         = throw(tref)
           , cb_mod       = throw(cb_mod)
           , interval     = throw(interval)
           , max_errors   = throw(max_errors)
           , max_crasches = throw(max_crasches)
           , block_time   = throw(time)
           }).

%%%_ * API -------------------------------------------------------------
start_link(Name, Args) ->
  gen_server:start_link({local, Name}, ?MODULE, Args, []).

stop(Ref) ->
  gen_server:call(Ref, stop, infinity).

call(Ref, Args) ->
  call(Ref, Args, []).

call(Ref, Args, Options0) ->
  SpanName = case Ref of
                _ when is_atom(Ref) ->
                    iolist_to_binary([<<"gen_lb call ">>, atom_to_binary(Ref)]);
                _ ->
                    <<"gen_lb call">>
             end,
  StartOpts = #{ attributes => #{},
                 links => [],
                 is_recording => true,
                 start_time => opentelemetry:timestamp(),
                 kind => ?SPAN_KIND_INTERNAL
               },
  ?with_span(SpanName, StartOpts,
    fun (_SpanCtx) ->
      Ctx = otel_ctx:get_current(),
      Options = [{otel_ctx, Ctx}] ++ Options0,
      gen_server:call(Ref, {call, Args, Options}, infinity)
    end).

block(Ref, Node) ->
  gen_server:call(Ref, {block, Node}, infinity).

unblock(Ref, Node) ->
  gen_server:call(Ref, {unblock, Node}, infinity).

%%%_ * gen_server callbacks --------------------------------------------
init(Args) ->
  process_flag(trap_exit, true),
  {ok, Cluster} = assoc(Args, cluster),
  {ok, CbMod}   = assoc(Args, cb_mod),
  {ok, TRef}    = timer:send_interval(1000, maybe_unblock),
  {ok, #s{ cluster_up   = [{Node,[]} || Node <- Cluster]
         , cb_mod       = CbMod
         , tref         = TRef
         , interval     = assoc(Args, interval,     ?INTERVAL)
         , max_errors   = assoc(Args, max_errors,   ?MAX_ERRORS)
         , max_crasches = assoc(Args, max_crasches, ?MAX_CRASCHES)
         , block_time   = assoc(Args, block_time,   ?BLOCK_TIME)
         }}.

terminate(_Rsn, S) ->
  {ok, cancel} = timer:cancel(S#s.tref),
  lists:foreach(fun(Pid) ->
                    receive {'EXIT', Pid, _} -> ok end
                end, dict:fetch_keys(S#s.reqs)).

handle_call({call, _Args, _Options}, _From, #s{cluster_up=[]} = S) ->
  {reply, {error, cluster_down}, S};
handle_call({call, Args, Options}, From, #s{cluster_up=[{Node,Info}|Up]} = S) ->
  ParentCtx = proplists:get_value(otel_ctx, Options, undefined),
  Pid = do_call(S#s.cb_mod, Args, From, Node, ParentCtx),
  Req = #r{ args     = Args
          , from     = From
          , node     = Node
          , parent_ctx = ParentCtx
          , attempts = assoc(Options, attempts, ?ATTEMPTS)
          },
  {noreply, S#s{ cluster_up = Up ++ [{Node,Info}] %round robin lb
               , reqs       = dict:store(Pid, Req, S#s.reqs)}};
handle_call({block, Node}, _From, S) ->
  case lists:keytake(Node, 1, S#s.cluster_up) of
    {value, {Node, _Info}, Up} ->
      {reply, ok, S#s{ cluster_up   = Up
                     , cluster_down = [{Node,inf}|S#s.cluster_down]
                     }};
    false ->
      case lists:keytake(Node, 1, S#s.cluster_down) of
        {value, {Node, _Time}, Down} ->
          {reply, ok, S#s{cluster_down = [{Node,inf}|Down]}};
        false ->
          {reply, {error, not_member}, S}
      end
  end;
handle_call({unblock, Node}, _From, S) ->
  case lists:keytake(Node, 1, S#s.cluster_down) of
    {value, {Node, _Time}, Down} ->
      {reply, ok, S#s{ cluster_up   = [{Node,[]}|S#s.cluster_up]
                     , cluster_down = Down
                     }};
    false ->
      case lists:keymember(Node, 1, S#s.cluster_up) of
        true  -> {reply, {error, not_blocked}, S};
        false -> {reply, {error, not_member}, S}
      end
  end;
handle_call(stop, _From, S) ->
  {stop, normal, ok, S}.

handle_cast(Msg, S) ->
  {stop, {bad_cast, Msg}, S}.

handle_info({'EXIT', Pid, normal}, S) ->
  {noreply, S#s{reqs=dict:erase(Pid, S#s.reqs)}};
handle_info({'EXIT', Pid, {Type, Rsn}}, S)
  when Type =:= call_error;
       Type =:= call_crash ->
  Req0  = dict:fetch(Pid, S#s.reqs),
  Reqs  = dict:erase(Pid, S#s.reqs),
  Up0   = cluster_add_failure(S#s.cluster_up, Req0#r.node, Type, S#s.interval),
  {Up1, Down} = cluster_maybe_block(
                  Up0, S#s.cluster_down, S#s.max_errors, S#s.max_crasches),
  case {Req0#r.attempt < Req0#r.attempts, Up1} of
    {true, []} ->
      %% everything is down
      gen_server:reply(Req0#r.from, {error, Rsn}),
      {noreply, S#s{ cluster_up   = Up1
                   , cluster_down = Down
                   , reqs         = Reqs}};
    {true, Up1} ->
      %% make sure we dont hit the same node twice in a row
      [{Node,Info}|Up] =
        case Up1 of
          [{N1,I1},{N2,I2}|Ns]
            when N1 =:= Req0#r.node -> [{N2,I2},{N1,I1}|Ns];
          Up1                       -> Up1
        end,
      NewPid = do_call(S#s.cb_mod, Req0#r.args, Req0#r.from, Node, Req0#r.parent_ctx),
      Req = Req0#r{ attempt = Req0#r.attempt + 1
                  , node    = Node
                  },
      {noreply, S#s{ cluster_up   = Up ++ [{Node,Info}] %round robin
                   , cluster_down = Down
                   , reqs         = dict:store(NewPid, Req, Reqs)
                   }};
    {false, _} ->
      gen_server:reply(Req0#r.from, {error, Rsn}),
      {noreply, S#s{ cluster_up   = Up1
                   , cluster_down = Down
                   , reqs         = Reqs
                   }}
  end;
handle_info({'EXIT', Pid, Rsn}, S) ->
  {stop, Rsn, S#s{reqs=dict:erase(Pid, S#s.reqs)}};
handle_info(maybe_unblock, S) ->
  {Up, Down} = cluster_maybe_unblock(
                 S#s.cluster_up, S#s.cluster_down, S#s.block_time),
  {noreply, S#s{ cluster_up   = Up
               , cluster_down = Down}};
handle_info(Msg, S) ->
  ?LOG_WARNING("~p", [Msg]),
  {noreply, S}.

code_change(_OldVsn, S, _Extra) ->
  {ok, S}.

%%%_ * Internals -------------------------------------------------------
do_call(CbMod, Args, From, Node, ParentCtx) ->
  erlang:spawn_link(
    fun() ->
        Daddy = erlang:self(),
        Ref   = erlang:make_ref(),
        {Pid, MRef} =
          erlang:spawn_monitor(
            fun() ->
                _ = otel_ctx:attach(ParentCtx),
                SpanName = iolist_to_binary([<<"gen_lb exec ">>, atom_to_binary(CbMod)]),
                StartOpts = #{ attributes => #{},
                               links => [],
                               is_recording => true,
                               start_time => opentelemetry:timestamp(),
                               kind => ?SPAN_KIND_INTERNAL
                             },
                ?with_span(SpanName, StartOpts,
                    fun(_SpanCtx) ->
                      case CbMod:exec(Node, Args) of
                        ok           ->
                          ?set_status(?OTEL_STATUS_OK, <<"no value">>),
                          Daddy ! {Ref, ok};
                        {ok, Res}    ->
                          ?set_status(?OTEL_STATUS_OK, <<"value">>),
                          Daddy ! {Ref, {ok, Res}};
                        {error, Rsn} ->
                          ?set_status(?OTEL_STATUS_ERROR, <<"call error">>),
                          Daddy ! {Ref, {error, Rsn}}
                      end
                    end)
            end),
        receive
          {Ref, ok} ->
            gen_server:reply(From, ok);
          {Ref, {ok, Res}} ->
            gen_server:reply(From, {ok, Res});
          {Ref, {error, Rsn}} ->
            exit({call_error, Rsn});
          {'DOWN', MRef, process, Pid, Rsn} ->
            exit({call_crash, Rsn})
        end
    end).

cluster_add_failure(Up, Node, Type, Interval) ->
  Now  = os:system_time(millisecond),
  Then = Now - Interval,
  lists:map(
    fun({N, Info0}) ->
        %% prune old errors
        Info = lists:filter(
                 fun({_Type, Time}) when Time <  Then -> false;
                    ({_Type, Time}) when Time >= Then -> true
                 end, Info0),
        if Node =:= N -> {N, [{Type,Now}|Info]};
           true       -> {N, Info} %already blocked
        end
    end, Up).

%% NOTE: assumes expired errors have been pruned
cluster_maybe_block(Up0, Down, MaxErrors, MaxCrasches) ->
  {Up, NewDown} =
    lists:partition(
      fun({Node, Info}) ->
          case lists:foldl(fun({call_error, _}, {E,C}) -> {E+1,C};
                              ({call_crash, _}, {E,C}) -> {E,  C+1}
                           end, {0, 0}, Info) of
            {E, _C} when MaxErrors =/= 0, E >= MaxErrors ->
              ?LOG_WARNING("blocking ~p (too many errors)", [Node]),
              false;
            {_E, C} when MaxCrasches =/= 0, C >= MaxCrasches ->
              ?LOG_WARNING("blocking ~p (too many crasches)", [Node]),
              false;
            {_, _} ->
              true
          end
      end, Up0),
  Now = os:system_time(millisecond),
  {Up, Down ++ [{Node, Now} || {Node, _Info} <- NewDown]}.

cluster_maybe_unblock(Up, Down, BlockTime) ->
  Timeout = os:system_time(millisecond) - BlockTime,
  {NewUp, NewDown} =
    lists:partition(
      fun({_Node,  inf })     -> false;
         ({ Node,  Time})
          when Time < Timeout -> ?LOG_WARNING("unblocking ~p", [Node]),
                                 true;
         ({_Node, _Time})     -> false
      end, Down),
  {Up ++ [{Node, []} || {Node, _Time} <- NewUp], NewDown}.

assoc(List, Key) ->
  case lists:keyfind(Key, 1, List) of
    {Key, Value} -> {ok, Value};
    false        -> {error, notfound}
  end.

assoc(List, Key, Default) ->
  case assoc(List, Key) of
    {ok, Value}       -> Value;
    {error, notfound} -> Default
  end.

%%%_* Tests ============================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

manual_block_test() ->
  {ok, Ref} = start_link(manual_block,
                         [{cb_mod, gen_lb_test},
                          {cluster, [foo, bar]}]),
  ok                    = block(Ref, foo),
  ok                    = block(Ref, bar),
  ok                    = block(Ref, bar),
  {error, not_member}   = block(Ref, baz),
  {error, cluster_down} = call(Ref, [ok]),
  ok                    = unblock(Ref, foo),
  {error, not_blocked}  = unblock(Ref, foo),
  {error, not_member}   = unblock(Ref, buz),
  {ok, ok}              = call(Ref, [ok]),
  ok                    = stop(Ref).

automatic_block_test() ->
  {ok, Ref} = start_link(automatic_block,
                         [{cb_mod, gen_lb_test},
                          {cluster, [foo, bar]}]),
  {error, error}        = call(Ref, [error]),
  {error, error}        = call(Ref, [error]),
  {error, cluster_down} = call(Ref, [ok]),
  ok                    = stop(Ref).

automatic_unblock_test_() ->
  {timeout, 660,
   fun() ->
       {ok, Ref} = start_link(automatic_unblock,
                              [{cb_mod, gen_lb_test},
                               {cluster, [baz, buz]},
                               {block_time, 5000}
                              ]),
       ok                    = block(Ref, baz),
       {error, error}        = call(Ref, [error]),
       {error, cluster_down} = call(Ref, [ok]),
       timer:sleep(10000),
       {ok, ok}              = call(Ref, [ok]),
       {error, error}        = call(Ref, [error]),
       {error, cluster_down} = call(Ref, [ok]),
       ok                    = stop(Ref)
   end}.

retry_test() ->
  {ok, Ref} = start_link(retry,
                         [{cb_mod, gen_lb_test},
                          {cluster, [crash, good]}]),
  {ok, ok}  = call(Ref, [ok]),
  ok        = stop(Ref).

no_retry_test() ->
  {ok, Ref} = start_link(no_retry,
                         [{cb_mod, gen_lb_test},
                          {cluster, [crash, good]}]),
  {error, _} = call(Ref, [ok], [{attempts, 1}]),
  ok = stop(Ref).

failures_expire_test_() ->
  {timeout, 60,
   fun() ->
       {ok, Ref} = start_link(failures_expire,
                              [{cb_mod, gen_lb_test},
                               {cluster, [foo, bar]},
                               {interval, 5000}
                              ]),
       {error, error} = call(Ref, [error]),
       timer:sleep(10000),
       {error, error} = call(Ref, [error]),
       {ok,ok}        = call(Ref, [ok]),
       ok = stop(Ref)
   end}.

bad_cast_test() ->
  process_flag(trap_exit, true),
  {ok, Ref} = start_link(bad_cast,
                         [{cb_mod, gen_lb_test}, {cluster, []}]),
  ok        = gen_server:cast(Ref, foo),
  receive {'EXIT', Ref, {bad_cast, foo}} -> ok end,
  process_flag(trap_exit, false),
  ok.

stray_msg_test() ->
  {ok, Ref} = start_link(stray_msg,
                         [{cb_mod, gen_lb_test}, {cluster, [foo]}]),
  Ref ! oops,
  {ok, ok} = call(Ref, [ok]),
  ok       = stop(Ref).

-else.
-endif.

%%%_* Emacs ============================================================
%%% Local Variables:
%%% allout-layout: t
%%% erlang-indent-level: 2
%%% End:
