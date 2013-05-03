
-module(epool).

-behaviour(gen_server).

%% API
-export([start/0, start/1, stop/0]).
-export([start_pool/3, start_pool/4, stop_pool/1]).
-export([enslave/1, enslave/2]).
-export([release/2]).
-export([transaction/2, transaction/3]).
-export([give_away/3]).
-export([info/1, info/2]).

-export([start_link/5]).
-export([start_link_worker/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
	  name = undefined :: atom(),
	  min  = 0 :: integer(),
	  max  = 0 :: integer() | infinity,
	  aliveTimeout = 0 :: integer() | infinity,
	  step = 1 :: integer(),
	  overflow = 0 :: integer(),
	  workerSup = undefined :: pid() | undefined,
	  workers = undefined :: ets:tab() | undefined,
	  monitors = undefined :: ets:tab() | undefined,
	  mfa :: {atom(), atom(), [term()]},
	  queue = queue:new() :: queue(),
	  queueMax = infinity :: integer() | infinity
}).

-define(WAIT_TIMEOUT, infinity).

%%%===================================================================
%%% API
%%%===================================================================

start() ->
    application:start(epool).

start(Type) ->
    application:start(epool, Type).

stop() ->
    application:stop(epool).

start_pool(Name, PoolSize, MFA) ->
    epool_sup:start_pool(Name, PoolSize, MFA, []).

start_pool(Name, PoolSize, MFA, Opts) ->
    epool_sup:start_pool(Name, PoolSize, MFA, Opts).

stop_pool(Name) ->
    epool_sup:stop_pool(Name).

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Name, Sup, PoolSize, MFA, Opts) ->
    gen_server:start_link({local, Name}, ?MODULE, [Name, Sup, PoolSize, MFA, Opts], []).

enslave(Pool) ->
    enslave(Pool, ?WAIT_TIMEOUT).

enslave(Pool, none) ->
    gen_server:call(Pool, {enslave, none});
enslave(Pool, Timeout) when is_integer(Timeout), Timeout > 0; Timeout =:= infinity ->
    gen_server:call(Pool, {enslave, Timeout}, Timeout).

release(Pool, Pid) ->
    gen_server:cast(Pool, {release, self(), Pid}).

transaction(Pool, F) ->
    transaction(Pool, F, queue).

transaction(Pool, F, Timeout) when is_function(F) ->
    case enslave(Pool, Timeout) of
	{ok, Pid} ->
	    try
		F(Pid)
	    after
		ok = release(Pool, Pid)
	    end;
	Error ->
	    Error
    end.

give_away(Pool, Worker, NewOwner) ->
    gen_server:call(Pool, {give_away, Worker, NewOwner}, infinity).

info(Pool) ->
    info(Pool, []).

info(Pool, Item) when is_atom(Item) ->
    [{Item, Value}] = info(Pool, [Item]),
    Value;
info(Pool, Items) when is_list(Items) ->
    case gen_server:call(Pool, {info, Items}) of
	{ok, Values} ->
	    Values;
	{error, Reason} ->
	    throw(Reason)
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([Name, Sup, PoolSize, MFA, Opts]) ->
    case init_state(Name, PoolSize, MFA, Opts) of
	{ok, State} ->
	    self() ! {start_worker_pool, Sup},
	    {ok, State};
	Error ->
	    {stop, Error}
    end.

-define(DEF_ALIVE_TIMEOUT, 0).
-define(DEF_STEP, 1).
-define(DEF_QUEUE_MAX, infinity).

init_state(Name, PoolSize, MFA, Opts) ->
    init_state(Name, PoolSize, MFA, Opts, #state{}).

init_state(Name, PoolSize, MFA, Opts, State) when is_integer(PoolSize) ->
    init_state(Name, {PoolSize, PoolSize}, MFA, Opts, State);
init_state(Name, {Min, Max}, {_, _, _} = MFA, Opts, State) 
  when is_atom(Name),
       is_integer(Min) and (Min >= 0),
       (is_integer(Max) and (Max >= Min)) or (Max =:= infinity),
       is_list(Opts) ->

    State1 = State#state{
	       name = Name,
	       min = Min,
	       max = Max,
	       mfa = MFA,
	       aliveTimeout = ?DEF_ALIVE_TIMEOUT,
	       step = ?DEF_STEP,
	       workers = ets:new(workers, [private]),
	       monitors = ets:new(monitors, [private]),
	       queueMax = ?DEF_QUEUE_MAX
	      },

    case set_pool_opts(Opts, State1) of
	{ok, State2} ->
	    {ok, State2};
	Error ->
	    Error
    end;
init_state(_, _, _, _, _) ->
    {error, badarg}.

set_pool_opts([], State) ->
    {ok, State};

set_pool_opts([{alive_timeout, Value} | Rest], State) 
  when is_integer(Value), Value >= 0; 
       Value =:= infinity ->
    set_pool_opts(Rest, State#state{aliveTimeout = Value});

set_pool_opts([{step, Value} | Rest], State) 
  when is_integer(Value), Value > 0 ->
    set_pool_opts(Rest, State#state{step = Value});

set_pool_opts([{queue_max, Value} | Rest], State) 
  when is_integer(Value), Value >= 0; 
       Value =:= infinity ->
    set_pool_opts(Rest, State#state{queueMax = Value});

set_pool_opts(_, _) ->
    {error, badarg}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({enslave, Timeout}, {FromPid, _} = From, State) ->
    case handle_on_enslave(From, Timeout, State) of
	{{ok, queued}, NewState} ->
	    {noreply, NewState};
	{{ok, Worker}, NewState} ->
	    true = monitors_add_owner(FromPid, Worker, NewState),
	    {reply, {ok, Worker}, NewState};
	{Error, NewState} ->
	    {reply, Error, NewState}
    end;

handle_call({give_away, Worker, NewOwner}, {FromPid, _} = _From, State) ->
    case monitors_del_owner(FromPid, Worker, State) of
	true ->
	    true = monitors_add_owner(NewOwner, Worker, State),
	    {reply, ok, State};
	false ->
	    {reply, {error, not_owner}, State}
    end;

handle_call({info, Items}, _From, State) ->
    Info = get_pool_info(Items, State),
    {reply, Info, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({release, Owner, Worker}, State) ->
    case monitors_del_owner(Owner, Worker, State) of
	true ->
	    case handle_on_release(Worker, State) of
		{{ok, NewOwner}, NewState} ->
		    {NewOwnerPid, _} = NewOwner,
		    true = monitors_add_owner(NewOwnerPid, Worker, NewState),
		    gen_server:reply(NewOwner, {ok, Worker});
		{ok, NewState} ->
		    ok
	    end,
	    {noreply, NewState};
	false ->
	    {noreply, State}
    end;

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({start_worker_pool, Sup}, State) ->
    {ok, NewState} = start_worker_pool(State, Sup),
    {noreply, NewState};

handle_info({checkin, Worker, Id}, State) ->
    case handle_on_checkin(Worker, Id, State) of
	{{ok, Owner}, NewState} ->
	    {OwnerPid, _} = Owner,
	    true = monitors_add_owner(OwnerPid, Worker, NewState),
	    gen_server:reply(Owner, {ok, Worker});
	{ok, NewState} ->
	    ok
    end,
    {noreply, NewState};

handle_info({dismiss, Pid, Tag}, State) ->
    NewState = handle_on_dismiss(Pid, Tag, State),
    {noreply, NewState};

handle_info({'DOWN', Monitor, process, Pid, _Reason}, State) ->
    #state{monitors = Monitors} = State,
    case ets:lookup(Monitors, Pid) of
	[{Pid, worker, Monitor, _}] ->
	    {ok, NewState} = workers_delete(Pid, State),
	    {noreply, NewState};

	[{Pid, owner, Monitor, Workers}] ->
	    true = ets:delete(Monitors, Pid),
	    NewState = lists:foldl(
			 fun (Worker, S) -> 
				 case handle_on_release(Worker, S) of
				     {{ok, NewOwner}, S1} ->
					 true = monitors_add_owner(NewOwner, Worker, S1),
					 gen_server:reply(NewOwner, {ok, Worker});
				     {ok, S1} ->
					 ok
				 end,
				 S1
			 end,
			 State, Workers),
	    {noreply, NewState};

	[] ->
	    {noreply, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% Handle events

handle_on_checkin(Worker, WorkerId, State) ->
    case waiting_queue_pop(State) of
	{{ok, Pid}, State1} ->
	    {ok, State2} = workers_checkin(Worker, WorkerId, busy, State1),
	    {{ok, Pid}, State2};

	{empty, State1} ->
	    {ok, State2} = workers_checkin(Worker, WorkerId, idle, State1),
	    {true, NewState} = workers_release(Worker, State2),
	    {ok, NewState}
    end.

handle_on_release(Worker, State) ->
    case workers_is_alive(Worker, State) of
	true ->
	    case waiting_queue_pop(State) of
		{{ok, Pid}, State1} ->
		    {{ok, Pid}, State1};

		{empty, State1} ->
		    {true, NewState} = workers_release(Worker, State1),
		    {ok, NewState}
	    end;
	false ->
	    {ok, State}
    end.

handle_on_dismiss(Pid, Tag, State) ->
    {_, NewState} = workers_try_dismiss_worker(Pid, Tag, State),
    NewState.

handle_on_enslave(Pid, WaitTimeout, State) ->
    case workers_enslave(State) of
	{{true, Worker}, State1} ->
	    {{ok, Worker}, State1};

	{false, State1} ->
	    case WaitTimeout of
		none ->
		    {{error, no_idle}, State1};
		_ ->
		    case waiting_queue_push(Pid, WaitTimeout, State1) of
			{ok, State2} ->
			    {{ok, queued}, State2};
			Error ->
			    {Error, State1}
		    end
	    end
    end.

%% Owners' monitors

monitors_add_owner(Owner, Worker, #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Owner) of
	[{Owner, owner, _, Workers}] ->
	    true = ets:update_element(Monitors, Owner, [{4, [Worker | Workers]}]);
	[] ->
	    Monitor = monitor(process, Owner),
	    true = ets:insert(Monitors, {Owner, owner, Monitor, [Worker]})
    end,
    true.

monitors_del_owner(Owner, Worker, #state{monitors = Monitors}) ->
    case ets:lookup(Monitors, Owner) of
	[{Owner, owner, Monitor, Workers}] ->
	    Workers1 = lists:filter(fun (W) -> W =/= Worker end, Workers),
	    N = length(Workers1),
	    case N < length(Workers) of
		true when N > 0 ->
		    true = ets:update_element(Monitors, Owner, [{4, [Workers1]}]),
		    true;
		true when N == 0 ->
		    true = demonitor(Monitor),
		    true = ets:delete(Monitors, Owner),
		    true;
		false ->
		    false
	    end;

	[] ->
	    false
    end.

%% Info

-define(EPOOL_INFO_ITEMS, [size, min, max, alive_timeout, step, 
			   count, busy, idle, 
			   queue_max, queue_size]).

get_pool_info([], State) ->
    get_pool_info(?EPOOL_INFO_ITEMS, [], State);
get_pool_info(Items, State) ->
    get_pool_info(Items, [], State).

get_pool_info([], Values, _) ->
    {ok, Values};
get_pool_info([Item | Rest], Values, State) ->
    case get_pool_info_item(Item, State) of
	{ok, Value} ->
	    get_pool_info(Rest, [{Item, Value} | Values], State);
	Error ->
	    Error
    end.

get_pool_info_item(size, #state{min = Min, max = Max}) ->
    {ok, {Min, Max}};
get_pool_info_item(min, #state{min = Min}) ->
    {ok, Min};
get_pool_info_item(max, #state{max = Max}) ->
    {ok, Max};
get_pool_info_item(alive_timeout, #state{aliveTimeout = Timeout}) ->
    {ok, Timeout};
get_pool_info_item(step, #state{step = Step}) ->
    {ok, Step};
get_pool_info_item(count, #state{min = Min, overflow = Overflow}) ->
    {ok, Min + Overflow};
get_pool_info_item(overflow, #state{overflow = Overflow}) ->
    {ok, Overflow};
get_pool_info_item(busy, #state{workers = Workers}) ->
    List = ets:match_object(Workers, {'_', '_', busy, '_'}),
    {ok, length(List)};
get_pool_info_item(idle, #state{workers = Workers}) ->
    List = ets:match_object(Workers, {'_', '_', idle, '_'}),
    {ok, length(List)};
get_pool_info_item(queue_max, #state{queueMax = Size}) ->
    {ok, Size};
get_pool_info_item(queue_size, #state{queue = Queue}) ->
    {ok, queue:len(Queue)};
get_pool_info_item(_, _) ->
    {error, badarg}.

%% Start processes

start_worker_pool(State = #state{min = Min}, SupRef) ->
    WorkerSupSpec = {epool_worker_sup, {epool_worker_sup, start_link, []},
		     permanent, 10000, supervisor, [epool_worker_sup]},
    {ok, Pid} = supervisor:start_child(SupRef, WorkerSupSpec),
    link(Pid),
    {ok, NewState} = start_workers(true, Min, State#state{workerSup = Pid}),
    {ok, NewState}.

start_workers(Sync, N, State) when N > 0 ->
    NewState = lists:foldl(
		 fun (_, S) ->
			 {ok, {Pid, Id}} = workers_new(Sync, S),
			 case Sync of
			     true ->
				 {ok, S1} = workers_checkin(Pid, Id, idle, S),
				 S1;
			     false ->
				 S
			 end
		 end,
		 State, lists:seq(1, N)
	  ),
    {ok, NewState};
start_workers(_, _, State) ->
    {ok, State}.

start_worker(Sup, Id, MFA = {WorkerModule, _, _}, Sync) ->
    WorkerSpec = {Id, {epool, start_link_worker, [self(), Id, MFA]},
		  permanent, 3000, worker, [epool_pool, WorkerModule]},
    {ok, Pid} = supervisor:start_child(Sup, WorkerSpec),

    case Sync of
	true ->
	    receive
		{checkin, Pid, Id} ->
		    Pid
	    after 0 ->
		    error(start_worker)
	    end;
	false ->
	    Pid
    end.
    
start_link_worker(Pool, Id, {M, F, A}) ->
    case apply(M, F, A) of
	{ok, Pid} ->
	    checkin(Pool, Pid, Id),
	    {ok, Pid};
	{ok, Pid, _} ->
	    checkin(Pool, Pid, Id),
	    {ok, Pid};
	Error ->
	    Error
    end.

checkin(Ref, Pid, Id) ->
    Ref ! {checkin, Pid, Id}.

%% Workers

workers_new(Sync, State) ->
    #state{name = Name,
	   workerSup = Sup,
	   mfa = MFA
	  } = State,

    WorkerId = workers_make_id(Name),
    Pid = start_worker(Sup, WorkerId, MFA, Sync),

    {ok, {Pid, WorkerId}}.

workers_make_id(_) ->
    make_ref().

workers_checkin(Pid, WorkerId, WorkerState, State) ->
    workers_checkin(Pid, WorkerId, WorkerState, undefined, State).

workers_checkin(Pid, WorkerId, WorkerState, TimerInfo, State) ->
    #state{workers = Workers, monitors = Monitors} = State,
    Monitor = monitor(process, Pid),
    true = ets:insert(Workers, {Pid, WorkerId, WorkerState, TimerInfo}),
    true = ets:insert(Monitors, {Pid, worker, Monitor, undefined}),
    {ok, State}.

workers_destroy(WorkerId, #state{workerSup = Sup}) ->
    ok = supervisor:terminate_child(Sup, WorkerId),
    ok = supervisor:delete_child(Sup, WorkerId),
    ok.

workers_delete(Pid, State) ->
    workers_delete(Pid, false, State).

workers_delete(Pid, Destroy, #state{workers = Workers, monitors = Monitors} = State) ->
    case ets:lookup(Workers, Pid) of
	[{Pid, Id, _, _}] ->
	    case ets:lookup(Monitors, Pid) of
		[{Pid, worker, Monitor, _}] ->
		    demonitor(Monitor),
		    ets:delete(Monitors, Pid);
		_ ->
		    ok
	    end,
	    ets:delete(Workers, Pid),
	    case Destroy of
		true ->
		    ok = workers_destroy(Id, State);
		false ->
		    ok
	    end;
	_ ->
	    ok
    end,
    {ok, State}.

workers_is_alive(Pid, #state{workers = Workers}) ->
    case ets:lookup(Workers, Pid) of
	[{Pid, _, _, _}] ->
	    true;
	[] ->
	    false
    end.

workers_dismiss(Pid, State) ->
    {ok, NewState} = workers_delete(Pid, true, State),
    {ok, NewState}.

workers_enslave(#state{workers = Workers} = State) ->    
    case ets:match_object(Workers, {'_', '_', idle, '_'}, 1) of
	{[{Pid, _, idle, TimerInfo}], _} ->
	    ok = dismiss_timer_cancel(TimerInfo),
	    true = ets:update_element(Workers, Pid, [{3, busy}, {4, undefined}]),
	    {{true, Pid}, State};

	'$end_of_table' ->
	    case workers_allow_new_workers(State) of
		{true, N} when N > 0 ->
		    {ok, {Pid, Id}} = workers_new(true, State),
		    {ok, State1} = workers_checkin(Pid, Id, busy, State),
		    {ok, State2} = start_workers(false, N-1, State1),
		    #state{overflow = Overflow} = State2,
		    {{true, Pid}, State2#state{overflow = Overflow + N}};

		false ->
		    {false, State}
	    end
    end.

workers_release(Pid, #state{workers = Workers} = State) ->
    case workers_allow_dismiss_worker(State) of
	true ->
	    {ok, State1} = workers_dismiss(Pid, State),
	    #state{overflow = Overflow} = State1,
	    case Overflow > 0 of
		true ->
		    {true, State1#state{overflow = Overflow - 1}}
	    end;

	{true, Timeout} ->
	    TimerInfo = dismiss_timer_create(Timeout, Pid),
	    true = ets:update_element(Workers, Pid, [{3, idle}, {4, TimerInfo}]),
	    {true, State};

	false ->
	    true = ets:update_element(Workers, Pid, [{3, idle}, {4, undefined}]),
	    {true, State}
    end.

workers_try_dismiss_worker(Pid, Tag, #state{workers = Workers, overflow = Overflow} = State) ->
    case ets:lookup(Workers, Pid) of
	[{Pid, _, idle, TimerInfo}] when Overflow > 0 ->
	    case dismiss_timer_check(Tag, TimerInfo) of
		true ->
		    {ok, State1} = workers_dismiss(Pid, State),
		    {ok, State2} = workers_cancel_dismiss_timers(State1#state{overflow = Overflow - 1}),
		    {true, State2};
		false ->
		    {false, State}
	    end;

	[{Pid, _, idle, _}] when Overflow == 0 ->
	    true = ets:update_element(Workers, Pid, [{4, undefined}]),
	    {false, State};

	_ ->
	    {false, State}
    end.

workers_cancel_dismiss_timers(#state{overflow = Overflow} = State) when Overflow > 0 ->
    {ok, State};
workers_cancel_dismiss_timers(#state{workers = Workers, overflow = Overflow} = State) when Overflow == 0 ->
    ets:foldl(fun (X, _) ->
		      case X of
			  {Pid, _, _, idle, {Timer, _}} -> 
			      ets:update_element(Workers, Pid, [{4, undefined}]),
			      timer:cancel(Timer),
			      ok;
			  _ ->
			      ok
		      end
	      end,
	      ok, Workers),
    {ok, State}.

dismiss_timer_create(Timeout, Pid) when is_integer(Timeout), Timeout > 0 ->
    Tag = make_ref(),
    {ok, Timer} = timer:send_after(Timeout, {dismiss, Pid, Tag}),
    {Timer, Tag}.

dismiss_timer_cancel(undefined) ->
    ok;
dismiss_timer_cancel({Timer, _}) ->
    timer:cancel(Timer),
    ok.

dismiss_timer_check(_, undefined) ->
    false;
dismiss_timer_check(TimerTag, {_, Tag}) ->
    TimerTag =:= Tag.

workers_allow_new_workers(#state{max = Max, step = Step}) when Max == infinity ->
    {true, Step};
workers_allow_new_workers(#state{min = Min, max = Max, overflow = Overflow, step = Step}) ->
    Allow = Max - (Min + Overflow),
    case Allow > 0 of
	true ->
	    {true, min(Step, Allow)};
	false ->
	    false
    end.

workers_allow_dismiss_worker(#state{overflow = Overflow}) when Overflow == 0 ->
    false;
workers_allow_dismiss_worker(#state{overflow = Overflow, aliveTimeout = Timeout}) when Overflow > 0 ->
    case Timeout of
	0 ->
	    true;
	infinity ->
	    false;
	_ when is_integer(Timeout), Timeout > 0 ->
	    {true, Timeout}
    end.

%% Waiting queue

waiting_queue_push(Pid, Timeout, State) ->
    #state{queue = Queue, queueMax = QueueMax} = State,
    case (QueueMax =:= infinity) or (queue:len(Queue) < QueueMax) of
	true ->
	    Queue1 = queue:in({Pid, Timeout, os:timestamp()}, Queue),
	    {ok, State#state{queue = Queue1}};
	false ->
	    {error, max_queue}
    end.

waiting_queue_pop(State = #state{queue = Queue}) ->
    case queue:out(Queue) of
	{{value, {Pid, Timeout, TmStart}}, Queue1} ->
	    NewState = State#state{queue = Queue1},
	    case waiting_timeout_valid(TmStart, Timeout) of
		true ->
		    {{ok, Pid}, NewState};
		false ->
		    waiting_queue_pop(NewState)
	    end;
	{empty, Queue} ->
	    {empty, State}
    end.

waiting_timeout_valid(_, infinity) ->
    true;
waiting_timeout_valid(TmStart, Timeout) ->
    Diff = timer:now_diff(os:timestamp(), TmStart),
    (Diff div 1000) < Timeout.


