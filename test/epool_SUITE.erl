
-module(epool_SUITE).

-include_lib("common_test/include/ct.hrl").

%% ct
-export([all/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

%% app
-export([app_start_stop/1]).
-export([app_not_started/1]).
-export([app_already_started/1]).

%% create
-export([create_pool_start_stop/1]).

%% pool
-export([pool_info/1]).
-export([pool_workers/1]).
-export([pool_enslave_release/1]).
-export([pool_transaction/1]).
-export([pool_owner_die/1]).

%% overflow
-export([overflow_start/1]).
-export([overflow_notimeout/1]).
-export([overflow_timeout/1]).
-export([overflow_step/1]).

%% waiting_queue
-export([waiting_queue_wait/1]).
-export([waiting_queue_max_queue/1]).
-export([waiting_queue_timeout/1]).
-export([waiting_queue_run/1]).

-define(POOL_NAME, epool_test).
-define(WAIT_TIMEOUT, 3000).

-define(WORKER_SPEC, {epool_test_worker, start_link, []}).
-define(POOL_SPEC_COMMON, {{2, 3}, ?WORKER_SPEC, [{alive_timeout, 0}, {queue_max, infinity}]}).
-define(POOL_SPEC_POOL, ?POOL_SPEC_COMMON).
-define(POOL_SPEC_OVERFLOW, ?POOL_SPEC_COMMON).
-define(POOL_SPEC_OVERFLOW_TIMEOUT, {{2, 3}, ?WORKER_SPEC, [{alive_timeout, 1000}]}).
-define(POOL_SPEC_OVERFLOW_STEP, {{5, 7}, ?WORKER_SPEC, [{alive_timeout, infinity}, {step, 3}]}).
-define(POOL_SPEC_WAITING_QUEUE, {2, ?WORKER_SPEC, [{queue_max, 2}]}).

all() ->
    [{group, app}, {group, create}, {group, pool}, {group, overflow}, {group, waiting_queue}].

groups() ->
    [{app, [
	    app_start_stop,
	    app_not_started,
	    app_already_started
	   ]},
     {create, [
	       create_pool_start_stop
	    ]},
     {pool, [
	     pool_info,
	     pool_workers,
	     pool_enslave_release,
	     pool_transaction,
	     pool_owner_die
	    ]},
     {overflow, [
		 overflow_start,
		 overflow_notimeout,
		 overflow_timeout,
		 overflow_step
		]},
     {waiting_queue, [
		      waiting_queue_wait,
		      waiting_queue_max_queue,
		      waiting_queue_timeout,
		      waiting_queue_run
		     ]}
    ].

init_per_suite(Config) ->
    timer:start(),
    [{wait_timeout, ?WAIT_TIMEOUT}, 
     {pool_name, ?POOL_NAME}
     | Config].

end_per_suite(_) ->
    ok.

init_per_group(app, Config) ->
    [{pool_spec, ?POOL_SPEC_COMMON} | Config];
init_per_group(create, Config) ->
    ok = epool:start(),
    [{pool_spec, ?POOL_SPEC_COMMON} | Config];
init_per_group(pool, Config) ->
    Name = ?config(pool_name, Config),
    {PoolSize, MFA, Opts} = ?POOL_SPEC_POOL,
    ok = epool:start(),
    {ok, _Pool} = epool:start_pool(Name, PoolSize, MFA, Opts),
    [{pool_spec, ?POOL_SPEC_POOL} | Config];
init_per_group(overflow, Config) ->
    ok = epool:start(),
    Config;
init_per_group(waiting_queue, Config) ->
    ok = epool:start(),
    [{pool_spec, ?POOL_SPEC_WAITING_QUEUE} | Config].

end_per_group(app, _) ->
    ok;
end_per_group(create, _) ->
    ok = epool:stop(),
    ok;
end_per_group(pool, Config) ->
    Name = ?config(pool_name, Config),
    ok = epool:stop_pool(Name),
    ok = epool:stop();
end_per_group(overflow, _) ->
    ok = epool:stop();
end_per_group(waiting_queue, _) ->
    ok = epool:stop().

init_per_testcase(overflow_start, Config) ->
    config_start_pool([{pool_spec, ?POOL_SPEC_OVERFLOW} | Config]);
init_per_testcase(overflow_notimeout, Config) ->
    config_start_pool([{pool_spec, ?POOL_SPEC_OVERFLOW} | Config]);
init_per_testcase(overflow_timeout, Config) ->
    config_start_pool([{pool_spec, ?POOL_SPEC_OVERFLOW_TIMEOUT} | Config]);
init_per_testcase(overflow_step, Config) ->
    config_start_pool([{pool_spec, ?POOL_SPEC_OVERFLOW_STEP} | Config]);
init_per_testcase(waiting_queue_wait, Config) ->
    config_start_pool(Config);
init_per_testcase(waiting_queue_max_queue, Config) ->
    config_start_pool(Config);
init_per_testcase(waiting_queue_timeout, Config) ->
    config_start_pool(Config);
init_per_testcase(waiting_queue_run, Config) ->
    config_start_pool(Config);
init_per_testcase(_, Config) ->
    Config.

end_per_testcase(overflow_start, Config) ->
    config_stop_pool(Config);
end_per_testcase(overflow_notimeout, Config) ->
    config_stop_pool(Config);
end_per_testcase(overflow_timeout, Config) ->
    config_stop_pool(Config);
end_per_testcase(overflow_step, Config) ->
    config_stop_pool(Config);
end_per_testcase(waiting_queue_wait, Config) ->
    config_stop_pool(Config);
end_per_testcase(waiting_queue_max_queue, Config) ->
    config_stop_pool(Config);
end_per_testcase(waiting_queue_timeout, Config) ->
    config_stop_pool(Config);
end_per_testcase(waiting_queue_run, Config) ->
    config_stop_pool(Config);
end_per_testcase(_, _) ->
    ok.

config_start_pool(Config) ->
    Name = ?config(pool_name, Config),
    {PoolSize, MFA, Opts} = ?config(pool_spec, Config),
    {ok, _} = epool:start_pool(Name, PoolSize, MFA, Opts),
    Config.

config_stop_pool(Config) ->
    Name = ?config(pool_name, Config),
    epool:stop_pool(Name),
    ok.

app_start_stop(_Config) ->
    ok = epool:start(),
    ok = epool:stop(),
    ok.

app_not_started(Config) ->
    Name = ?config(pool_name, Config),
    {PoolSize, MFA, Opts} = ?config(pool_spec, Config),

    {error, epool_not_started} = epool:start_pool(Name, PoolSize, MFA, Opts),
    ok.

app_already_started(_Config) ->
    ok = epool:start(),
    {error, {already_started, epool}} = epool:start(),
    ok = epool:stop(),
    ok.

create_pool_start_stop(Config) ->
    Name = ?config(pool_name, Config),
    {PoolSize, MFA, Opts} = ?config(pool_spec, Config),

    {ok, Pid} = epool:start_pool(Name, PoolSize, MFA, Opts),
    Monitor = monitor(process, Pid),
    ok = epool:stop_pool(Name),
    receive
	{'DOWN', Monitor, process, Pid, shutdown} ->
	    ok
    after 3000 ->
	    ct:fail(timeout)
    end,
    ok.

pool_info(Config) ->
    Pool = ?config(pool_name, Config),

    _Opts = epool:info(Pool, [
			      size, min, max, count,
			      alive_timeout, step, 
			      overflow, busy, idle,
			      queue_max, queue_size]
		      ),
    ok.

pool_workers(Config) ->
    Pool = ?config(pool_name, Config),

    Count = epool:info(Pool, count),
    0 = epool:info(Pool, busy),
    Count = epool:info(Pool, idle),

    ok.

pool_enslave_release(Config) ->
    Pool = ?config(pool_name, Config),

    Min = epool:info(Pool, min),
    {ok, Pid} = epool:enslave(Pool, none),
    Min1 = Min - 1,
    Min1 = epool:info(Pool, idle),
    1 = epool:info(Pool, busy),
    ok = epool:release(Pool, Pid),
    Min = epool:info(Pool, idle),

    ok.

pool_transaction(Config) ->
    Pool = ?config(pool_name, Config),

    foo = transaction_echo(Pool, foo),
    ok.

pool_owner_die(Config) ->
    Pool = ?config(pool_name, Config),
    Min = epool:info(Pool, min),

    Self = self(),
    {Pid, Monitor} = spawn_monitor(
		       fun () ->
			       {ok, _} = epool:enslave(Pool, none),
			       Tag = make_ref(),
			       Self ! {ok, self(), Tag},
			       receive
				   {ok, Tag} ->
				       ok
			       end
		       end),

    receive
	{ok, Pid, Tag} ->
	    1 = epool:info(Pool, busy),
	    Pid ! {ok, Tag}
    after 3000 ->
	    ct:fail(timeout)
    end,

    receive
	{'DOWN', Monitor, process, Pid, _} ->
	    0 = epool:info(Pool, busy),
	    Min = epool:info(Pool, idle)
    after 3000 ->
	    ct:fail(timeout)
    end,
    ok.		    

overflow_start(Config) ->
    Pool = ?config(pool_name, Config),
    Min = epool:info(Pool, min),
    Max = epool:info(Pool, max),
    Overflow = Max - Min,

    _Workers = enslave_list(Pool, Min),
    0 = epool:info(Pool, idle),
    _OverflowWorkers = enslave_list(Pool, Max-Min),
    Overflow = epool:info(Pool, overflow),
    Max = epool:info(Pool, busy),

    ok.

overflow_notimeout(Config) ->
    Pool = ?config(pool_name, Config),
    Min = epool:info(Pool, min),
    Max = epool:info(Pool, max),
    Overflow = Max - Min,

    _Workers = enslave_list(Pool, Min),
    OverflowWorkers = enslave_list(Pool, Max-Min),
    Overflow = epool:info(Pool, overflow),
    release_list(Pool, OverflowWorkers),
    0 = epool:info(Pool, overflow),

    ok.

overflow_timeout(Config) ->
    Pool = ?config(pool_name, Config),
    Min = epool:info(Pool, min),
    AliveTimeout = epool:info(Pool, alive_timeout),

    Workers = enslave_list(Pool, Min),
    {ok, Worker} = epool:enslave(Pool, infinity),
    1 = epool:info(Pool, overflow),
    Monitor = monitor(process, Worker),
    Now = now(),
    ok = epool:release(Pool, Worker),
    release_list(Pool, Workers),
    receive
	{'DOWN', Monitor, process, Worker, _} ->
	    case timer:now_diff(now(), Now) >= (AliveTimeout * 1000) of
		true ->
		    Min = epool:info(Pool, count);
		false ->
		    ct:fail(wrong_timeout)
	    end
    after (AliveTimeout * 2) ->
	    ct:fail(timeout)
    end,
    ok.

overflow_step(Config) ->
    Pool = ?config(pool_name, Config),
    Min = epool:info(Pool, min),
    Max = epool:info(Pool, max),
    Step = epool:info(Pool, step),
    Overflow = min(Max-Min, Step),

    _Workers = enslave_list(Pool, Min),
    {ok, _Worker} = epool:enslave(Pool, infinity),
    Overflow = epool:info(Pool, overflow),

    ok.

waiting_queue_wait(Config) ->
    Pool = ?config(pool_name, Config),
    Max = epool:info(Pool, max),

    _Workers = enslave_list(Pool, Max),
    0 = epool:info(Pool, queue_size),
    _Waiting = spawn_enslave_list(Pool, 1),
    1 = epool:info(Pool, queue_size),

    ok.

waiting_queue_max_queue(Config) ->
    Pool = ?config(pool_name, Config),
    Max = epool:info(Pool, max),
    QueueMax = epool:info(Pool, queue_max),

    _Workers = enslave_list(Pool, Max),
    _Waiting = spawn_enslave_list(Pool, QueueMax),
    {error, max_queue} = epool:enslave(Pool, infinity),

    ok.

waiting_queue_timeout(Config) ->
    Pool = ?config(pool_name, Config),
    Max = epool:info(Pool, max),

    _Workers = enslave_list(Pool, Max),
    0 = epool:info(Pool, idle),
    Self = self(),
    {Pid, Monitor} = spawn_monitor(fun () ->
					   try
					       foo = transaction_echo(Pool, foo, 500)
					   catch
					       exit:{timeout, _} ->
						   Self ! {ok, self()}
					   end
				   end),
    receive
	{ok, Pid} ->
	    ok;
	{'DOWN', Monitor, process, Pid, Reason} ->
	    ct:fail({transaction_failed, Reason})
    after 3000 ->
	    ct:fail(timeout)
    end,
    ok.

waiting_queue_run(Config) ->
    Pool = ?config(pool_name, Config),
    Max = epool:info(Pool, max),

    [Worker | _Workers] = enslave_list(Pool, Max),
    0 = epool:info(Pool, idle),
    Self = self(),
    {Pid, Monitor} = spawn_monitor(fun () ->
					   Self ! {start, self()},
					   foo = transaction_echo(Pool, foo),
					   Self ! {ok, self()}
				   end),
    receive
	{start, Pid} ->
	    ok
    after 3000 ->
	    ct:fail(spawn_transaction_fail)
    end,
    1 = epool:info(Pool, queue_size),
    epool:release(Pool, Worker),
    receive
	{ok, Pid} ->
	    ok;
	{'DOWN', Monitor, process, Pid, Reason} ->
	    ct:fail({transaction_failed, Reason})
    after 3000 ->
	    ct:fail(timeout)
    end,
    ok.

enslave_list(Pool, N) ->
    lists:foldl(
      fun (_, Workers) ->
	      {ok, Pid} = epool:enslave(Pool, none),
	      [Pid | Workers]
      end,
      [], lists:seq(1, N)).

release_list(Pool, Workers) ->
    lists:foreach(fun (Pid) -> epool:release(Pool, Pid) end, Workers).

spawn_enslave_list(Pool, N) ->
    lists:foldl(
      fun (_, Pids) ->
	      Pid = spawn_enslave(Pool),
	      [Pid | Pids]
      end,
      [], lists:seq(1, N)).

spawn_enslave(Pool) ->
    Self = self(),
    Pid = spawn(
	    fun() ->
		    Self ! {ok, self()},
		    epool:enslave(Pool, infinity)
	    end),
    receive
	{ok, Pid} ->
	    Pid
    after 3000 ->
	    throw(spawn_enslave_fail)
    end.

transaction_echo(Pool, Token) ->
    transaction_echo(Pool, Token, infinity).

transaction_echo(Pool, Token, Timeout) ->
    epool:transaction(Pool, fun (Pid) -> epool_test_worker:echo(Pid, Token) end, Timeout).



