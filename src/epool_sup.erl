%%%-------------------------------------------------------------------
%%% @author KernelPanic <alevandal@kernelpanic.svyazcom.ru>
%%% @copyright (C) 2013, KernelPanic
%%% @doc
%%%
%%% @end
%%% Created : 28 Jan 2013 by KernelPanic <alevandal@kernelpanic.svyazcom.ru>
%%%-------------------------------------------------------------------
-module(epool_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, stop/0, 
	 start_pool/4, stop_pool/1
]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

stop() ->
    case whereis(?SERVER) of
	P when is_pid(P) ->
	    exit(?SERVER, kill);
	_ ->
	    ok
    end.

start_pool(Name, PoolSize, MFA, Opts) ->
    ChildSpec = {Name,
		 {epool_pool_sup, start_link, [Name, PoolSize, MFA, Opts]},
		 permanent, 3000, supervisor, [epool_pool_sup]},

    try
	supervisor:start_child(?SERVER, ChildSpec)
    catch
	exit:{noproc, _} ->
	    {error, epool_not_started}
    end.

stop_pool(Name) ->
    supervisor:terminate_child(?SERVER, Name),
    supervisor:delete_child(?SERVER, Name).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 10,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    {ok, {SupFlags, []}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
