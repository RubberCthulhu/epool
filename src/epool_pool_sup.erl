
-module(epool_pool_sup).

-behaviour(supervisor).

%% API
-export([start_link/4]).

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
start_link(Name, PoolSize, MFA, Opts) ->
    supervisor:start_link(?MODULE, [Name, PoolSize, MFA, Opts]).

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
init([Name, PoolSize, MFA, Opts]) ->
    RestartStrategy = one_for_all,
    MaxRestarts = 1,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 5000,
    Type = worker,

    PoolManager = {epool, {epool, start_link, [Name, self(), PoolSize, MFA, Opts]},
	      Restart, Shutdown, Type, [epool]},

    {ok, {SupFlags, [PoolManager]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
