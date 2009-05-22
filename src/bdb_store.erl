%%%-------------------------------------------------------------------
%%% @author Matt Williamson <dawsdesign@gmail.com>
%%% @copyright (C) 2009, Matt Williamson
%%% @doc
%%%
%%% @end
%%% Created : 18 May 2009 by Matt Williamson <dawsdesign@gmail.com>
%%%-------------------------------------------------------------------
-module(bdb_store).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define('DRIVER_NAME', 'bdb_drv').

-record(state, {port}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initiates the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    SearchDir = filename:join([filename:dirname(code:which(?MODULE)), "..", "priv"]),
    case erl_ddll:load(SearchDir, atom_to_list(?DRIVER_NAME)) of
	ok ->
	    {ok, #state{port=open_port({spawn, ?DRIVER_NAME}, [binary])}};
	Error ->
	    Error
    end.

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
%%-------------------------------------------------------------------
handle_call({put, Key, Value}, _From, State)
  when is_binary(Key) and is_binary(Value) ->
    io:format("Putting value for key ~p...~n", [Key]),
    Message = <<1, Key/binary, Value/binary>>,
    io:format("Message -> C: ~p~n", [Message]),
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};

handle_call({get, Key}, _From, State)
  when is_binary(Key) ->
    io:format("Getting value for key ~p...~n", [Key]),
    Message = <<2, Key/binary>>,
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};

handle_call({delete, Key}, _From, State)
  when is_binary(Key) ->
    io:format("Deleting value for key ~p...~n", [Key]),
    Message = <<3, Key/binary>>,
    Reply = send_command(State#state.port, Message),
    {reply, Reply, State};

handle_call(_Request, _From, State) ->
    Reply = {error, unkown_call},
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
terminate(_Reason, State) ->
    port_close(State#state.port),
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
send_command(Port, Command) ->
    port_command(Port, Command),
    receive
	Data ->
	    io:format("Data: ~p~n", [Data]),
	    Data
    after 500 ->
	    io:format("Received nothing!~n"),
	    {error, timeout}
    end.
