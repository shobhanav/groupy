%% @author eavnvya
%% @doc @todo Add description to gms3.


-module(gms4).

%% ====================================================================
%% API functions
%% ====================================================================
-export([start/1, start/2, init/3, init/4]).


start(Id) ->
	Rnd = random:uniform(1000),
	Self = self(),
	{ok, spawn_link(fun()-> init(Id, Rnd, Self) end)}.

init(Id, Rnd, Master) ->
	random:seed(Rnd, Rnd, Rnd),
	leader(Id, Master,1, [], [Master],[]).

start(Id, Grp) ->
	Rnd = random:uniform(1000),
	Self = self(),
	{ok, spawn_link(fun()-> init(Id, Rnd, Grp, Self) end)}.

init(Id, Rnd, Grp, Master) ->
	random:seed(Rnd, Rnd, Rnd),
	Self = self(),
	Grp ! {join, Master, Self},
	receive
		{view, N, [Leader|Slaves], Group} ->
			Master ! {view, Group},
			erlang:monitor(process, Leader),
			slave(Id, Master, Leader, N+1, {view, N, [Leader|Slaves], Group}, Slaves, Group)
	after 3000 ->
		Master ! {error, "no reply from leader"}
	end.


leader(Id, Master,N, Slaves, Group, MsgList) ->
	receive
		{mcast, Msg} ->			
			bcast(Id, {msg, N, Msg}, Slaves),
			Master ! Msg,
			NewMsgList = [{N,Msg}|MsgList], %%store all multicasted messages
			leader(Id, Master, N+1, Slaves, Group, NewMsgList);
		{join, Wrk, Peer} ->
			Slaves2 = lists:append(Slaves, [Peer]),
			Group2 = lists:append(Group, [Wrk]),
			bcast(Id, {view, N, [self()|Slaves2], Group2}, Slaves2),
			Master ! {view, Group2},
			leader(Id, Master, N+1, Slaves2, Group2, MsgList);
		{resendMsg, I, P} ->			
			case lists:keyfind(I, 1, MsgList) of 
				{S,Message} ->
					io:format("Leader resending msgs~n"),
					P ! {resend_msg, S,Message},
					leader(Id, Master, N, Slaves, Group, MsgList);
				false ->
					io:format("Msg not found in leader's Msg List~n"),
					leader(Id, Master, N, Slaves, Group, MsgList)
				end;
		stop ->
			ok
	end.

slave(Id, Master, Leader,N, Last, Slaves, Group) ->	
	receive
		{mcast, Msg} ->
			Leader ! {mcast, Msg},
			slave(Id, Master, Leader, N, Last, Slaves, Group);
		{join, Wrk, Peer} ->
			Leader ! {join, Wrk, Peer},
			slave(Id, Master, Leader, N, Last, Slaves, Group);		
		{msg, I, _} when I < N -> 			
			slave(Id, Master, Leader, N, Last, Slaves, Group);
		{msg, I, Msg} when I > N ->			
			Ns = lists:seq(N,I),			
			io:format("Msg with seq numbers ~w lost by slave: ~w~n",[Ns,Id]),
			lists:map(fun(Missing) -> Leader ! {resendMsg, Missing, self()}  end, Ns),
			Master ! Msg,
			slave(Id, Master, Leader, I+1, {msg,I,Msg}, Slaves, Group);
		{msg, I, Msg} ->
			Master ! Msg,
			slave(Id, Master, Leader, I+1, {msg,I,Msg}, Slaves, Group);			
		{view, I, [Leader|Slaves2], Group2} ->
			Master ! {view, Group2},
			slave(Id, Master, Leader, I+1, {view, I, [Leader|Slaves2], Group2}, Slaves2, Group2);
		{'DOWN', _Ref, process, Leader, _Reason} ->			
			election(Id, Master, N, Last, Slaves, Group);
		{resend_msg, S,_} ->			
			io:format("Lost message with seq numbers ~w received again by slave: ~w~n",[S,Id]),
			slave(Id, Master, Leader, N, Last, Slaves, Group);
		stop ->
			ok
end.

%% ====================================================================
%% Internal functions
%% ====================================================================


bcast(Id, Msg, Nodes) ->
	lists:foreach(fun(Node) -> 	 dropMsg(Node, Msg) end, Nodes).
crash(Id) ->
	case random:uniform(100) of
		100 ->
			io:format("leader ~w: crash~n", [Id]),
			exit(no_luck);
		_ ->
			ok
	end.

dropMsg(Node, Msg) ->
	case random:uniform(100) of
		100 ->
			io:format("Msg dropped for node ~w~n", [Node]),
			drop;
		_ ->
			Node ! Msg
	end.


election(Id, Master,N, Last, Slaves, [_|Group]) ->
	Self = self(),
	case Slaves of
		[Self|Rest] ->
			bcast(Id, Last, Rest),
			bcast(Id, {view,N,Slaves,Group},Rest),
			Master ! {view, Group},
			leader(Id, Master, N+1, Rest, Group,[]);
		[Leader|Rest] ->
			erlang:monitor(process, Leader),
			slave(Id, Master, Leader, N, Last, Rest, Group)
	end.


