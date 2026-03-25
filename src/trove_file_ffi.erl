-module(trove_file_ffi).
-export([open/1, open_read/1, close/1, append/2, pread/3, datasync/1, file_size/1,
         mkdir_p/1, list_dir/1, delete_file/1, hash/1, dir_fsync/1,
         try_lock/1, unlock/1]).

open(Path) ->
    map_result(file:open(Path, [read, append, raw, binary])).

%% NOTE: `raw` is intentionally omitted here (unlike open/1) because snapshot
%% file handles are created by the DB actor but used from caller processes.
%% With `raw` mode, pread/close are restricted to the opening process.
open_read(Path) ->
    map_result(file:open(Path, [read, binary])).

close(Fd) ->
    map_ok_nil(file:close(Fd)).

append(Fd, Data) ->
    case file:write(Fd, Data) of
        ok ->
            case file:position(Fd, cur) of
                {ok, EndPos} -> {ok, EndPos - byte_size(Data)};
                {error, Reason} -> {error, atom_to_binary(Reason, utf8)}
            end;
        {error, Reason} -> {error, atom_to_binary(Reason, utf8)}
    end.

pread(Fd, Offset, Length) ->
    case file:pread(Fd, Offset, Length) of
        {ok, Data} -> {ok, Data};
        eof -> {error, <<"eof">>};
        {error, Reason} -> {error, atom_to_binary(Reason, utf8)}
    end.

datasync(Fd) ->
    map_ok_nil(file:datasync(Fd)).

file_size(Fd) ->
    map_result(file:position(Fd, eof)).

mkdir_p(Path) ->
    map_ok_nil(filelib:ensure_path(Path)).

list_dir(Path) ->
    case file:list_dir(Path) of
        {ok, Files} -> {ok, [unicode:characters_to_binary(F) || F <- Files]};
        {error, Reason} -> {error, atom_to_binary(Reason, utf8)}
    end.

delete_file(Path) ->
    map_ok_nil(file:delete(Path)).

hash(Data) ->
    <<Truncated:16/binary, _/binary>> = crypto:hash(blake2b, Data),
    Truncated.

dir_fsync(Path) ->
    case file:open(Path, [read, raw]) of
        {ok, Fd} ->
            Result = file:datasync(Fd),
            _ = file:close(Fd),
            map_ok_nil(Result);
        {error, eisdir} ->
            {ok, nil};
        {error, Reason} ->
            {error, atom_to_binary(Reason, utf8)}
    end.

ensure_lock_table() ->
    case ets:info(trove_db_locks) of
        undefined ->
            Self = self(),
            Ref = make_ref(),
            spawn(fun() ->
                try
                    ets:new(trove_db_locks, [named_table, public, set]),
                    Self ! {Ref, ok},
                    table_owner_loop()
                catch
                    error:badarg ->
                        %% Another process created it between our check and create
                        Self ! {Ref, ok}
                end
            end),
            receive
                {Ref, ok} -> ok
            after 5000 ->
                error(lock_table_timeout)
            end;
        _ ->
            ok
    end.

table_owner_loop() ->
    receive _ -> table_owner_loop() end.

try_lock(Path) ->
    ensure_lock_table(),
    Normalized = unicode:characters_to_binary(filename:absname(Path)),
    case ets:insert_new(trove_db_locks, {Normalized, self()}) of
        true ->
            {ok, nil};
        false ->
            [{_, Owner}] = ets:lookup(trove_db_locks, Normalized),
            case is_process_alive(Owner) of
                false ->
                    ets:delete(trove_db_locks, Normalized),
                    try_lock(Path);
                true ->
                    {error, <<"database is already open at this path">>}
            end
    end.

unlock(Path) ->
    ensure_lock_table(),
    Normalized = unicode:characters_to_binary(filename:absname(Path)),
    case ets:lookup(trove_db_locks, Normalized) of
        [{_, Owner}] when Owner =:= self() ->
            ets:delete(trove_db_locks, Normalized),
            {ok, nil};
        _ ->
            {ok, nil}
    end.

map_result({ok, Value}) -> {ok, Value};
map_result({error, Reason}) -> {error, atom_to_binary(Reason, utf8)}.

map_ok_nil(ok) -> {ok, nil};
map_ok_nil({error, Reason}) -> {error, atom_to_binary(Reason, utf8)}.
