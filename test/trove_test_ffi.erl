-module(trove_test_ffi).
-export([suppress_crash_reports/0]).

suppress_crash_reports() ->
    logger:add_primary_filter(suppress_expected_test_noise, {
        fun(#{msg := {report, #{label := {proc_lib, crash}}}}, _) -> stop;
           (#{msg := {string, Msg}}, _) when is_list(Msg) ->
               case lists:prefix("Actor discarding unexpected message:", Msg) of
                   true -> stop;
                   false -> ignore
               end;
           (#{msg := {Fmt, Args}}, _) when is_list(Fmt), is_list(Args) ->
               try io_lib:format(Fmt, Args) of
                   Formatted ->
                       Flat = lists:flatten(Formatted),
                       case lists:prefix("Actor discarding unexpected message:", Flat) of
                           true -> stop;
                           false -> ignore
                       end
               catch _:_ -> ignore
               end;
           (_, _) -> ignore
        end, #{}
    }),
    nil.
