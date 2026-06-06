#!/usr/bin/env escript
%% -*- erlang -*-
%%
%% Micro-benchmark: io_uring write vs prim_file write.
%%
%% Usage:
%%   escript bench_io_uring.escript [iterations] [msg_size_bytes]
%%
%% Defaults: 50_000 iterations, 1024 bytes.
%%
%% Run from the rabbitmq-server root:
%%   escript bench_io_uring.escript
%%   escript bench_io_uring.escript 100000 512

-mode(compile).

main(Args) ->
    N    = arg(Args, 1, 50000),
    Size = arg(Args, 2, 1024),

    io:format("~nBench: ~b writes of ~b bytes each~n", [N, Size]),
    io:format("-----------------------------------------------~n"),

    Payload = crypto:strong_rand_bytes(Size),

    %% ---- prim_file baseline ----
    PrimFile = "/tmp/bench_prim.bin",
    {ok, Fd} = file:open(PrimFile, [write, binary, raw]),
    T0 = erlang:monotonic_time(microsecond),
    lists:foreach(fun(_) -> ok = file:write(Fd, Payload) end,
                  lists:seq(1, N)),
    T1 = erlang:monotonic_time(microsecond),
    file:close(Fd),
    file:delete(PrimFile),

    PrimUs = T1 - T0,
    PrimThroughput = throughput(N, Size, PrimUs),
    io:format("prim_file : ~8.2f ms  (~7.2f MB/s)~n",
              [PrimUs / 1000, PrimThroughput]),

    %% ---- io_uring ----
    IouFile = "/tmp/bench_iouring.bin",
    case start_io_uring(IouFile) of
        {ok, Ring, RawFd} ->
            T2 = erlang:monotonic_time(microsecond),
            write_loop_iouring(Ring, RawFd, Payload, N, 0),
            T3 = erlang:monotonic_time(microsecond),
            io_uring:teardown(Ring),
            file:delete(IouFile),

            IouUs = T3 - T2,
            IouThroughput = throughput(N, Size, IouUs),
            io:format("io_uring  : ~8.2f ms  (~7.2f MB/s)~n",
                      [IouUs / 1000, IouThroughput]),

            Ratio = PrimUs / IouUs,
            io:format("~nio_uring speedup: ~.2fx~n~n", [Ratio]);
        {error, Reason} ->
            io:format("io_uring unavailable (~p) — skipping~n~n", [Reason])
    end.

%% ---- helpers ----

write_loop_iouring(_Ring, _RawFd, _Payload, 0, _Off) ->
    ok;
write_loop_iouring(Ring, RawFd, Payload, Rem, Off) ->
    Ref = make_ref(),
    ok = io_uring:prep(Ring, Ref, {write, RawFd, Payload, Off}),
    ok = io_uring:submit(Ring),
    {ok, Cqe} = io_uring:wait_cqe(Ring),
    _Res = io_uring:cqe_res(Cqe),
    ok = io_uring:cqe_seen(Ring, Cqe),
    write_loop_iouring(Ring, RawFd, Payload, Rem - 1, Off + byte_size(Payload)).

start_io_uring(Path) ->
    try
        {ok, Ring} = io_uring:setup(256, 0),
        {ok, RawFd} = io_uring_file:open(Path, [wronly, creat]),
        {ok, Ring, RawFd}
    catch
        _:Reason -> {error, Reason}
    end.

throughput(N, Size, Microseconds) ->
    Bytes = N * Size,
    Seconds = Microseconds / 1_000_000,
    Bytes / Seconds / 1_048_576.   %% MB/s

arg(Args, Pos, Default) ->
    case lists:nth(Pos, Args ++ lists:duplicate(Pos, undefined)) of
        undefined -> Default;
        V         -> list_to_integer(V)
    end.
