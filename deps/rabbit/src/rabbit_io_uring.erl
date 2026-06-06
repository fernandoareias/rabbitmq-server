%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

-module(rabbit_io_uring).

%% Adapter providing zero-copy async I/O via io_uring on Linux.
%%
%% Ring lifecycle
%%   create_ring/0,1 — allocates an io_uring ring (one per queue or writer).
%%   close_ring/1    — tears it down.
%%
%% Write path (zero-copy)
%%   write/4   — single binary write at a given file offset.
%%   writev/4  — iovec batch write: preps one SQE per binary, single submit.
%%               Eliminates iolist_to_binary; each binary is passed directly
%%               to the kernel without an extra user-space copy.
%%   pwritev/3 — scatter write: list of {Offset, Binary} pairs, one submit.
%%               Replaces file:pwrite/2 in the queue store / index flush paths.
%%
%% Read path (scatter gather)
%%   preadv/3  — scatter gather pread: list of {Offset, Size} pairs per fd,
%%               one submit, returns binaries in order.
%%
%% SQPOLL
%%   create_ring/1 with flag=sqpoll — the kernel thread polls the SQ ring
%%   continuously, eliminating the io_uring_enter syscall per submission.
%%   Requires Linux >= 5.12 for unprivileged use; falls back to normal ring
%%   on failure.
%%
%% Availability
%%   start/0 — detects kernel support once; result cached in persistent_term.
%%   is_available/0 — O(1) check thereafter.

-export([start/0, stop/0, is_available/0]).
-export([create_ring/0, create_ring/1, create_queue_ring/0, close_ring/1]).
-export([open_fd/1, open_fd/2, close_fd/2]).
-export([write/4, writev/4, pwritev/3]).
-export([preadv/3]).

-include_lib("kernel/include/logger.hrl").

-define(AVAILABLE_KEY, rabbit_io_uring_available).
-define(RING_ENTRIES,  256).

-type ring()    :: term().
-type raw_fd()  :: integer().

%%--------------------------------------------------------------------
%% Lifecycle
%%--------------------------------------------------------------------

-spec start() -> ok.
start() ->
    case persistent_term:get(?AVAILABLE_KEY, undefined) of
        undefined -> do_detect();
        _         -> ok
    end.

-spec stop() -> ok.
stop() ->
    catch persistent_term:erase(?AVAILABLE_KEY),
    ok.

-spec is_available() -> boolean().
is_available() ->
    persistent_term:get(?AVAILABLE_KEY, false).

%%--------------------------------------------------------------------
%% Ring management
%%--------------------------------------------------------------------

%% Creates a standard ring (no SQPOLL).
-spec create_ring() -> {ok, ring()} | {error, term()}.
create_ring() ->
    create_ring(normal).

%% Creates a ring with the given mode.
%% Mode = normal | sqpoll
%% SQPOLL eliminates the io_uring_enter syscall per submission; the kernel
%% polls the SQ ring continuously. Requires Linux >= 5.12 for unprivileged
%% processes. Falls back to a normal ring on failure.
-spec create_ring(normal | sqpoll) -> {ok, ring()} | {error, term()}.
create_ring(normal) ->
    io_uring:setup(?RING_ENTRIES, 0);
create_ring(sqpoll) ->
    Flag = io_uring:sqpoll_flag(),
    case io_uring:setup(?RING_ENTRIES, Flag) of
        {ok, _} = Ok -> Ok;
        {error, _} ->
            ?LOG_INFO("io_uring: SQPOLL unavailable (CAP_SYS_NICE required?)"
                      ", falling back to normal ring"),
            io_uring:setup(?RING_ENTRIES, 0)
    end.

%% Creates a ring using the mode configured via rabbitmq.conf
%% (message_store.io_uring_sqpoll). Used by queue store and index on init.
-spec create_queue_ring() -> {ok, ring()} | {error, term()}.
create_queue_ring() ->
    Mode = case application:get_env(rabbit, msg_store_io_uring_sqpoll, false) of
        true  -> sqpoll;
        false -> normal
    end,
    create_ring(Mode).

-spec close_ring(ring()) -> ok.
close_ring(Ring) ->
    _ = io_uring:teardown(Ring),
    ok.

%%--------------------------------------------------------------------
%% File descriptor management
%%--------------------------------------------------------------------

%% Opens a file for writing (O_WRONLY | O_CREAT).
-spec open_fd(file:filename()) -> {ok, raw_fd()} | {error, term()}.
open_fd(Path) ->
    io_uring_file:open(Path, [wronly, creat]).

%% Opens a file with explicit flag list, e.g. [rdonly] for reads.
-spec open_fd(file:filename(), [atom()]) -> {ok, raw_fd()} | {error, term()}.
open_fd(Path, Flags) ->
    io_uring_file:open(Path, Flags).

-spec close_fd(ring(), raw_fd()) -> ok | {error, term()}.
close_fd(Ring, RawFd) ->
    Ref = make_ref(),
    ok = io_uring:prep(Ring, Ref, {close, RawFd}),
    {ok, _} = io_uring:submit(Ring),
    collect_cqe(Ring, Ref).

%%--------------------------------------------------------------------
%% Write path
%%--------------------------------------------------------------------

%% Single binary write at Offset. Equivalent to pwrite(Fd, Data, Offset).
-spec write(ring(), raw_fd(), binary(), non_neg_integer()) -> ok | {error, term()}.
write(Ring, RawFd, Data, Offset) ->
    Ref = make_ref(),
    ok = io_uring:prep(Ring, Ref, {write, RawFd, Data, Offset}),
    {ok, _} = io_uring:submit(Ring),
    collect_cqe(Ring, Ref).

%% Zero-copy iovec batch write. Preps one write SQE per binary in IoVec,
%% then submits them all in a single io_uring_enter call.
%%
%% This replaces:  iolist_to_binary(IoVec) → write(Ring, Fd, Bin, Off)
%% With:           writev(Ring, Fd, IoVec, Off)  — no extra allocation.
%%
%% Each binary in IoVec is written sequentially starting from StartOffset.
%% Returns {ok, TotalBytesWritten}.
-spec writev(ring(), raw_fd(), [binary()], non_neg_integer()) ->
    {ok, non_neg_integer()} | {error, term()}.
writev(_Ring, _RawFd, [], _StartOffset) ->
    {ok, 0};
writev(Ring, RawFd, IoVec, StartOffset) ->
    {N, _} = lists:foldl(
        fun(Bin, {Count, Off}) ->
            Ref = make_ref(),
            ok = io_uring:prep(Ring, Ref, {write, RawFd, Bin, Off}),
            {Count + 1, Off + byte_size(Bin)}
        end, {0, StartOffset}, IoVec),
    {ok, _} = io_uring:submit(Ring),
    TotalSize = lists:foldl(fun(B, Acc) -> Acc + byte_size(B) end, 0, IoVec),
    case collect_n_cqes(Ring, N) of
        ok          -> {ok, TotalSize};
        {error, _} = E -> E
    end.

%% Scatter write: list of {FileOffset, Binary} pairs.
%% Replaces file:pwrite(Fd, [{Offset, Data}]) in flush_buffer paths.
%% All writes are submitted in a single io_uring_enter call.
-spec pwritev(ring(), raw_fd(), [{non_neg_integer(), binary()}]) ->
    ok | {error, term()}.
pwritev(_Ring, _RawFd, []) ->
    ok;
pwritev(Ring, RawFd, OffsetBins) ->
    N = lists:foldl(
        fun({Offset, Bin}, Count) ->
            Ref = make_ref(),
            ok = io_uring:prep(Ring, Ref, {write, RawFd, Bin, Offset}),
            Count + 1
        end, 0, OffsetBins),
    {ok, _} = io_uring:submit(Ring),
    collect_n_cqes(Ring, N).

%%--------------------------------------------------------------------
%% Read path (scatter gather)
%%--------------------------------------------------------------------

%% Scatter pread: preps one read SQE per {Offset, Size} entry, single submit.
%% Returns binaries in the same order as the input list.
%%
%% Replaces N individual file:pread calls with a single io_uring_enter.
-spec preadv(ring(), raw_fd(), [{non_neg_integer(), pos_integer()}]) ->
    {ok, [binary()]} | {error, term()}.
preadv(_Ring, _RawFd, []) ->
    {ok, []};
preadv(Ring, RawFd, OffsetSizes) ->
    %% Use the {Offset, Size} tuple as the correlation tag so we can
    %% reassemble results in order after collecting CQEs out-of-order.
    Indexed = lists:zip(OffsetSizes, lists:seq(1, length(OffsetSizes))),
    lists:foreach(
        fun({{Offset, Size}, Idx}) ->
            ok = io_uring:prep(Ring, {read_tag, Idx}, {read, RawFd, Size, Offset})
        end, Indexed),
    {ok, _} = io_uring:submit(Ring),
    N = length(OffsetSizes),
    case collect_read_cqes(Ring, N, #{}) of
        {ok, ResultMap} ->
            Bins = [maps:get(I, ResultMap) || I <- lists:seq(1, N)],
            {ok, Bins};
        {error, _} = E -> E
    end.

%%--------------------------------------------------------------------
%% Internal helpers
%%--------------------------------------------------------------------

do_detect() ->
    Enabled = application:get_env(rabbit, msg_store_io_uring, false),
    Available = case {Enabled, os:type()} of
        {true, {unix, linux}} ->
            try
                {ok, R} = io_uring:setup(1, 0),
                io_uring:teardown(R),
                true
            catch _:_ ->
                false
            end;
        _ ->
            false
    end,
    persistent_term:put(?AVAILABLE_KEY, Available),
    case Available of
        true ->
            Mode = application:get_env(rabbit, msg_store_io_uring_sqpoll, false),
            ?LOG_INFO("io_uring: enabled (sqpoll=~p)", [Mode]);
        false ->
            ok
    end.

%% Collects one CQE matching Ref; discards stale CQEs from prior ops.
collect_cqe(Ring, Ref) ->
    {ok, Cqe} = io_uring:wait_cqe(Ring),
    Tag = io_uring:cqe_tag(Cqe),
    Res = io_uring:cqe_res(Cqe),
    ok = io_uring:cqe_seen(Ring, Cqe),
    if
        Tag =:= Ref, Res >= 0 -> ok;
        Tag =:= Ref            -> {error, Res};
        true                   -> collect_cqe(Ring, Ref)
    end.

%% Collects N write CQEs in one dirty-scheduler call via wait_n_cqes.
%% Replaces the N-call loop that caused N dirty-scheduler switches per flush.
collect_n_cqes(_Ring, 0) ->
    ok;
collect_n_cqes(Ring, N) ->
    case io_uring:wait_n_cqes(Ring, N) of
        {ok, Results}  -> check_write_results(Results);
        {error, _} = E -> E
    end.

check_write_results([]) ->
    ok;
check_write_results([{_Tag, Res, _Data} | Rest]) when Res >= 0 ->
    check_write_results(Rest);
check_write_results([{_Tag, Res, _Data} | _]) ->
    {error, Res}.

%% Collects N read CQEs; accumulates results by index tag.
%% Uses per-CQE wait_cqe to avoid interaction issues between batch collection
%% and the cq_lock held during reads.
collect_read_cqes(_Ring, 0, Acc) ->
    {ok, Acc};
collect_read_cqes(Ring, N, Acc) ->
    {ok, Cqe} = io_uring:wait_cqe(Ring),
    Tag = io_uring:cqe_tag(Cqe),
    Res = io_uring:cqe_res(Cqe),
    case {Tag, Res} of
        {{read_tag, Idx}, R} when R >= 0 ->
            %% cqe_data returns {ok, Binary}; must be called before cqe_seen,
            %% as cqe_seen frees the CQE context.
            {ok, Data} = io_uring:cqe_data(Cqe),
            ok = io_uring:cqe_seen(Ring, Cqe),
            collect_read_cqes(Ring, N - 1, Acc#{Idx => Data});
        {_, R} when R < 0 ->
            ok = io_uring:cqe_seen(Ring, Cqe),
            {error, R};
        _ ->
            ok = io_uring:cqe_seen(Ring, Cqe),
            collect_read_cqes(Ring, N, Acc)
    end.
