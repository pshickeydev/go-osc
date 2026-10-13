# Changelog

## Unreleased

Fixed (triaged from upstream issues; relevant to triton's OSC listener on :6013):

-   Bundle parsing honored element lengths: elements are now read with a bounded
    reader, and a non-positive element length (trailing zeros after the last
    element) no longer drops the whole bundle. (#53)
-   A malformed datagram no longer terminates `Server.Serve`; parse failures
    are dropped and the read loop continues, while `ReceivePacket` still
    reports them to one-shot callers. `readPacket` returns an untyped nil
    packet for unknown leading bytes, and a truncated timetag argument no
    longer has its error swallowed. (#29)
-   `Message.Match` anchors the pattern at both ends: a message to `/time`
    matches neither a handler for `/time/str` nor one for `/foo/time`. (#44)
-   `getRegEx` compiles address patterns with `regexp.Compile`; an invalid
    pattern (e.g. a stray `\` in a garbled datagram) yields no-match instead
    of a panic in the dispatch goroutine.
-   `readBlob` uses `io.ReadFull`, fixing blobs larger than the bufio buffer
    (4096 bytes) and detecting truncated blobs. (#62)
-   `StandardDispatcher` is safe for concurrent use; `AddMsgHandler` may be
    called while the server is dispatching. (#64)

Docs:

-   README no longer claims wildcard support for handler addresses; wildcards
    apply to the address patterns of received messages. (#38)
