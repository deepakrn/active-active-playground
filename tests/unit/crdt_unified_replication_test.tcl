# Active-Active CRDT Unified Replication Integration Test Suite
# Tests:
# 1. CRDT.OP wire protocol and centralized dispatch
# 2. Echo suppression and clock advancement
# 3. String HLC-LWW convergence
# 4. Set Two-Phase LWW convergence
# 5. Counter PN-Vector integer & float convergence
# 6. Stream disjoint triplet interleaving
# 7. Keyspace deletion tombstones & survival invariants
# 8. RDB DUMP/RESTORE and Non-Destructive State Merge

start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "1" "save" ""]] {
    set node_a [srv 0 client]
    set node_a_host [srv 0 host]
    set node_a_port [srv 0 port]

    test "Unified CRDT.OP Envelope: String LWW execution and echo suppression" {
        $node_a del mystr
        # Replicated write from Node 2 with HLC 1000
        assert_equal [$node_a crdt.op 2 1000 1 1 mystr "first_value"] "OK"
        assert_equal [$node_a get mystr] "first_value"

        # Stale replicated write from Node 2 with HLC 500 (lower) should be ignored
        assert_equal [$node_a crdt.op 2 500 1 1 mystr "stale_value"] "OK"
        assert_equal [$node_a get mystr] "first_value"

        # Newer replicated write from Node 3 with HLC 2000 wins
        assert_equal [$node_a crdt.op 3 2000 1 1 mystr "winner_value"] "OK"
        assert_equal [$node_a get mystr] "winner_value"

        # Echo suppression: write with node's own origin_id (1) is ignored
        assert_equal [$node_a crdt.op 1 3000 1 1 mystr "echoed_value"] "OK"
        assert_equal [$node_a get mystr] "winner_value"
    }

    test "Unified CRDT.OP Envelope: Set Two-Phase LWW Add and Remove" {
        $node_a del myset
        # Add apple at HLC 1000 from Node 2
        assert_equal [$node_a crdt.op 2 1000 2 1 myset "apple"] "OK"
        assert_equal [$node_a scard myset] 1
        assert_equal [$node_a sismember myset "apple"] 1

        # Add banana at HLC 1500 from Node 3
        assert_equal [$node_a crdt.op 3 1500 2 1 myset "banana"] "OK"
        assert_equal [$node_a scard myset] 2

        # Remove apple at HLC 2000 from Node 2
        assert_equal [$node_a crdt.op 2 2000 2 2 myset "apple"] "OK"
        assert_equal [$node_a scard myset] 1
        assert_equal [$node_a sismember myset "apple"] 0
        assert_equal [$node_a sismember myset "banana"] 1

        # Out-of-order stale add of apple at HLC 1200 < 2000 is ignored
        assert_equal [$node_a crdt.op 3 1200 2 1 myset "apple"] "OK"
        assert_equal [$node_a sismember myset "apple"] 0

        # Concurrent revive: add apple at HLC 3000 > 2000 succeeds
        assert_equal [$node_a crdt.op 3 3000 2 1 myset "apple"] "OK"
        assert_equal [$node_a sismember myset "apple"] 1
        assert_equal [$node_a scard myset] 2
    }

    test "Unified CRDT.OP Envelope: Counter PN-Vector Integer & Float Increments" {
        $node_a del mycnt
        # Origin 2 increments by 10
        assert_equal [$node_a crdt.op 2 1000 3 1 mycnt "10"] "OK"
        assert_equal [$node_a get mycnt] "10"

        # Origin 3 increments by 25
        assert_equal [$node_a crdt.op 3 1100 3 1 mycnt "25"] "OK"
        assert_equal [$node_a get mycnt] "35"

        # Origin 2 decrements by -5
        assert_equal [$node_a crdt.op 2 1200 3 1 mycnt "-5"] "OK"
        assert_equal [$node_a get mycnt] "30"

        # Float counter
        $node_a del myfloatcnt
        assert_equal [$node_a crdt.op 2 1000 3 2 myfloatcnt "0.25"] "OK"
        assert_equal [$node_a crdt.op 3 1100 3 2 myfloatcnt "0.75"] "OK"
        set float_val [$node_a get myfloatcnt]
        assert {abs($float_val - 1.0) < 0.0001}
    }

    test "Unified CRDT.OP Envelope: Stream Disjoint Triplet ID Interleaving & Ack" {
        $node_a del mystrm
        # Append from Node 2
        assert_equal [$node_a crdt.op 2 1000 4 1 mystrm "1000-2-0" "sensor" "temperature"] "OK"
        assert_equal [$node_a xlen mystrm] 1

        # Append from Node 3 at the identical timestamp
        assert_equal [$node_a crdt.op 3 1000 4 1 mystrm "1000-3-0" "sensor" "humidity"] "OK"
        assert_equal [$node_a xlen mystrm] 2

        # Acknowledge entry in PEL
        assert_equal [$node_a crdt.op 2 2000 4 2 mystrm "1000-2-0"] "OK"
    }

    test "Unified CRDT.OP Envelope: Hierarchical Keyspace Deletion Tombstones" {
        $node_a del mydelkey
        # Set string value at HLC 1000
        assert_equal [$node_a crdt.op 2 1000 1 1 mydelkey "survivor"] "OK"
        assert_equal [$node_a get mydelkey] "survivor"

        # Explicit deletion tombstone at HLC 2000
        assert_equal [$node_a crdt.op 2 2000 6 1 mydelkey] "OK"
        assert_equal [$node_a exists mydelkey] 0

        # Stale write arriving with HLC 1500 < 2000 is masked by key tombstone
        assert_equal [$node_a crdt.op 3 1500 1 1 mydelkey "zombie_write"] "OK"
        assert_equal [$node_a exists mydelkey] 0

        # Newer write arriving with HLC 3000 > 2000 survives
        assert_equal [$node_a crdt.op 3 3000 1 1 mydelkey "revived_write"] "OK"
        assert_equal [$node_a exists mydelkey] 1
        assert_equal [$node_a get mydelkey] "revived_write"
    }

    test "Unified CRDT RDB Persistence: DUMP and RESTORE across CRDT types" {
        # 1. String CRDT
        $node_a del test_str
        assert_equal [$node_a crdt.op 2 1000 1 1 test_str "persist_me"] "OK"
        set dump_str [$node_a dump test_str]
        $node_a del test_str
        $node_a restore test_str 0 $dump_str
        assert_equal [$node_a get test_str] "persist_me"

        # 2. Set CRDT
        $node_a del test_set
        assert_equal [$node_a crdt.op 2 1000 2 1 test_set "memberA"] "OK"
        assert_equal [$node_a crdt.op 2 1100 2 1 test_set "memberB"] "OK"
        set dump_set [$node_a dump test_set]
        $node_a del test_set
        $node_a restore test_set 0 $dump_set
        assert_equal [$node_a scard test_set] 2
        assert_equal [$node_a sismember test_set "memberA"] 1
        assert_equal [$node_a sismember test_set "memberB"] 1

        # 3. Counter CRDT
        $node_a del test_cnt
        assert_equal [$node_a crdt.op 2 1000 3 1 test_cnt "42"] "OK"
        set dump_cnt [$node_a dump test_cnt]
        $node_a del test_cnt
        $node_a restore test_cnt 0 $dump_cnt
        assert_equal [$node_a get test_cnt] "42"

        # 4. Stream CRDT
        $node_a del test_strm
        assert_equal [$node_a crdt.op 2 1000 4 1 test_strm "1000-2-0" "field1" "value1"] "OK"
        set dump_strm [$node_a dump test_strm]
        $node_a del test_strm
        $node_a restore test_strm 0 $dump_strm
        assert_equal [$node_a xlen test_strm] 1
    }

    test "Unified CRDT RDB SAVE and In-Memory Integrity" {
        $node_a del test_str test_set test_cnt test_strm
        assert_equal [$node_a crdt.op 2 1000 1 1 test_str "str_val"] "OK"
        assert_equal [$node_a crdt.op 2 1000 2 1 test_set "member1"] "OK"
        assert_equal [$node_a crdt.op 2 1000 3 1 test_cnt "100"] "OK"
        assert_equal [$node_a crdt.op 2 1000 4 1 test_strm "1000-2-0" "f" "v"] "OK"

        assert_equal [$node_a save] "OK"

        assert_equal [$node_a get test_str] "str_val"
        assert_equal [$node_a scard test_set] 1
        assert_equal [$node_a get test_cnt] "100"
        assert_equal [$node_a xlen test_strm] 1
    }
}
