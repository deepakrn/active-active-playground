# Active-Active CRDT List Engine Phase 3 & 4 Tests:
# 1. RDB Serialization / Deserialization & Persistence with Tombstones
# 2. Non-Destructive State Sync (FULLRESYNC_MERGE)
# 3. Bounded Peer Replication Buffer Limits

start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "1" "save" ""]] {
    set node_a [srv 0 client]
    set node_a_host [srv 0 host]
    set node_a_port [srv 0 port]

    test "CRDT List RDB DUMP and RESTORE basic persistence" {
        $node_a del crdt_dump_list
        $node_a rpush crdt_dump_list "item1" "item2" "item3"
        assert_equal [$node_a object encoding crdt_dump_list] "crdt_list"
        assert_equal [$node_a lrange crdt_dump_list 0 -1] {item1 item2 item3}

        # Dump serialized RDB payload
        set dump_payload [$node_a dump crdt_dump_list]
        assert {[string length $dump_payload] > 0}

        # Delete key and restore
        $node_a del crdt_dump_list
        assert_equal [$node_a exists crdt_dump_list] 0

        $node_a restore crdt_dump_list 0 $dump_payload
        assert_equal [$node_a object encoding crdt_dump_list] "crdt_list"
        assert_equal [$node_a llen crdt_dump_list] 3
        assert_equal [$node_a lrange crdt_dump_list 0 -1] {item1 item2 item3}
    }

    test "CRDT List RDB DUMP and RESTORE with Tombstones and Post-Restore Mutation" {
        $node_a del crdt_tomb_list
        # Setup: V0 (root) -> V1 (1000:1, "first") -> V2 (2000:1, "second") -> V3 (3000:1, "third")
        assert_equal [$node_a crdt.linsert crdt_tomb_list 0 0 1000 1 "first"] "OK"
        assert_equal [$node_a crdt.linsert crdt_tomb_list 1000 1 2000 1 "second"] "OK"
        assert_equal [$node_a crdt.linsert crdt_tomb_list 2000 1 3000 1 "third"] "OK"
        assert_equal [$node_a lrange crdt_tomb_list 0 -1] {first second third}

        # Delete "second" (2000:1) with tombstone timestamp 4000:1
        assert_equal [$node_a crdt.ldelete crdt_tomb_list 2000 1 4000 1] "OK"
        assert_equal [$node_a lrange crdt_tomb_list 0 -1] {first third}
        assert_equal [$node_a llen crdt_tomb_list] 2

        # Dump with tombstone
        set dump_tomb [$node_a dump crdt_tomb_list]
        $node_a del crdt_tomb_list

        # Restore
        $node_a restore crdt_tomb_list 0 $dump_tomb
        assert_equal [$node_a object encoding crdt_tomb_list] "crdt_list"
        assert_equal [$node_a llen crdt_tomb_list] 2
        assert_equal [$node_a lrange crdt_tomb_list 0 -1] {first third}

        # Insert after the tombstone vertex (2000:1): new vertex (2500:1, "after_tomb")
        # In RGA sibling ordering, V3 (3000:1) has higher priority than 2500:1, so V3 comes first
        assert_equal [$node_a crdt.linsert crdt_tomb_list 2000 1 2500 1 "after_tomb"] "OK"
        assert_equal [$node_a lrange crdt_tomb_list 0 -1] {first third after_tomb}
        assert_equal [$node_a llen crdt_tomb_list] 3

        # Insert higher-priority sibling (3500:1) after tombstone V2 (2000:1) -> 3500 > 3000 -> comes before "third"
        assert_equal [$node_a crdt.linsert crdt_tomb_list 2000 1 3500 1 "before_third"] "OK"
        assert_equal [$node_a lrange crdt_tomb_list 0 -1] {first before_third third after_tomb}
        assert_equal [$node_a llen crdt_tomb_list] 4
    }

    test "CRDT List SAVE and Server Restart Persistence" {
        $node_a del crdt_persisted
        $node_a rpush crdt_persisted "alpha" "beta" "gamma" "delta"
        $node_a lset crdt_persisted 1 "BETA_MOD"
        assert_equal [$node_a lpop crdt_persisted] "alpha"
        assert_equal [$node_a lrange crdt_persisted 0 -1] {BETA_MOD gamma delta}

        # Perform synchronous SAVE to disk
        assert_equal [$node_a save] "OK"

        # Restart server with data preserved
        restart_server 0 true false
        set node_a [srv 0 client]

        assert_equal [$node_a object encoding crdt_persisted] "crdt_list"
        assert_equal [$node_a llen crdt_persisted] 3
        assert_equal [$node_a lrange crdt_persisted 0 -1] {BETA_MOD gamma delta}

        # Verify continuing writes after restart
        $node_a rpush crdt_persisted "epsilon"
        assert_equal [$node_a lrange crdt_persisted 0 -1] {BETA_MOD gamma delta epsilon}
    }

    test "Non-Destructive Merge (FULLRESYNC_MERGE) via Replication" {
        # Start Node B (active-active yes, origin 2)
        start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "2" "save" ""]] {
            set node_b [srv 0 client]
            set node_b_host [srv 0 host]
            set node_b_port [srv 0 port]

            # Setup initial state on Node A: {sync_k: ["A1", "A2"]} using explicit CRDT inserts
            $node_a del sync_k
            assert_equal [$node_a crdt.linsert sync_k 0 0 100 1 "A1"] "OK"
            assert_equal [$node_a crdt.linsert sync_k 100 1 200 1 "A2"] "OK"
            assert_equal [$node_a lrange sync_k 0 -1] {A1 A2}

            # Setup distinct initial state on Node B: {sync_k: ["B1", "B2"]}
            $node_b del sync_k
            assert_equal [$node_b crdt.linsert sync_k 0 0 150 2 "B1"] "OK"
            assert_equal [$node_b crdt.linsert sync_k 150 2 250 2 "B2"] "OK"
            assert_equal [$node_b lrange sync_k 0 -1] {B1 B2}

            # Node B also has an independent key: unique_b
            $node_b del unique_b
            $node_b rpush unique_b "only_on_b"

            # Connect Node B as replica of Node A (triggers full resync snapshot transfer)
            $node_b replicaof $node_a_host $node_a_port
            wait_for_condition 50 100 {
                [string match {*master_link_status:up*} [$node_b info replication]]
            } else {
                fail "Node B failed to synchronize with Node A"
            }

            # Verify Node B non-destructively merged Node A's data without flushing:
            # 1. unique_b must still exist!
            assert_equal [$node_b exists unique_b] 1
            assert_equal [$node_b lrange unique_b 0 -1] {only_on_b}

            # 2. sync_k must contain all 4 elements merged in deterministic RGA order!
            # RGA Sibling ordering at root:
            # V(B1, 150:2) vs V(A1, 100:1) -> 150:2 > 100:1 -> B1 is before A1
            # V(B2, 250:2) is child of B1.
            # V(A2, 200:1) is child of A1.
            # Total order: B1 -> B2 -> A1 -> A2
            assert_equal [$node_b llen sync_k] 4
            assert_equal [$node_b lrange sync_k 0 -1] {B1 B2 A1 A2}

            # Disconnect replication
            $node_b replicaof no one
        }
    }

    test "Non-Destructive Merge with Concurrent Tombstones" {
        start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "2" "save" ""]] {
            set node_b [srv 0 client]
            set node_b_host [srv 0 host]
            set node_b_port [srv 0 port]

            # Common base on Node A and Node B: V1(100:1, "X"), V2(200:1, "Y"), V3(300:1, "Z")
            $node_a del merge_tomb
            $node_b del merge_tomb
            $node_a crdt.linsert merge_tomb 0 0 100 1 "X"
            $node_a crdt.linsert merge_tomb 100 1 200 1 "Y"
            $node_a crdt.linsert merge_tomb 200 1 300 1 "Z"

            $node_b crdt.linsert merge_tomb 0 0 100 1 "X"
            $node_b crdt.linsert merge_tomb 100 1 200 1 "Y"
            $node_b crdt.linsert merge_tomb 200 1 300 1 "Z"

            # Node A deletes "Y" (200:1)
            $node_a crdt.ldelete merge_tomb 200 1 400 1
            assert_equal [$node_a lrange merge_tomb 0 -1] {X Z}

            # Node B inserts "Y_child" (250:2) concurrently after "Y"
            $node_b crdt.linsert merge_tomb 200 1 250 2 "Y_child"
            assert_equal [$node_b lrange merge_tomb 0 -1] {X Y Z Y_child}

            # Synchronize Node A snapshot into Node B
            $node_b replicaof $node_a_host $node_a_port
            wait_for_condition 50 100 {
                [string match {*master_link_status:up*} [$node_b info replication]]
            } else {
                fail "Node B failed to sync"
            }

            # Node B should have merged the tombstone for "Y", while keeping "Y_child"
            assert_equal [$node_b lrange merge_tomb 0 -1] {X Z Y_child}
            assert_equal [$node_b llen merge_tomb] 3

            $node_b replicaof no one
        }
    }

    test "Bounded Peer Replication Buffer Limit enforcement" {
        start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "3" "active-active-peer-buffer-limit" "131072" "repl-backlog-size" "131072" "client-output-buffer-limit" "replica 0 0 0"]] {
            set srv_c [srv 0 client]
            set srv_c_host [srv 0 host]
            set srv_c_port [srv 0 port]

            assert_equal [lindex [$srv_c config get active-active-peer-buffer-limit] 1] "131072"

            start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "4"]] {
                set srv_d [srv 0 client]
                set srv_d_pid [srv 0 pid]

                $srv_d replicaof $srv_c_host $srv_c_port
                wait_for_condition 50 100 {
                    [string match {*master_link_status:up*} [$srv_d info replication]]
                } else {
                    fail "Replica failed to connect"
                }

                # Freeze replica process with SIGSTOP so it cannot drain socket buffers
                exec kill -STOP $srv_d_pid

                # Flood primary with writes exceeding kernel socket buffer and peer buffer limit
                set large_val [string repeat "Z" 32768]
                for {set i 0} {$i < 300} {incr i} {
                    $srv_c rpush big_buf_key $large_val
                }

                # Primary should detect limit overrun and disconnect the frozen peer replica
                wait_for_condition 50 100 {
                    [string match {*connected_slaves:0*} [$srv_c info replication]]
                } else {
                    exec kill -CONT $srv_d_pid
                    fail "Peer replica was not disconnected after exceeding buffer limit"
                }

                # Unfreeze replica for clean shutdown
                exec kill -CONT $srv_d_pid
            }
        }
    }
}
