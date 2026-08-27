# Active-Active CRDT List Engine: Partition & Buffer Overrun Verification Test
# 1. Network partition between two primaries with massive concurrent conflicting writes
# 2. Backlog wrap-around & buffer overrun
# 3. Post-partition non-destructive state synchronization (FULLRESYNC_MERGE)
# 4. Bit-for-bit final state convergence

start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "1" "active-active-peer-buffer-limit" "16384" "repl-backlog-size" "16384" "replica-read-only" "no" "save" ""]] {
    set node_a [srv 0 client]
    set node_a_host [srv 0 host]
    set node_a_port [srv 0 port]
    set node_a_pid [srv 0 pid]

    start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "2" "active-active-peer-buffer-limit" "16384" "repl-backlog-size" "16384" "replica-read-only" "no" "save" ""]] {
        set node_b [srv 0 client]
        set node_b_host [srv 0 host]
        set node_b_port [srv 0 port]
        set node_b_pid [srv 0 pid]

        test "Test 1: Network Partition with Heavy Conflicting Writes & Backlog Overrun" {
            # Step 1: Initial shared baseline state created on Node A and replicated to Node B
            $node_a del shared_list
            $node_a rpush shared_list "BASE_1" "BASE_2" "BASE_3" "BASE_4" "BASE_5"

            # Sync baseline to Node B
            $node_b replicaof $node_a_host $node_a_port
            wait_for_condition 50 100 {
                [string match {*master_link_status:up*} [$node_b info replication]]
            } else {
                fail "Node B failed to sync initial baseline"
            }
            $node_b replicaof no one

            assert_equal [$node_a lrange shared_list 0 -1] {BASE_1 BASE_2 BASE_3 BASE_4 BASE_5}
            assert_equal [$node_b lrange shared_list 0 -1] {BASE_1 BASE_2 BASE_3 BASE_4 BASE_5}

            # Step 2: Simulate partition - Node A and Node B receive massive conflicting writes
            # Write volume is > 100KB, exceeding 16KB backlog by >6x

            # Node A mutations (Origin 1)
            for {set i 1} {$i <= 100} {incr i} {
                $node_a lpush shared_list [format "A_HEAD_%03d_%s" $i [string repeat "A" 100]]
                $node_a rpush shared_list [format "A_TAIL_%03d_%s" $i [string repeat "A" 100]]
            }
            # Node A pops and modifications
            for {set i 0} {$i < 20} {incr i} {
                $node_a lpop shared_list
            }
            $node_a linsert shared_list AFTER "BASE_3" "A_AFTER_BASE3"
            $node_a lrem shared_list 1 "BASE_4"

            # Node B mutations (Origin 2) concurrently
            for {set i 1} {$i <= 100} {incr i} {
                $node_b lpush shared_list [format "B_HEAD_%03d_%s" $i [string repeat "B" 100]]
                $node_b rpush shared_list [format "B_TAIL_%03d_%s" $i [string repeat "B" 100]]
            }
            # Node B pops and modifications
            for {set i 0} {$i < 20} {incr i} {
                $node_b rpop shared_list
            }
            $node_b linsert shared_list BEFORE "BASE_3" "B_BEFORE_BASE3"
            $node_b lrem shared_list 1 "BASE_2"

            # Verify local lengths during partition
            set len_a [$node_a llen shared_list]
            set len_b [$node_b llen shared_list]
            assert {$len_a > 150}
            assert {$len_b > 150}

            # Step 3: Resolve partition -> Node B triggers FULLRESYNC_MERGE from Node A
            $node_b replicaof $node_a_host $node_a_port
            wait_for_condition 100 100 {
                [string match {*master_link_status:up*} [$node_b info replication]]
            } else {
                fail "Node B failed to synchronize with Node A during partition recovery"
            }
            $node_b replicaof no one

            # Node B now holds the full unified merged state (S_A ⊔ S_B)
            set merged_len [$node_b llen shared_list]
            assert {$merged_len > 300}

            # Sync the unified merged state from Node B to Node A
            set merged_dump [$node_b dump shared_list]
            $node_a del shared_list
            $node_a restore shared_list 0 $merged_dump

            # Step 4: Verification of Final Convergence
            set final_a [$node_a lrange shared_list 0 -1]
            set final_b [$node_b lrange shared_list 0 -1]

            # 1. Both nodes must agree on the EXACT SAME list length
            assert_equal [$node_a llen shared_list] [$node_b llen shared_list]

            # 2. Both nodes must agree on the EXACT SAME list content and element ordering (SEC)
            assert_equal $final_a $final_b

            # 3. Verify specific conflict resolutions:
            # - BASE_3 is present
            assert {[lsearch -exact $final_a "BASE_3"] != -1}
            # - A_AFTER_BASE3 and B_BEFORE_BASE3 are both present
            assert {[lsearch -exact $final_a "A_AFTER_BASE3"] != -1}
            assert {[lsearch -exact $final_a "B_BEFORE_BASE3"] != -1}
            # - BASE_4 (deleted by A) is gone
            assert_equal [lsearch -exact $final_a "BASE_4"] -1
            # - BASE_2 (deleted by B) is gone
            assert_equal [lsearch -exact $final_a "BASE_2"] -1
        }

        test "Test 2: Slow Replica Buffer Overrun Disconnect and Re-convergence" {
            $node_a del buf_test_list
            $node_b del buf_test_list
            $node_a rpush buf_test_list "INIT_1" "INIT_2"

            # Connect Node B as replica
            $node_b replicaof $node_a_host $node_a_port
            wait_for_condition 50 100 {
                [string match {*master_link_status:up*} [$node_b info replication]]
            } else {
                fail "Node B failed to connect"
            }

            # Freeze Node B process (simulating severe network stall / WAN freeze)
            exec kill -STOP $node_b_pid

            # Node A receives large burst of writes exceeding active-active-peer-buffer-limit (16KB)
            set large_payload [string repeat "W" 8192]
            for {set i 1} {$i <= 10} {incr i} {
                $node_a rpush buf_test_list [format "BURST_A_%d_%s" $i $large_payload]
            }

            # Node A should detect buffer overrun and disconnect the frozen peer replica
            wait_for_condition 50 100 {
                [string match {*connected_slaves:0*} [$node_a info replication]]
            } else {
                exec kill -CONT $node_b_pid
                fail "Node A did not disconnect peer replica after buffer overrun"
            }

            # Unfreeze Node B
            exec kill -CONT $node_b_pid
            $node_b replicaof no one

            # Node B also accepts concurrent local writes during the disconnect
            for {set i 1} {$i <= 5} {incr i} {
                $node_b lpush buf_test_list [format "CONCURRENT_B_%d_%s" $i $large_payload]
            }

            # Re-establish replication -> triggers non-destructive FULLRESYNC_MERGE
            $node_b replicaof $node_a_host $node_a_port
            wait_for_condition 50 100 {
                [string match {*master_link_status:up*} [$node_b info replication]]
            } else {
                fail "Node B failed to reconnect"
            }
            $node_b replicaof no one

            # Replicate unified state to Node A
            set merged_buf_dump [$node_b dump buf_test_list]
            $node_a del buf_test_list
            $node_a restore buf_test_list 0 $merged_buf_dump

            # Verify identical convergence
            set list_a [$node_a lrange buf_test_list 0 -1]
            set list_b [$node_b lrange buf_test_list 0 -1]

            assert_equal [$node_a llen buf_test_list] [$node_b llen buf_test_list]
            assert_equal $list_a $list_b

            # Verify that both Node A's burst writes and Node B's concurrent writes exist
            assert {[lsearch -glob $list_a "BURST_A_1_*"] != -1}
            assert {[lsearch -glob $list_a "BURST_A_10_*"] != -1}
            assert {[lsearch -glob $list_a "CONCURRENT_B_1_*"] != -1}
            assert {[lsearch -glob $list_a "CONCURRENT_B_5_*"] != -1}
        }
    }
}
