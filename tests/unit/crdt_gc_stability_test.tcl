# Active-Active CRDT List Engine Phase 5 Tests:
# 1. Dynamic Stability Horizon Tombstone Garbage Collection
# 2. Tombstone Safety (H_stable < delete_hlc)
# 3. Descendant Anchor Safety (Active descendants preserve parent tombstones)
# 4. Straggler Lease Eviction from GC Quorum
# 5. 3-Node Active-Active Replication Mesh Simulation & Convergence

start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "1" "active-active-gc-lease-ms" "3600000" "save" ""]] {
    set node_a [srv 0 client]
    set node_a_host [srv 0 host]
    set node_a_port [srv 0 port]

    test "CRDT GC & Stability Horizon: Configuration and INFO section" {
        assert_equal [lindex [$node_a config get active-active-gc-lease-ms] 1] "3600000"
        
        $node_a config set active-active-gc-lease-ms 5000
        assert_equal [lindex [$node_a config get active-active-gc-lease-ms] 1] "5000"
        $node_a config set active-active-gc-lease-ms 3600000

        # Verify INFO crdt section exists and has expected fields
        set crdt_info [$node_a info crdt]
        assert {[string match "*# CRDT*" $crdt_info]}
        assert {[string match "*crdt_active_active_enabled:1*" $crdt_info]}
        assert {[string match "*crdt_origin_id:1*" $crdt_info]}
        assert {[string match "*crdt_h_stable:*" $crdt_info]}
        assert {[string match "*crdt_gc_lease_ms:3600000*" $crdt_info]}
    }

    test "Tombstone safety: Tombstones are NOT pruned if H_stable < delete_hlc" {
        $node_a del crdt_safe_tomb
        # Insert V1 (1000:1, "first"), V2 (2000:1, "second"), V3 (3000:1, "third")
        assert_equal [$node_a crdt.linsert crdt_safe_tomb 0 0 1000 1 "first"] "OK"
        assert_equal [$node_a crdt.linsert crdt_safe_tomb 1000 1 2000 1 "second"] "OK"
        assert_equal [$node_a crdt.linsert crdt_safe_tomb 2000 1 3000 1 "third"] "OK"
        assert_equal [$node_a lrange crdt_safe_tomb 0 -1] {first second third}

        # Delete "second" (2000:1) with del_hlc=4000
        assert_equal [$node_a crdt.ldelete crdt_safe_tomb 2000 1 4000 1] "OK"
        assert_equal [$node_a lrange crdt_safe_tomb 0 -1] {first third}
        assert_equal [$node_a llen crdt_safe_tomb] 2

        # Explicitly run GC with h_stable = 3500 (< del_hlc 4000)
        assert_equal [$node_a crdt.gc crdt_safe_tomb 3500] 0

        # Tombstone V2 (2000:1) must still be present as an anchor
        # Peer inserts after V2 (2000:1): (2500:2, "after_tomb")
        assert_equal [$node_a crdt.linsert crdt_safe_tomb 2000 1 2500 2 "after_tomb"] "OK"
        assert_equal [$node_a lrange crdt_safe_tomb 0 -1] {first third after_tomb}
    }

    test "Tombstone pruning under Stability Horizon" {
        $node_a del crdt_prune_list
        # Setup: V1 (100:1, "item1"), V2 (200:1, "item2")
        assert_equal [$node_a crdt.linsert crdt_prune_list 0 0 100 1 "item1"] "OK"
        assert_equal [$node_a crdt.linsert crdt_prune_list 0 0 200 1 "item2"] "OK"
        assert_equal [$node_a lrange crdt_prune_list 0 -1] {item2 item1}

        # Delete both elements with del_hlc=300:1 and del_hlc=400:1
        assert_equal [$node_a crdt.ldelete crdt_prune_list 100 1 300 1] "OK"
        assert_equal [$node_a crdt.ldelete crdt_prune_list 200 1 400 1] "OK"
        assert_equal [$node_a llen crdt_prune_list] 0

        # Run GC with H_stable = 500 -> both tombstones pruned, key is completely deleted
        assert_equal [$node_a crdt.gc crdt_prune_list 500] 2
        assert_equal [$node_a exists crdt_prune_list] 0
    }

    test "Descendant anchor safety: Active descendants preserve parent tombstones" {
        $node_a del crdt_anchor_tree
        # Chain: Root -> Parent (1000:1, "parent") -> Child (2000:1, "child") -> Grandchild (3000:1, "grandchild")
        assert_equal [$node_a crdt.linsert crdt_anchor_tree 0 0 1000 1 "parent"] "OK"
        assert_equal [$node_a crdt.linsert crdt_anchor_tree 1000 1 2000 1 "child"] "OK"
        assert_equal [$node_a crdt.linsert crdt_anchor_tree 2000 1 3000 1 "grandchild"] "OK"
        assert_equal [$node_a lrange crdt_anchor_tree 0 -1] {parent child grandchild}

        # Delete Parent (1000:1) with del_hlc=5000 and Child (2000:1) with del_hlc=6000
        # Grandchild (3000:1) is still active (visible)!
        assert_equal [$node_a crdt.ldelete crdt_anchor_tree 1000 1 5000 1] "OK"
        assert_equal [$node_a crdt.ldelete crdt_anchor_tree 2000 1 6000 1] "OK"
        assert_equal [$node_a lrange crdt_anchor_tree 0 -1] {grandchild}
        assert_equal [$node_a llen crdt_anchor_tree] 1

        # Attempt to prune with H_stable = 10000:
        # Parent and Child MUST NOT be pruned because Grandchild is an active descendant!
        assert_equal [$node_a crdt.gc crdt_anchor_tree 10000] 0
        assert_equal [$node_a lrange crdt_anchor_tree 0 -1] {grandchild}

        # Now delete Grandchild with del_hlc=7000
        assert_equal [$node_a crdt.ldelete crdt_anchor_tree 3000 1 7000 1] "OK"
        assert_equal [$node_a llen crdt_anchor_tree] 0

        # Prune with H_stable = 6500 (< Grandchild del_hlc 7000) -> 0 pruned
        assert_equal [$node_a crdt.gc crdt_anchor_tree 6500] 0

        # Prune with H_stable = 8000 (>= all del_hlc) -> all 3 tombstones pruned!
        assert_equal [$node_a crdt.gc crdt_anchor_tree 8000] 3
        assert_equal [$node_a exists crdt_anchor_tree] 0
    }

    test "Dynamic Quorum Stability Horizon Calculation & Peer ACK Tracking" {
        # Reset stability peer tracking
        assert_equal [$node_a crdt.stability reset] "OK"

        # Register Peer 2 and Peer 3
        assert_equal [$node_a crdt.stability peer_add 2 1000] "OK"
        assert_equal [$node_a crdt.stability peer_add 3 2000] "OK"

        # H_stable is min(1000, 2000, local_hlc) = 1000
        set stab_info [$node_a crdt.stability info]
        set h_stable [$node_a crdt.stability get]
        assert_equal $h_stable 1000

        # Peer 2 acknowledges up to HLC 3000
        assert_equal [$node_a crdt.stability peer_ack 2 3000] "OK"
        assert_equal [$node_a crdt.stability get] 2000

        # Peer 3 acknowledges up to HLC 4000
        assert_equal [$node_a crdt.stability peer_ack 3 4000] "OK"
        assert {[$node_a crdt.stability get] >= 3000}
    }

    test "Straggler Lease Eviction from GC Quorum" {
        $node_a del crdt_lease_test
        $node_a crdt.stability reset
        
        # Set short GC lease of 300ms
        $node_a config set active-active-gc-lease-ms 300

        # Register Peer 2 (silent / lagging at HLC 100) and Peer 3 (active at HLC 5000)
        $node_a crdt.stability peer_add 2 100
        $node_a crdt.stability peer_add 3 5000

        # Initially H_stable is 100
        assert_equal [$node_a crdt.stability get] 100

        # Insert and delete an item at HLC 1000
        assert_equal [$node_a crdt.linsert crdt_lease_test 0 0 500 1 "lease_item"] "OK"
        assert_equal [$node_a crdt.ldelete crdt_lease_test 500 1 1000 1] "OK"

        # Wait for Peer 2's lease to expire (Peer 3 remains active via periodic heartbeat)
        after 200
        $node_a crdt.stability peer_heartbeat 3
        after 250
        $node_a crdt.stability peer_heartbeat 3

        # Peer 2 is evicted as straggler! H_stable unfreezes to min(5000, local_hlc) >= 1000
        set h_stable_unfrozen [$node_a crdt.stability get]
        assert {$h_stable_unfrozen >= 1000}

        # GC now successfully reclaims the tombstone
        assert_equal [$node_a crdt.gc crdt_lease_test] 1
        assert_equal [$node_a exists crdt_lease_test] 0

        # Verify straggler ejection metric
        set crdt_info [$node_a info crdt]
        assert {[string match "*crdt_straggler_ejections:1*" $crdt_info]}

        # Peer 2 sends new ACK and rejoins quorum automatically
        assert_equal [$node_a crdt.stability peer_ack 2 6000] "OK"
        set crdt_info [$node_a info crdt]
        assert {[string match "*crdt_ejected_peers:0*" $crdt_info]}
        assert {[string match "*crdt_active_peers:2*" $crdt_info]}

        # Restore default lease
        $node_a config set active-active-gc-lease-ms 3600000
    }

    test "3-Node Active-Active Replication Mesh Simulation" {
        start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "2" "save" ""]] {
            set node_b [srv 0 client]
            set node_b_host [srv 0 host]
            set node_b_port [srv 0 port]

            start_server [list overrides [list "active-active" "yes" "active-active-origin-id" "3" "save" ""]] {
                set node_c [srv 0 client]
                set node_c_host [srv 0 host]
                set node_c_port [srv 0 port]

                $node_a del mesh_list
                $node_b del mesh_list
                $node_c del mesh_list

                # -------------------------------------------------------------
                # Step 1: Concurrent writes across all 3 nodes
                # -------------------------------------------------------------
                # Node A inserts "A1" (100:1) and "A2" (200:1)
                assert_equal [$node_a crdt.linsert mesh_list 0 0 100 1 "A1"] "OK"
                assert_equal [$node_a crdt.linsert mesh_list 100 1 200 1 "A2"] "OK"

                # Node B inserts "B1" (150:2) and "B2" (250:2)
                assert_equal [$node_b crdt.linsert mesh_list 0 0 150 2 "B1"] "OK"
                assert_equal [$node_b crdt.linsert mesh_list 150 2 250 2 "B2"] "OK"

                # Node C inserts "C1" (180:3) and "C2" (280:3)
                assert_equal [$node_c crdt.linsert mesh_list 0 0 180 3 "C1"] "OK"
                assert_equal [$node_c crdt.linsert mesh_list 180 3 280 3 "C2"] "OK"

                # -------------------------------------------------------------
                # Step 2: Cross-propagate replication frames in mesh
                # -------------------------------------------------------------
                # Node A receives from B and C
                $node_a crdt.linsert mesh_list 0 0 150 2 "B1"
                $node_a crdt.linsert mesh_list 150 2 250 2 "B2"
                $node_a crdt.linsert mesh_list 0 0 180 3 "C1"
                $node_a crdt.linsert mesh_list 180 3 280 3 "C2"

                # Node B receives from A and C
                $node_b crdt.linsert mesh_list 0 0 100 1 "A1"
                $node_b crdt.linsert mesh_list 100 1 200 1 "A2"
                $node_b crdt.linsert mesh_list 0 0 180 3 "C1"
                $node_b crdt.linsert mesh_list 180 3 280 3 "C2"

                # Node C receives from A and B
                $node_c crdt.linsert mesh_list 0 0 100 1 "A1"
                $node_c crdt.linsert mesh_list 100 1 200 1 "A2"
                $node_c crdt.linsert mesh_list 0 0 150 2 "B1"
                $node_c crdt.linsert mesh_list 150 2 250 2 "B2"

                # Verify deterministic convergence across all 3 nodes!
                # RGA order at root:
                # Sibling priority: C1 (180:3) > B1 (150:2) > A1 (100:1)
                # Tree order: C1 -> C2 -> B1 -> B2 -> A1 -> A2
                set expected_list {C1 C2 B1 B2 A1 A2}
                assert_equal [$node_a lrange mesh_list 0 -1] $expected_list
                assert_equal [$node_b lrange mesh_list 0 -1] $expected_list
                assert_equal [$node_c lrange mesh_list 0 -1] $expected_list

                # -------------------------------------------------------------
                # Step 3: Concurrent deletions & Tombstone GC in mesh
                # -------------------------------------------------------------
                # Node A deletes "A1" (100:1) with del_hlc=500:1
                $node_a crdt.ldelete mesh_list 100 1 500 1
                $node_b crdt.ldelete mesh_list 100 1 500 1
                $node_c crdt.ldelete mesh_list 100 1 500 1

                # Node B deletes "B1" (150:2) with del_hlc=550:2
                $node_a crdt.ldelete mesh_list 150 2 550 2
                $node_b crdt.ldelete mesh_list 150 2 550 2
                $node_c crdt.ldelete mesh_list 150 2 550 2

                # Verify visible items on all 3 nodes (A1 and B1 are hidden tombstones; A2 and B2 are preserved!)
                set expected_after_del {C1 C2 B2 A2}
                assert_equal [$node_a lrange mesh_list 0 -1] $expected_after_del
                assert_equal [$node_b lrange mesh_list 0 -1] $expected_after_del
                assert_equal [$node_c lrange mesh_list 0 -1] $expected_after_del

                # Nodes exchange ACKs up to HLC 600
                $node_a crdt.stability peer_ack 2 600
                $node_a crdt.stability peer_ack 3 600

                $node_b crdt.stability peer_ack 1 600
                $node_b crdt.stability peer_ack 3 600

                $node_c crdt.stability peer_ack 1 600
                $node_c crdt.stability peer_ack 2 600

                # Note: A1 is parent of live A2, B1 is parent of live B2.
                # So A1 and B1 tombstones are preserved as anchors!
                assert_equal [$node_a crdt.gc mesh_list] 0
                assert_equal [$node_b crdt.gc mesh_list] 0
                assert_equal [$node_c crdt.gc mesh_list] 0

                # Now delete A2 and B2 at HLC 700
                $node_a crdt.ldelete mesh_list 200 1 700 1
                $node_b crdt.ldelete mesh_list 200 1 700 1
                $node_c crdt.ldelete mesh_list 200 1 700 1

                $node_a crdt.ldelete mesh_list 250 2 700 2
                $node_b crdt.ldelete mesh_list 250 2 700 2
                $node_c crdt.ldelete mesh_list 250 2 700 2

                # Exchange ACKs up to HLC 800
                $node_a crdt.stability peer_ack 2 800
                $node_a crdt.stability peer_ack 3 800
                $node_b crdt.stability peer_ack 1 800
                $node_b crdt.stability peer_ack 3 800
                $node_c crdt.stability peer_ack 1 800
                $node_c crdt.stability peer_ack 2 800

                # Trigger GC on all nodes -> all 4 tombstones (A1, A2, B1, B2) are pruned!
                assert_equal [$node_a crdt.gc mesh_list] 4
                assert_equal [$node_b crdt.gc mesh_list] 4
                assert_equal [$node_c crdt.gc mesh_list] 4

                # Remaining live items C1, C2 remain intact and identical on all nodes
                assert_equal [$node_a lrange mesh_list 0 -1] {C1 C2}
                assert_equal [$node_b lrange mesh_list 0 -1] {C1 C2}
                assert_equal [$node_c lrange mesh_list 0 -1] {C1 C2}
                assert_equal [$node_a llen mesh_list] 2
                assert_equal [$node_b llen mesh_list] 2
                assert_equal [$node_c llen mesh_list] 2
            }
        }
    }
}
