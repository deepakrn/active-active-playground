# Active-Active CRDT List Engine Interception & Replication Tests

start_server [list overrides [list save "" "active-active" "yes" "active-active-origin-id" "1"] ] {
    test "Active-Active configuration validation" {
        assert_equal [lindex [r config get active-active] 1] "yes"
        assert_equal [lindex [r config get active-active-origin-id] 1] "1"
        assert_equal [lindex [r config get active-active-peer-buffer-limit] 1] "67108864"
        
        r config set active-active-origin-id 42
        assert_equal [lindex [r config get active-active-origin-id] 1] "42"
        r config set active-active-origin-id 1
    }

    test "CRDT List creation and OBJECT ENCODING" {
        r del crdt_l1
        r rpush crdt_l1 a b c
        assert_equal [r object encoding crdt_l1] "crdt_list"
        assert_equal [r llen crdt_l1] 3
        assert_equal [r lrange crdt_l1 0 -1] {a b c}
    }

    test "CRDT LPUSH and RPUSH operations" {
        r del crdt_l2
        r lpush crdt_l2 1 2 3
        assert_equal [r lrange crdt_l2 0 -1] {3 2 1}
        r rpush crdt_l2 4 5 6
        assert_equal [r lrange crdt_l2 0 -1] {3 2 1 4 5 6}
        assert_equal [r llen crdt_l2] 6
    }

    test "CRDT LPUSHX and RPUSHX" {
        r del nonexist_crdt
        assert_equal [r lpushx nonexist_crdt x] 0
        assert_equal [r rpushx nonexist_crdt x] 0
        r rpush nonexist_crdt base
        assert_equal [r lpushx nonexist_crdt head] 2
        assert_equal [r rpushx nonexist_crdt tail] 3
        assert_equal [r lrange nonexist_crdt 0 -1] {head base tail}
    }

    test "CRDT LINDEX and LPOS" {
        r del crdt_idx
        r rpush crdt_idx apple banana cherry date banana elderberry
        assert_equal [r lindex crdt_idx 0] "apple"
        assert_equal [r lindex crdt_idx 2] "cherry"
        assert_equal [r lindex crdt_idx -1] "elderberry"
        assert_equal [r lindex crdt_idx -2] "banana"
        assert_equal [r lpos crdt_idx banana] 1
        assert_equal [r lpos crdt_idx banana rank 2] 4
        assert_equal [r lpos crdt_idx banana count 2] {1 4}
    }

    test "CRDT LPOP and RPOP single and multi-count" {
        r del crdt_pop
        r rpush crdt_pop 10 20 30 40 50 60
        assert_equal [r lpop crdt_pop] "10"
        assert_equal [r rpop crdt_pop] "60"
        assert_equal [r lrange crdt_pop 0 -1] {20 30 40 50}
        assert_equal [r lpop crdt_pop 2] {20 30}
        assert_equal [r rpop crdt_pop 2] {50 40}
        assert_equal [r llen crdt_pop] 0
    }

    test "CRDT LINSERT before and after" {
        r del crdt_ins
        r rpush crdt_ins A B D E
        r linsert crdt_ins before D C
        assert_equal [r lrange crdt_ins 0 -1] {A B C D E}
        r linsert crdt_ins after E F
        assert_equal [r lrange crdt_ins 0 -1] {A B C D E F}
    }

    test "CRDT LSET and LREM" {
        r del crdt_mod
        r rpush crdt_mod a b x c x d x e
        r lset crdt_mod 1 B
        assert_equal [r lindex crdt_mod 1] "B"
        r lrem crdt_mod 2 x
        assert_equal [r lrange crdt_mod 0 -1] {a B c d x e}
        r lrem crdt_mod -1 x
        assert_equal [r lrange crdt_mod 0 -1] {a B c d e}
    }

    test "CRDT LTRIM" {
        r del crdt_t
        r rpush crdt_t 0 1 2 3 4 5 6 7 8 9
        r ltrim crdt_t 2 6
        assert_equal [r lrange crdt_t 0 -1] {2 3 4 5 6}
        assert_equal [r llen crdt_t] 5
    }

    test "CRDT Replication: CRDT.LINSERT and CRDT.LDELETE commands" {
        r del crdt_rep
        # Peer 2 sends CRDT.LINSERT parent_hlc=0, parent_node=0, new_hlc=1000, new_node=2, value="first"
        assert_equal [r crdt.linsert crdt_rep 0 0 1000 2 "first"] "OK"
        # Peer 2 sends CRDT.LINSERT after "first" (1000, 2): new_hlc=2000, new_node=2, value="second"
        assert_equal [r crdt.linsert crdt_rep 1000 2 2000 2 "second"] "OK"
        # Peer 3 sends CRDT.LINSERT after "first" (1000, 2): new_hlc=3000, new_node=3, value="third"
        # Note: 3000:3 has higher timestamp than 2000:2, so RGA puts "third" BEFORE "second"
        assert_equal [r crdt.linsert crdt_rep 1000 2 3000 3 "third"] "OK"
        
        assert_equal [r lrange crdt_rep 0 -1] {first third second}
        
        # Peer 2 deletes "third" (target: 3000:3, del_hlc: 4000, del_node: 2)
        assert_equal [r crdt.ldelete crdt_rep 3000 3 4000 2] "OK"
        assert_equal [r lrange crdt_rep 0 -1] {first second}
        
        # Deleting nonexistent or already-deleted vertex is idempotent
        assert_equal [r crdt.ldelete crdt_rep 3000 3 4000 2] "OK"
        assert_equal [r crdt.ldelete crdt_rep 9999 9 5000 1] "OK"
        assert_equal [r lrange crdt_rep 0 -1] {first second}
    }

    test "CRDT Replication: Concurrent Insert & Delete Convergence" {
        r del crdt_conv
        # Root -> V1 (100:1, "A") -> V2 (200:1, "B")
        assert_equal [r crdt.linsert crdt_conv 0 0 100 1 "A"] "OK"
        assert_equal [r crdt.linsert crdt_conv 100 1 200 1 "B"] "OK"
        assert_equal [r lrange crdt_conv 0 -1] {A B}

        # Peer 2 deletes V1 (100:1) with del_hlc=300:2
        assert_equal [r crdt.ldelete crdt_conv 100 1 300 2] "OK"
        assert_equal [r lrange crdt_conv 0 -1] {B}

        # Peer 3 inserts V3 (350:3, "A_child") after V1 (100:1)
        # Even though V1 is deleted, it acts as a tombstone anchor for V3
        assert_equal [r crdt.linsert crdt_conv 100 1 350 3 "A_child"] "OK"
        assert_equal [r lrange crdt_conv 0 -1] {A_child B}
    }

    test "CRDT Replication: Multi-node RGA Total Ordering Tie-breaking" {
        r del crdt_tie
        # Base vertex V0 (1000:1, "Root")
        assert_equal [r crdt.linsert crdt_tie 0 0 1000 1 "Root"] "OK"

        # Peer 2 and Peer 3 concurrently insert after V0 at the EXACT SAME HLC timestamp 2000
        # Peer 2: (2000:2, "FromPeer2")
        # Peer 3: (2000:3, "FromPeer3")
        # Origin ID 3 > Origin ID 2 -> Peer 3 has higher total order ID -> Peer 3 vertex appears before Peer 2
        assert_equal [r crdt.linsert crdt_tie 1000 1 2000 2 "FromPeer2"] "OK"
        assert_equal [r crdt.linsert crdt_tie 1000 1 2000 3 "FromPeer3"] "OK"

        assert_equal [r lrange crdt_tie 0 -1] {Root FromPeer3 FromPeer2}
    }

    test "CRDT Operations: LMOVE and RPOPLPUSH between CRDT lists" {
        r del crdt_src crdt_dst
        r rpush crdt_src 1 2 3
        r rpush crdt_dst a b
        assert_equal [r lmove crdt_src crdt_dst right left] "3"
        assert_equal [r lrange crdt_src 0 -1] {1 2}
        assert_equal [r lrange crdt_dst 0 -1] {3 a b}
        assert_equal [r rpoplpush crdt_src crdt_dst] "2"
        assert_equal [r lrange crdt_src 0 -1] {1}
        assert_equal [r lrange crdt_dst 0 -1] {2 3 a b}
    }

    test "CRDT Replication: Dynamic Conversion from Quicklist/Listpack" {
        # Disable active-active to create standard list
        r config set active-active no
        r del conv_list
        r rpush conv_list item1 item2 item3
        assert_equal [r object encoding conv_list] "listpack"
        
        # Enable active-active: next operation automatically converts to crdt_list
        r config set active-active yes
        r rpush conv_list item4
        assert_equal [r object encoding conv_list] "crdt_list"
        assert_equal [r lrange conv_list 0 -1] {item1 item2 item3 item4}
        assert_equal [r llen conv_list] 4
    }

    test "CRDT Stress: 1000 push and pop mutations" {
        r del crdt_stress
        for {set i 0} {$i < 500} {incr i} {
            r rpush crdt_stress "item_$i"
        }
        for {set i 0} {$i < 500} {incr i} {
            r lpush crdt_stress "head_$i"
        }
        assert_equal [r llen crdt_stress] 1000
        assert_equal [r lindex crdt_stress 0] "head_499"
        assert_equal [r lindex crdt_stress -1] "item_499"

        # Pop 250 from head and 250 from tail
        r lpop crdt_stress 250
        r rpop crdt_stress 250
        assert_equal [r llen crdt_stress] 500
    }
}
