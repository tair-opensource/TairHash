set testmodule [file normalize your_path/tairhash_module.so]

# Entries of an EXHSYNCGET reply as sorted {field value version} triples, so replies
# compare independently of hash iteration order.
proc sync_entries {reply} {
    set out {}
    foreach {f v ver} [lindex $reply 1] {
        lappend out [list $f $v $ver]
    }
    return [lsort -index 0 $out]
}

proc synced {client key} {
    lindex [$client exhsyncget $key] 0
}

# Major and minor version of the server under test (Valkey reports a compatible redis_version).
proc tairhash_server_at_least {major minor} {
    lassign [lrange [split [s redis_version] .] 0 1] ma mi
    expr {$ma > $major || ($ma == $major && $mi >= $minor)}
}

# DUMP of a hash written by TairHash 2.0 (encoding version 0, on a server with RDB version 9):
# the fields plain (a, version 1), versioned (b, version 7) and expiring (c, version 3, with
# a TTL that ends in 2100).
proc tairhash_v0_payload {} {
    binary format H* 0781b5a8ab85ab21f800020305016b05086578706972696e6702030281000003bb2cc3d800050163050976657273696f6e6564020702000501620505706c61696e020102000501610009000f05136cec7272da
}

# A DUMP payload of `body`, the payload without its CRC64, with the CRC64 recomputed.
proc tairhash_with_crc64 {body} {
    set crc 0
    binary scan $body cu* bytes
    foreach byte $bytes {
        set crc [expr {$crc ^ $byte}]
        for {set bit 0} {$bit < 8} {incr bit} {
            set crc [expr {($crc >> 1) ^ (($crc & 1) ? 0x95AC9329AC4BC9B5 : 0)}]
        }
    }
    return $body[binary format w $crc]
}

# The file the server appends its AOF to.
proc tairhash_aof_path {} {
    set dir [lindex [r config get dir] 1]
    set files [lsort -dictionary [glob -nocomplain [file join $dir appendonlydir *.incr.aof]]]
    if {[llength $files] == 0} {
        # Before Redis 7.0 the AOF is a single file. Redis 5.0 does not report its name, which is
        # then the default.
        set name [lindex [r config get appendfilename] 1]
        if {$name eq {}} {
            set name appendonly.aof
        }
        return [file join $dir $name]
    }
    return [lindex $files end]
}

# Appends a command to the AOF and has the server replay it, which is how a test gives a
# server a command it accepts only from the replication stream or while loading.
proc tairhash_replay_from_aof {args} {
    r config set appendonly yes
    waitForBgrewriteaof r
    set fd [open [tairhash_aof_path] a]
    fconfigure $fd -translation binary
    puts -nonewline $fd "*2\r\n\$6\r\nSELECT\r\n\$1\r\n9\r\n*[llength $args]\r\n"
    foreach arg $args {
        puts -nonewline $fd "\$[string length $arg]\r\n$arg\r\n"
    }
    close $fd
    r debug loadaof
    r config set appendonly no
}

# Whether the expire timer is done with the hashes of the test that uses it: read without
# expiring anything, which a read of the fields would do.
proc tairhash_timer_done {} {
    for {set i 0} {$i < 10} {incr i} {
        if {[r pttl tomb$i] <= 0 || [r exhlen tomb$i] != 0 || [r exhlen plain$i] != 1} {
            return 0
        }
    }
    return 1
}

proc tairhash_attach_replica {replica host port} {
    $replica replicaof $host $port
    wait_for_condition 50 100 {
        [lindex [$replica role] 0] eq {slave} &&
        [string match {*master_link_status:up*} [$replica info replication]]
    } else {
        fail "Can't turn the instance into a replica"
    }
}

# Waits until the replica is linked to the master and has the same data and sync state.
proc tairhash_wait_converged {master replica} {
    wait_for_condition 50 100 {
        [string match {*master_link_status:up*} [$replica info replication]] &&
        [$master debug digest] eq [$replica debug digest]
    } else {
        fail "The replica did not converge on its master"
    }
}

start_server {tags {"tairhash lww"} overrides {bind 0.0.0.0}} {
    r module load $testmodule

    test {EXHMSETLWW applies higher versions and reports losing writes} {
        r del k
        assert_equal {1 1} [r exhmsetlww k f1 a 10 f2 b 10]
        assert_equal {0 1} [r exhmsetlww k f1 old 9 f2 new 11]
        assert_equal {a 10} [r exhgetwithver k f1]
        assert_equal {new 11} [r exhgetwithver k f2]
    }

    test {EXHMSETLWW breaks version ties by the byte-wise greater value} {
        r del k
        r exhmsetlww k f b 5
        assert_equal {0} [r exhmsetlww k f a 5]
        assert_equal {0} [r exhmsetlww k f b 5]
        assert_equal {1} [r exhmsetlww k f c 5]
        assert_equal {1} [r exhmsetlww k f cc 5]
        assert_equal {cc 5} [r exhgetwithver k f]

        # The tie-break makes the outcome independent of arrival order.
        r del k
        r exhmsetlww k f cc 5
        r exhmsetlww k f c 5
        r exhmsetlww k f a 5
        assert_equal {cc 5} [r exhgetwithver k f]
    }

    test {EXHMSETLWW compares binary values as unsigned bytes} {
        r del k
        r exhmsetlww k f "\x7f" 1
        assert_equal {1} [r exhmsetlww k f "\x80" 1]
        assert_equal {0} [r exhmsetlww k f "\x00\xff" 1]
    }

    test {EXHMSETLWW applies repeated fields in argument order} {
        r del k
        assert_equal {1 1 0} [r exhmsetlww k f a 1 f b 2 f c 1]
        assert_equal {b 2} [r exhgetwithver k f]
    }

    test {EXHMSETLWW clears the TTL of a field it overwrites and keeps it on a losing write} {
        r del k
        r exhset k f v ex 100
        assert_equal {1} [r exhmsetlww k f v2 1000000]
        assert_equal -1 [r exhttl k f]

        r exhset k g v ex 100 abs 50
        assert_equal {0} [r exhmsetlww k g w 10]
        assert {[r exhttl k g] > 0}
        assert_equal v [r exhget k g]
    }

    test {EXHMSETLWW rejects malformed input before writing anything} {
        r del k
        assert_error {*wrong number of arguments*} {r exhmsetlww k f v}
        assert_error {*wrong number of arguments*} {r exhmsetlww k f v 1 g}
        assert_error {*syntax*} {r exhmsetlww k f v 1 g w 0}
        assert_error {*syntax*} {r exhmsetlww k f v 1 g w -3}
        assert_error {*syntax*} {r exhmsetlww k f v 1 g w nope}
        assert_equal 0 [r exists k]

        r set str x
        assert_error {*WRONGTYPE*} {r exhmsetlww str f v 1}
    }

    test {EXHDELLWW deletes at the same or a higher version and keeps newer writes} {
        r del k
        r exhmsetlww k f1 a 10 f2 b 10 f3 c 10
        assert_equal {1 1 0} [r exhdellww k f1 10 f2 11 f3 9]
        assert_equal {f3} [r exhkeys k]
        assert_equal {c 10} [r exhgetwithver k f3]
        # Deleting an already deleted field at the same or a lower version is in effect.
        assert_equal {1 1} [r exhdellww k f1 10 f2 3]
    }

    test {EXHDELLWW leaves a tombstone that writes up to its version lose to} {
        r del k
        r exhmsetlww k f a 10 keep x 1
        assert_equal {1} [r exhdellww k f 12]
        assert_equal {0 0} [r exhmsetlww k f late 11 f tie 12]
        assert_equal 0 [r exhexists k f]
        assert_equal {1} [r exhmsetlww k f new 13]
        assert_equal {new 13} [r exhgetwithver k f]
        # The landed write dropped the tombstone: once the field is gone without one, any
        # version lands again.
        r exhdel k f
        assert_equal {1} [r exhmsetlww k f older 5]
    }

    test {A write by another command over a tombstone continues from the delete version} {
        foreach cmd {
            {exhset k f v}
            {exhsetnx k f v}
            {exhmset k f v}
            {exhmsetwithopts k f v 0 100}
            {exhincrby k f 1}
            {exhincrbyfloat k f 1.5}
        } {
            r del k
            r exhmsetlww k f 1 10
            r exhdellww k f 12
            r {*}$cmd
            assert_equal 13 [r exhver k f]
            # The writes the tombstone held off still lose.
            assert_equal {0 0} [r exhmsetlww k f late 5 f late 12]
            assert_equal 13 [r exhver k f]
            assert_equal {1} [r exhmsetlww k f newer 14]
        }
    }

    test {GT compares with the version of a tombstone, ABS overrides it} {
        r del k gone
        r exhmsetlww k f 1 10 keep v 1
        r exhdellww k f 12
        assert_error {*stale*} {r exhset k f v gt 12}
        assert_error {*stale*} {r exhincrby k f 1 gt 5}
        assert_error {*stale*} {r exhincrbyfloat k f 1.5 gt 12}
        assert_equal 0 [r exhexists k f]
        assert_equal {0} [r exhmsetlww k f late 12]
        assert_equal 1 [r exhset k f v gt 13]
        assert_equal 13 [r exhver k f]

        # A refused write leaves a hash with only tombstones as it was.
        r exhdellww gone f 12
        assert_error {*stale*} {r exhset gone f v gt 12}
        assert_equal 1 [r exists gone]
        assert {[r pttl gone] > 0}

        r exhdellww k f 20
        r exhset k f v abs 3
        assert_equal 3 [r exhver k f]
    }

    test {EXHDELLWW of a missing key keeps the tombstones in a hash that expires with them} {
        r del k
        assert_equal {1} [r exhdellww k f 5]
        assert_equal 1 [r exists k]
        assert_equal 0 [r exhlen k]
        set pttl [r pttl k]
        assert {$pttl > 3500000 && $pttl <= 3600000}
        assert_equal {0} [r exhmsetlww k f old 5]
        assert_equal 1 [r exists k]

        # A write that adds a field makes it a regular hash again.
        assert_equal {1} [r exhmsetlww k f v 6]
        assert_equal -1 [r ttl k]
    }

    test {EXHDELLWW that empties a hash with a TTL of its own keeps that TTL} {
        r del k
        r exhmsetlww k f v 1
        r expire k 100000
        assert_equal {1} [r exhdellww k f 1]
        assert_equal 1 [r exists k]
        assert {[r ttl k] > 99000}
        assert_equal {1} [r exhmsetlww k f v 2]
        assert {[r ttl k] > 99000}
    }

    test {Any write that adds a field removes the TTL of a hash with only tombstones} {
        foreach cmd {
            {exhset k g v}
            {exhsetnx k g v}
            {exhmset k g v}
            {exhmsetwithopts k g v 0 100}
            {exhincrby k g 1}
            {exhincrbyfloat k g 1.5}
        } {
            r del k
            r exhdellww k f 1
            assert {[r pttl k] > 0}
            r {*}$cmd
            assert_equal -1 [r ttl k]
        }
    }

    test {A hash with many tombstones is freed in the background} {
        # The server counts the objects it freed lazily since Redis 6.2.
        if {[s lazyfreed_objects] ne {}} {
            r del k
            set deletes {}
            for {set i 0} {$i < 200} {incr i} {
                lappend deletes "f$i" 1
            }
            r exhdellww k {*}$deletes
            r config resetstat
            r unlink k
            wait_for_condition 50 100 {
                [s lazyfreed_objects] == 1
            } else {
                fail "The hash was not freed in the background"
            }
        }
    }

    test {EXHDELLWW rejects malformed input before deleting anything} {
        r del k
        r exhmsetlww k f v 1
        assert_error {*wrong number of arguments*} {r exhdellww k}
        assert_error {*wrong number of arguments*} {r exhdellww k f}
        assert_error {*wrong number of arguments*} {r exhdellww k f 1 g}
        assert_error {*syntax*} {r exhdellww k f 1 g 0}
        assert_error {*syntax*} {r exhdellww k f 1 g nope}
        assert_equal {f} [r exhkeys k]

        r set str x
        assert_error {*WRONGTYPE*} {r exhdellww str f 1}
    }

    if {[tairhash_server_at_least 6 2]} {
        test {The internal LWW commands are rejected from clients} {
            r del k
            assert_error {*replication stream*} {r exhsyncrepl k 0 0}
            assert_error {*replication stream*} {r exhtombrepl k 1 f 1 1}
            assert_error {*replication stream*} {r exhsyncepoch 1}
            assert_equal 0 [r exists k]
        }
    }

    test {A value saved by TairHash 2.0 loads and takes last-write-wins writes} {
        r del k
        r restore k 0 [tairhash_v0_payload]
        assert_equal {a 1} [r exhgetwithver k plain]
        assert_equal {b 7} [r exhgetwithver k versioned]
        assert_equal {c 3} [r exhgetwithver k expiring]
        assert_equal -1 [r exhttl k plain]
        assert {[r exhttl k expiring] > 0}
        assert_equal {0 1} [r exhmsetlww k versioned old 6 versioned new 8]
        assert_equal {1 0} [r exhdellww k plain 1 expiring 2]
        assert_equal {expiring versioned} [lsort [r exhkeys k]]
        # It is saved in the current encoding from now on.
        r debug reload
        assert_equal {new 8} [r exhgetwithver k versioned]
        assert_equal {0} [r exhmsetlww k plain late 1]
    }

    # Servers before Redis 6.2 exit on a corrupt RESTORE payload instead of rejecting it.
    if {[tairhash_server_at_least 6 2]} {
        test {RESTORE rejects a value whose write sequence is out of range} {
            r del k src
            r exhset src f v
            # The value ends with the sequence of its last field, saved as 0x81 and 8 bytes, then
            # its end marker; the payload adds the RDB version in 2 bytes.
            set body [string range [r dump src] 0 end-8]
            set at [expr {[string length $body] - 11}]
            # One above the highest sequence a node accepts, LLONG_MAX / 2.
            set body [string replace $body $at [expr {$at + 7}] [binary format H* 4000000000000000]]
            assert_error {*Bad data format*} {r restore k 0 [tairhash_with_crc64 $body]}
            assert_equal 0 [r exists k]
            r del src
        }
    }

    test {EXHMSETREPL in the layout of TairHash 2.0 is applied} {
        r flushall
        tairhash_replay_from_aof exhmsetrepl k 1 f v 3 0 g w 4 4102444800000
        assert_equal {v 3} [r exhgetwithver k f]
        assert_equal {w 4} [r exhgetwithver k g]
        assert_equal -1 [r exhttl k f]
        assert {[r exhttl k g] > 0}
    }

    test {EXHTOMBREPL restores tombstones with the deadlines it carries} {
        r flushall
        # Deadline 1 is long past; 4102444800000 is in 2100.
        tairhash_replay_from_aof exhtombrepl k 1 ended 7 1 live 7 4102444800000
        assert_equal {1 0} [r exhmsetlww k ended v 5 live v 5]
    }

    test {Tombstones and the TTL of a hash with only tombstones survive DEBUG RELOAD} {
        r flushall
        r exhmsetlww deleted keep v 1 f v 1
        r exhdellww deleted f 7
        r exhdellww gone f 7
        set gone_pttl [r pttl gone]
        r debug reload
        assert_equal {0} [r exhmsetlww deleted f old 7]
        assert_equal {0} [r exhmsetlww gone f old 7]
        assert {[r pttl gone] > 0 && [r pttl gone] <= $gone_pttl}
        # It is still a hash with only tombstones: a write that adds a field removes its TTL.
        r exhset gone g v
        assert_equal -1 [r ttl gone]
    }

    test {Tombstones survive AOF replay and AOF rewrite} {
        r flushall
        r config set aof-use-rdb-preamble no
        r config set appendonly yes
        waitForBgrewriteaof r
        assert_equal ok [s aof_last_bgrewrite_status]

        # Replayed from the incremental AOF, i.e. the propagated effects.
        r exhmsetlww deleted keep v 1 f v 1
        r exhdellww deleted f 7
        r exhdellww gone f 7
        set check {
            assert_equal {0} [r exhmsetlww deleted f old 7]
            assert_equal {0} [r exhmsetlww gone f old 7]
            assert {[r pttl gone] > 0}
        }
        r debug loadaof
        eval $check
        set digest [r debug digest]

        # Replayed from the rewritten base, i.e. TairHashTypeAofRewrite.
        r bgrewriteaof
        waitForBgrewriteaof r
        assert_equal ok [s aof_last_bgrewrite_status]
        r debug loadaof
        assert_equal $digest [r debug digest]
        eval $check
        r exhset gone g v
        assert_equal -1 [r ttl gone]

        r config set appendonly no
        r config set aof-use-rdb-preamble yes
    }
}

# The sync commands need the server events of Redis 6.0+.
start_server {tags {"tairhash lww sync"} overrides {bind 0.0.0.0}} {
    r module load $testmodule
    if {![tairhash_server_at_least 6 0]} {
        test {The sync commands are left out without server events} {
            assert_error {*unknown command*} {r exhsyncbegin k}
            assert_error {*unknown command*} {r exhsyncget k}
        }
    } else {

        test {EXHSYNCGET reports a missing or never-synced hash as a miss} {
            r del k
            assert_equal {0 {}} [r exhsyncget k]
            r exhmsetlww k f v 1
            assert_equal {0 {}} [r exhsyncget k]
            assert_equal {0 {}} [r exhsyncget k f]
        }

        test {EXHSYNCBEGIN creates a placeholder that expires with the lease} {
            r del k
            set token [r exhsyncbegin k]
            assert {$token > 0}
            assert_equal 1 [r exists k]
            assert_equal 0 [r exhlen k]
            set pttl [r pttl k]
            assert {$pttl > 50000 && $pttl <= 60000}
            assert_equal 0 [synced r k]
        }

        test {EXHSYNCBEGIN leaves the TTL of an existing hash alone} {
            r del k
            r exhmsetlww k f v 1
            assert_equal -1 [r ttl k]
            r exhsyncbegin k
            assert_equal -1 [r ttl k]
            r expire k 100000
            r exhsyncbegin k
            assert {[r ttl k] > 99000}
        }

        test {Concurrent syncs share the lease and only the first EXHSYNC completes} {
            r del k
            set t1 [r exhsyncbegin k]
            set t2 [r exhsyncbegin k]
            assert_equal $t1 $t2
            assert_equal {1 0} [r exhsync k $t1 f a 1]
            assert_error {*EXHSYNCBEGIN*} {r exhsync k $t2 f b 2}
            assert_equal {{f a 1}} [sync_entries [r exhsyncget k]]
            # The next sync gets a new lease.
            assert {[r exhsyncbegin k] > $t1}
        }

        test {EXHSYNC merges the snapshot, marks the hash synced and makes the placeholder permanent} {
            r del k
            assert_equal {2 0} [r exhsync k [r exhsyncbegin k] f1 a 10 f2 b 20]
            assert_equal -1 [r ttl k]
            assert_equal 1 [synced r k]
            assert_equal {{f1 a 10} {f2 b 20}} [sync_entries [r exhsyncget k]]
        }

        test {EXHSYNC keeps a TTL the hash had of its own} {
            r del k
            r exhmsetlww k f v 1
            r expire k 100000
            r exhsync k [r exhsyncbegin k] f v 1
            assert {[r ttl k] > 99000}
            assert_equal 1 [synced r k]
        }

        test {EXHSYNC keeps newer cached writes and removes fields the source no longer has} {
            r del k
            r exhsync k [r exhsyncbegin k] f1 a 10 f2 b 20 gone x 5
            # The source now holds f2 at an older version than the cache, and no 'gone'.
            assert_equal {0 1} [r exhsync k [r exhsyncbegin k] f1 a 10 f2 old 15]
            assert_equal {{f1 a 10} {f2 b 20}} [sync_entries [r exhsyncget k]]
        }

        test {EXHSYNC keeps a field written after the token even when its version is old} {
            r del k
            r exhsync k [r exhsyncbegin k] f1 a 10
            set token [r exhsyncbegin k]
            # Raced the snapshot read: a late-arriving write carrying an old version.
            r exhmsetlww k late v 1
            assert_equal {0 0} [r exhsync k $token f1 a 10]
            assert_equal {{f1 a 10} {late v 1}} [sync_entries [r exhsyncget k]]

            # A sync opened after that write removes it if the source lacks it.
            assert_equal {0 1} [r exhsync k [r exhsyncbegin k] f1 a 10]
            assert_equal {{f1 a 10}} [sync_entries [r exhsyncget k]]
        }

        test {EXHSYNC applies repeated snapshot fields in argument order} {
            r del k
            r exhmsetlww k absent x 1
            assert_equal {2 1} [r exhsync k [r exhsyncbegin k] f a 1 f b 2 f c 1]
            assert_equal {{f b 2}} [sync_entries [r exhsyncget k]]
        }

        test {Fields that carry no write sequence sort below a later token} {
            # Loaded from a value TairHash 2.0 saved.
            r del k
            r restore k 0 [tairhash_v0_payload]
            set token [r exhsyncbegin k]
            r exhmsetlww k raced x 1
            assert_equal {0 2} [r exhsync k $token plain a 1]
            assert_equal {{plain a 1} {raced x 1}} [sync_entries [r exhsyncget k]]

            # Applied from an EXHMSETREPL in the layout of TairHash 2.0.
            r flushall
            tairhash_replay_from_aof exhmsetrepl k 1 f v 3 0 g w 4 0
            set token [r exhsyncbegin k]
            r exhmsetlww k raced x 1
            assert_equal {0 1} [r exhsync k $token f v 3]
            assert_equal {{f v 3} {raced x 1}} [sync_entries [r exhsyncget k]]
        }

        test {EXHSYNC fencing covers every write command} {
            r del k
            r exhsync k [r exhsyncbegin k] base v 1
            set token [r exhsyncbegin k]
            r exhset k a v
            r exhsetnx k b v
            r exhmset k c v
            r exhmsetwithopts k d v 0 100
            r exhincrby k e 1
            r exhincrbyfloat k f 1.5
            assert_equal {0 0} [r exhsync k $token base v 1]
            assert_equal 7 [r exhlen k]
        }

        test {A delete that races the snapshot read is not undone by EXHSYNC} {
            r del k
            r exhsync k [r exhsyncbegin k] f a 5 g b 5
            set token [r exhsyncbegin k]
            # The snapshot was read before the source deleted f; the delete reaches the cache first.
            r exhdellww k f 6
            assert_equal {0 0} [r exhsync k $token f a 5 g b 5]
            assert_equal {{g b 5}} [sync_entries [r exhsyncget k]]
        }

        test {EXHSYNC fails once the key went away during the sync} {
            r del k
            set token [r exhsyncbegin k]
            r del k
            assert_error {*EXHSYNCBEGIN*} {r exhsync k $token f a 1}
            assert_equal 0 [r exists k]

            # Nor does a lease of a key recreated in the meantime accept the old token.
            set token [r exhsyncbegin k]
            r del k
            set fresh [r exhsyncbegin k]
            assert_error {*EXHSYNCBEGIN*} {r exhsync k $token f a 1}
            assert_equal {1 0} [r exhsync k $fresh f b 2]
        }

        test {EXHSYNC requires an open lease} {
            r del k
            r exhmsetlww k f v 1
            assert_error {*EXHSYNCBEGIN*} {r exhsync k 12345 f a 10}
            set token [r exhsyncbegin k]
            assert_error {*EXHSYNCBEGIN*} {r exhsync k [expr {$token + 1}] f a 10}
            assert_equal {f} [r exhkeys k]
            assert_equal 0 [synced r k]
        }

        test {A hash with an open sync survives losing its last field} {
            r del k
            r exhmsetlww k f v 1
            set token [r exhsyncbegin k]
            r exhdel k f
            assert_equal 1 [r exists k]
            assert {[r pttl k] > 0}
            assert_equal {1 0} [r exhsync k $token g w 1]
            assert_equal -1 [r ttl k]
        }

        test {EXHSYNC with an empty snapshot caches an empty synced hash} {
            r del k
            assert_equal {0 0} [r exhsync k [r exhsyncbegin k]]
            assert_equal 1 [r exists k]
            assert_equal -1 [r ttl k]
            assert_equal 0 [r exhlen k]
            assert_equal {1 {}} [r exhsyncget k]
        }

        test {A synced hash survives losing its last field, an unsynced one does not} {
            r del synced plain
            r exhsync synced [r exhsyncbegin synced] f v 1
            r exhdel synced f
            assert_equal 1 [r exists synced]
            assert_equal 1 [synced r synced]

            r exhmsetlww plain f v 1
            r exhdel plain f
            assert_equal 0 [r exists plain]
        }

        test {EXHSYNCGET with fields returns only those that exist, in request order} {
            r del k
            r exhsync k [r exhsyncbegin k] f1 a 10 f2 b 20
            assert_equal {1 {f2 b 20 f1 a 10}} [r exhsyncget k f2 missing f1]
        }

        test {EXHSYNCGET omits expired fields} {
            r del k probe
            r exhsync k [r exhsyncbegin k] f1 a 10
            r exhset k f2 v px 50
            # A key set after the field expires after it.
            r set probe x px 50
            wait_for_condition 50 100 {
                [r exists probe] == 0
            } else {
                fail "The probe key did not expire"
            }
            assert_equal {{f1 a 10}} [sync_entries [r exhsyncget k]]
            assert_equal {f1 a 10} [lindex [r exhsyncget k f1 f2] 1]
        }

        test {EXHSYNC rejects malformed input} {
            r del k
            assert_error {*wrong number of arguments*} {r exhsync k}
            assert_error {*wrong number of arguments*} {r exhsync k 1 f v}
            assert_error {*wrong number of arguments*} {r exhsyncbegin}
            assert_error {*wrong number of arguments*} {r exhsyncbegin k k}
            assert_error {*syntax*} {r exhsync k 0}
            assert_error {*syntax*} {r exhsync k -1}
            assert_error {*syntax*} {r exhsync k nope}
            set token [r exhsyncbegin k]
            assert_error {*syntax*} {r exhsync k $token f v 0}
            assert_equal 0 [synced r k]

            r set str x
            assert_error {*WRONGTYPE*} {r exhsync str 1}
            assert_error {*WRONGTYPE*} {r exhsyncget str}
            assert_error {*WRONGTYPE*} {r exhsyncbegin str}
        }

        test {EXHSYNCGET leaves watched keys alone} {
            r del k
            r exhsync k [r exhsyncbegin k] f a 10
            r watch k
            r exhsyncget k
            r exhsyncget k f
            r multi
            r ping
            assert_equal PONG [r exec]
        }

        test {DEBUG RELOAD makes synced hashes misses and keeps their data} {
            r flushall
            r exhsync synced [r exhsyncbegin synced] f1 a 10 f2 b 20
            r exhsync empty [r exhsyncbegin empty]
            r debug reload
            assert_equal {0 {}} [r exhsyncget synced]
            assert_equal {0 {}} [r exhsyncget empty]
            assert_equal {a 10} [r exhgetwithver synced f1]
            # A new sync makes it a hit again.
            r exhsync synced [r exhsyncbegin synced] f1 a 10 f2 b 20
            assert_equal {{f1 a 10} {f2 b 20}} [sync_entries [r exhsyncget synced]]
        }

        test {RESTORE does not make a hash synced} {
            r del k other
            r exhsync k [r exhsyncbegin k] f old 1 gone x 1
            r exhdellww k gone 2
            set payload [r dump k]
            r exhmsetlww k f new 2

            # The payload lacks that write: only a sync can vouch for the hash again.
            r restore k 0 $payload replace
            assert_equal {0 {}} [r exhsyncget k]
            assert_equal {old 1} [r exhgetwithver k f]
            r restore other 0 $payload
            assert_equal {0 {}} [r exhsyncget other]

            # The tombstones come along.
            assert_equal {0} [r exhmsetlww k gone late 2]
            assert_equal {0} [r exhmsetlww other gone late 2]
        }

        test {RENAME and MOVE keep the sync state, the tombstones and an open sync of a hash} {
            r select 10
            r del b
            r select 9
            r del a b
            r exhsync a [r exhsyncbegin a] f v 1 gone x 1
            r exhdellww a gone 2
            set token [r exhsyncbegin a]

            r rename a b
            assert_equal {1 {f v 1}} [r exhsyncget b]
            assert_equal {0} [r exhmsetlww b gone late 2]
            assert_error {*EXHSYNCBEGIN*} {r exhsync a $token f v 1}
            assert_equal {0 0} [r exhsync b $token f v 1]

            set token [r exhsyncbegin b]
            r move b 10
            r select 10
            assert_equal {1 {f v 1}} [r exhsyncget b]
            assert_equal {0} [r exhmsetlww b gone late 2]
            assert_equal {0 0} [r exhsync b $token f v 1]
            r del b
            r select 9
        }

        test {COPY makes a hash that is not synced and has no tombstones} {
            r del a acopy
            r exhsync a [r exhsyncbegin a] f v 1 gone x 1
            r exhdellww a gone 2
            # Not every server and expire mode can copy a TairHash.
            if {![catch {r copy a acopy}]} {
                assert_equal {0 {}} [r exhsyncget acopy]
                assert_equal {v 1} [r exhgetwithver acopy f]
                assert_equal {1} [r exhmsetlww acopy gone late 2]
                assert_equal {1 {f v 1}} [r exhsyncget a]
            }
        }

        test {An empty hash that is no longer synced does not stay behind} {
            r flushall
            r exhsync missed [r exhsyncbegin missed]
            r exhsync resynced [r exhsyncbegin resynced]
            r exhsync abandoned [r exhsyncbegin abandoned]
            r debug reload
            foreach key {missed resynced abandoned} {
                assert_equal 1 [r exists $key]
                assert_equal -1 [r ttl $key]
            }

            # A read that misses it removes it.
            assert_equal {0 {}} [r exhsyncget missed]
            assert_equal 0 [r exists missed]

            # A sync of it makes it a placeholder again, which expires if the sync is abandoned.
            r exhsyncbegin abandoned
            set pttl [r pttl abandoned]
            assert {$pttl > 50000 && $pttl <= 60000}
            assert_equal {0 0} [r exhsync resynced [r exhsyncbegin resynced]]
            assert_equal -1 [r ttl resynced]
            assert_equal {1 {}} [r exhsyncget resynced]

            # A TTL the key has of its own is left alone.
            r exhsync own [r exhsyncbegin own]
            r expire own 100000
            r debug reload
            r exhsyncbegin own
            assert {[r ttl own] > 99000}
        }

        test {Sync state survives AOF replay and AOF rewrite as a miss, placeholders with their TTL} {
            r flushall
            r config set aof-use-rdb-preamble no
            r config set appendonly yes
            waitForBgrewriteaof r
            assert_equal ok [s aof_last_bgrewrite_status]

            # Replayed from the incremental AOF, i.e. the propagated effects.
            r exhsync synced [r exhsyncbegin synced] f1 a 10 gone x 1
            r exhsync synced [r exhsyncbegin synced] f1 a 10
            r exhsyncbegin placeholder
            set check {
                assert_equal {0 {}} [r exhsyncget synced]
                assert_equal {f1} [r exhkeys synced]
                assert {[r pttl placeholder] > 0}
            }
            r debug loadaof
            eval $check
            set digest [r debug digest]

            # Replayed from the rewritten base, i.e. TairHashTypeAofRewrite.
            r bgrewriteaof
            waitForBgrewriteaof r
            assert_equal ok [s aof_last_bgrewrite_status]
            r debug loadaof
            assert_equal $digest [r debug digest]
            eval $check
            r exhset placeholder g v
            assert_equal -1 [r ttl placeholder]

            r config set appendonly no
            r config set aof-use-rdb-preamble yes
        }

        test {Replaying the AOF does not decide again whether an emptied hash is kept} {
            r flushall
            r config set aof-use-rdb-preamble no
            r config set appendonly yes
            waitForBgrewriteaof r
            assert_equal ok [s aof_last_bgrewrite_status]

            # Kept without fields because it is synced, with the TTL the key has of its own.
            r exhsync synced [r exhsyncbegin synced] f v 1
            r expire synced 100000
            r exhdel synced f
            r exhmsetlww synced g v 2
            # Emptied by a sync.
            r exhmsetlww resynced a v 1 b v 1
            r expire resynced 100000
            r exhsync resynced [r exhsyncbegin resynced]
            r exhmsetlww resynced c v 5
            # Kept without fields because a sync of it is open.
            r exhmsetlww leased f v 1
            r expire leased 100000
            r exhsyncbegin leased
            r exhdel leased f
            r exhmsetlww leased g v 2
            # Kept for its tombstone, and without a TTL because it is synced.
            r exhsync tomb [r exhsyncbegin tomb] a v 1 b v 1
            r exhdellww tomb a 2
            r exhdel tomb b
            r exhmsetlww tomb c v 5

            set check {
                foreach key {synced resynced leased} {
                    assert {[r ttl $key] > 99000}
                }
                assert_equal -1 [r ttl tomb]
                assert_equal {g} [r exhkeys synced]
                assert_equal {c} [r exhkeys resynced]
                assert_equal {g} [r exhkeys leased]
                assert_equal {c} [r exhkeys tomb]
            }
            eval $check
            r debug loadaof
            eval $check

            r config set appendonly no
            r config set aof-use-rdb-preamble yes
        }
    }
}

start_server {tags {"tairhash lww retention"} overrides {bind 0.0.0.0}} {
    r module load $testmodule lww_tombstone_ttl 1 sync_lease_ttl 1

    test {A tombstone holds writes off until lww_tombstone_ttl} {
        r del k gone
        r exhmsetlww k keep v 1 f v 1
        r exhdellww k f 5
        assert_equal {0} [r exhmsetlww k f old 4]
        # A hash with only a tombstone, set later, expires with it.
        r exhdellww gone f 5
        wait_for_condition 50 100 {
            [r exists gone] == 0
        } else {
            fail "The hash with only a tombstone did not expire"
        }
        assert_equal {1} [r exhmsetlww k f old 4]
    }

    if {[tairhash_server_at_least 6 0]} {
        test {A sync lease ends after sync_lease_ttl} {
            r del placeholder k
            set token [r exhsyncbegin placeholder]
            r exhmsetlww k f v 1
            set k_token [r exhsyncbegin k]
            # While a lease is open EXHSYNCBEGIN returns its token.
            wait_for_condition 50 100 {
                [r exhsyncbegin k] != $k_token
            } else {
                fail "The sync lease did not end"
            }
            wait_for_condition 50 100 {
                [r exists placeholder] == 0
            } else {
                fail "The placeholder did not expire with its lease"
            }
            assert_error {*EXHSYNCBEGIN*} {r exhsync k $k_token f v 1}
            set fresh [r exhsyncbegin k]
            assert {$fresh > $k_token}
            assert_equal {0 0} [r exhsync k $fresh f v 1]
        }

        test {A hash whose sync lease ended is not kept once it loses its last field} {
            r del k probe
            r exhmsetlww k f v 1
            r exhsyncbegin k
            # A lease opened later ends later.
            set probe_token [r exhsyncbegin probe]
            wait_for_condition 50 100 {
                [r exhsyncbegin probe] != $probe_token
            } else {
                fail "The sync lease did not end"
            }
            r exhdel k f
            assert_equal 0 [r exists k]
        }
    }

    # A hash with only a tombstone expires with it, so one set after the tombstones of
    # another hash shows when those have ended.
    proc tairhash_wait_tombstones_ended {} {
        r del gone
        r exhdellww gone f 1
        wait_for_condition 50 100 {
            [r exists gone] == 0
        } else {
            fail "The hash with only a tombstone did not expire"
        }
    }

    test {Expired tombstones are dropped as deletes go on} {
        r del k
        r exhmsetlww k keep v 1
        for {set i 0} {$i < 100} {incr i} {
            r exhdellww k "a$i" 1
        }
        set before [r memory usage k]
        tairhash_wait_tombstones_ended
        for {set i 0} {$i < 30} {incr i} {
            r exhdellww k "b$i" 1
        }
        assert {[r memory usage k] < $before}
    }

    test {Expired tombstones are dropped by later writes} {
        r del k
        r exhmsetlww k keep v 1
        for {set i 0} {$i < 50} {incr i} {
            r exhdellww k "a$i" 1
        }
        set before [r memory usage k]
        set version 1
        wait_for_condition 50 100 {
            [r exhmsetlww k keep v [incr version]] eq {1} && [r memory usage k] < $before
        } else {
            fail "The expired tombstones were not dropped"
        }
    }

    test {Expired tombstones are not saved} {
        r del k
        r exhmsetlww k keep v 1
        for {set i 0} {$i < 50} {incr i} {
            r exhdellww k "a$i" 1
        }
        set before [r memory usage k]
        tairhash_wait_tombstones_ended
        r debug reload
        assert {[r memory usage k] < $before}
        assert_equal {v 1} [r exhgetwithver k keep]
    }

    if {[tairhash_server_at_least 6 0]} {
        # Last in this server: it leaves the write sequence ahead of the clock for good.
        test {A sync lease ends on time when the write sequence is ahead of the clock} {
            r del k
            r exhmsetlww k f v 1
            # A burst of more than a million field writes a second, or a master with a faster
            # clock, leaves the sequence ahead; here by far more than the lease lasts.
            tairhash_replay_from_aof exhmsetrepl ahead 2 f v 1 0 4000000000000000000
            set token [r exhsyncbegin k]
            assert {$token > 4000000000000000000}
            wait_for_condition 50 100 {
                [r exhsyncbegin k] != $token
            } else {
                fail "The sync lease did not end"
            }
            assert_error {*EXHSYNCBEGIN*} {r exhsync k $token f v 1}
        }
    }
}

# restart_server takes these arguments since the Redis 6.2 test harness.
if {[info commands restart_server] ne {} && [llength [info args restart_server]] >= 3} {
    start_server [list tags {"tairhash lww restart"} overrides [list bind 0.0.0.0 loadmodule $testmodule]] {
        test {A restart makes synced hashes misses and sequences continue above persisted ones} {
            r flushall
            r exhsync k [r exhsyncbegin k] f v 1
            r exhmsetlww k g v 1
            set token [r exhsyncbegin k]
            r save
            restart_server 0 true false
            assert_equal 0 [synced r k]
            assert_equal {v 1} [r exhgetwithver k f]
            assert_error {*EXHSYNCBEGIN*} {r exhsync k $token f v 1}
            assert {[r exhsyncbegin k] > $token}
        }
    }
}

# Last-write-wins writes carry their write sequence, which only the state format can, so
# they replicate in it even when replication_format is compat.
foreach format {state compat} {
    start_server {tags {"tairhash lww repl"} overrides {bind 0.0.0.0}} {
        r module load $testmodule
        set replica [srv 0 client]

        start_server {overrides {bind 0.0.0.0}} {
            r module load $testmodule replication_format $format
            set master [srv 0 client]

            set sync [tairhash_server_at_least 6 0]
            # Written before the replica attaches, so it arrives in the RDB of the full sync.
            $master exhmsetlww before f v 1
            $master exhdellww before g 3
            if {$sync} {
                $master exhsync before [$master exhsyncbegin before] f v 1
            }

            $replica replicaof [srv 0 host] [srv 0 port]
            wait_for_condition 50 100 {
                [lindex [$replica role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }

            test "A full sync carries the LWW state to the replica ($format)" {
                wait_for_ofs_sync $master $replica
                assert_equal [$master debug digest] [$replica debug digest]
                if {$sync} {
                    assert_equal {1 {f v 1}} [$replica exhsyncget before]
                }
            }

            test "Replicas converge on LWW writes and deletes ($format)" {
                $master exhmsetlww w f1 a 10 f2 b 20
                $master exhmsetlww w f1 stale 5
                $master exhdellww w f2 20
                $master exhmsetlww w f2 old 19
                $master exhdellww wgone f 4
                # A write by another command over a tombstone.
                $master exhmsetlww over f a 10
                $master exhdellww over f 12
                $master exhset over f plain
                wait_for_ofs_sync $master $replica
                assert_equal [$master debug digest] [$replica debug digest]
                assert_equal {f1} [$replica exhkeys w]
                assert {[$replica pttl wgone] > 0}
                assert_equal {plain 13} [$replica exhgetwithver over f]
            }

            if {$sync} {
                test "Replicas converge on syncs ($format)" {
                    $master exhmsetlww k f1 a 10 f2 b 20 f3 c 1
                    $master exhmsetlww k f1 stale 5
                    set token [$master exhsyncbegin k]
                    $master exhmsetlww k late x 1
                    $master exhdellww k f3 2
                    $master exhsync k $token f1 a 10 f2 b 25 f3 c 1
                    $master exhsync k [$master exhsyncbegin k] f1 a 10 f2 b 25
                    $master exhsync empty [$master exhsyncbegin empty]
                    $master exhdellww gone f 4
                    $master exhsyncbegin placeholder

                    wait_for_ofs_sync $master $replica
                    assert_equal [$master debug digest] [$replica debug digest]
                    assert_equal {{f1 a 10} {f2 b 25}} [sync_entries [$replica exhsyncget k]]
                    assert_equal {1 {}} [$replica exhsyncget empty]
                    assert {[$replica pttl gone] > 0}
                    assert {[$replica pttl placeholder] > 0}
                    assert_equal -1 [$replica ttl k]
                }

                test "A promoted replica keeps the data and tombstones but nothing is synced ($format)" {
                    set token [$master exhsyncbegin k]
                    $replica replicaof no one
                    wait_for_condition 50 100 {
                        [lindex [$replica role] 0] eq {master}
                    } else {
                        fail "The replica was not promoted"
                    }
                    assert_equal {0 {}} [$replica exhsyncget k]
                    assert_equal {a 10} [$replica exhgetwithver k f1]
                    assert_equal {0} [$replica exhmsetlww gone f old 4]
                    assert_equal {0} [$replica exhmsetlww before g old 3]
                    # The old master's lease did not come along.
                    assert_error {*EXHSYNCBEGIN*} {$replica exhsync k $token f1 a 10}
                    $replica exhsync k [$replica exhsyncbegin k] f1 a 10 f2 b 25
                    assert_equal 1 [synced $replica k]
                }
            }
        }
    }
}

start_server {tags {"tairhash lww expire timer"} overrides {bind 0.0.0.0}} {
    r module load $testmodule active_expire_period 50

    test {The expire timer propagates what it does to hashes with tombstones in well-formed transactions} {
        r config set appendonly yes
        waitForBgrewriteaof r
        # In one run the timer leaves some hashes with only tombstones, which get a key TTL,
        # and expires fields of others.
        for {set i 0} {$i < 10} {incr i} {
            r exhmsetlww tomb$i a 1 1
            r exhdellww tomb$i a 2
            r exhset tomb$i b v px 100
            r exhset plain$i b v px 100
            r exhset plain$i keep v
        }
        wait_for_condition 100 100 {
            [tairhash_timer_done]
        } else {
            fail "The expire timer did not expire the fields"
        }
        r ping

        set fd [open [tairhash_aof_path] r]
        fconfigure $fd -translation binary
        set open 0
        set malformed 0
        foreach line [split [read $fd] "\n"] {
            set line [string trimright $line "\r"]
            if {$line eq {MULTI}} {
                incr malformed $open
                set open 1
            } elseif {$line eq {EXEC}} {
                incr malformed [expr {!$open}]
                set open 0
            }
        }
        close $fd
        assert_equal 0 $malformed
        r config set appendonly no
    }
}

start_server {tags {"tairhash lww options"} overrides {bind 0.0.0.0}} {
    test {Retention options that would void the guarantees are rejected at load} {
        # A tombstone must outlive a sync that was open when it was set.
        assert_error {*rror loading the extension*} {r module load $testmodule lww_tombstone_ttl 59}
        assert_error {*rror loading the extension*} {r module load $testmodule sync_lease_ttl 3601}
        assert_error {*rror loading the extension*} {r module load $testmodule lww_tombstone_ttl 10 sync_lease_ttl 11}
        # A TTL too long to compute a deadline from.
        assert_error {*rror loading the extension*} {r module load $testmodule lww_tombstone_ttl 9223372036854775}
        assert_error {*rror loading the extension*} {r module load $testmodule lww_tombstone_ttl 0}
        assert_error {*rror loading the extension*} {r module load $testmodule sync_lease_ttl -1}

        assert_equal OK [r module load $testmodule lww_tombstone_ttl 4611686018427387 sync_lease_ttl 4611686018427387]
        assert_equal {1} [r exhdellww k f 5]
        assert_equal {0} [r exhmsetlww k f late 5]
        assert {[r pttl k] > 0}
    }
}

start_server {tags {"tairhash lww lease"} overrides {bind 0.0.0.0}} {
    r module load $testmodule
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]

    start_server {overrides {bind 0.0.0.0}} {
        r module load $testmodule
        set master [srv 0 client]

        if {[tairhash_server_at_least 6 0]} {
            test {A sync lease does not survive a failover and back} {
                # A ping in flight during the failover would cost the demoted master its
                # partial resync, and with its dataset the lease this test is about.
                $master config set repl-ping-replica-period 3600
                tairhash_attach_replica $replica [srv 0 host] [srv 0 port]
                $master exhmsetlww k f v 1
                wait_for_ofs_sync $master $replica
                set token [$master exhsyncbegin k]

                $replica replicaof no one
                tairhash_attach_replica $master $replica_host $replica_port
                assert {[status $replica sync_partial_ok] > 0}
                $replica exhmsetlww k g v 1
                wait_for_ofs_sync $replica $master

                $master replicaof no one
                assert_error {*EXHSYNCBEGIN*} {$master exhsync k $token f v 1}
                assert_equal {f g} [lsort [$master exhkeys k]]
                assert {[$master exhsyncbegin k] > $token}
            }

            test {A master that becomes a replica has no epoch until it gets its master's} {
                $master exhsync e [$master exhsyncbegin e] f v 1
                assert_equal 1 [synced $master e]
                # Nothing listens on that port: it stays without a master.
                $master replicaof [srv 0 host] [expr {[srv 0 port] + 10000}]
                assert_equal {0 {}} [$master exhsyncget e]
                $master replicaof no one
                assert_equal {0 {}} [$master exhsyncget e]
                assert_equal {v 1} [$master exhgetwithver e f]
            }
        }
    }
}

# A master puts each new sync epoch in the replication stream, so a replica ends at its
# master's epoch however the stream reaches it.
start_server {tags {"tairhash lww epoch"} overrides {bind 0.0.0.0}} {
    r module load $testmodule
    set lagging [srv 0 client]
    set lagging_host [srv 0 host]
    set lagging_port [srv 0 port]

    start_server {overrides {bind 0.0.0.0}} {
        r module load $testmodule
        set replica [srv 0 client]
        set replica_host [srv 0 host]
        set replica_port [srv 0 port]

        start_server {overrides {bind 0.0.0.0}} {
            r module load $testmodule
            set master [srv 0 client]

            if {[tairhash_server_at_least 6 0]} {
                test {A replica that replays the backlog of a promoted replica ends at its epoch} {
                    tairhash_attach_replica $replica [srv 0 host] [srv 0 port]
                    tairhash_attach_replica $lagging [srv 0 host] [srv 0 port]
                    $master exhsync h [$master exhsyncbegin h] f v 1
                    wait_for_ofs_sync $master $replica
                    wait_for_ofs_sync $master $lagging
                    assert_equal 1 [synced $lagging h]

                    # One replica loses its link (nothing listens on that port) and misses a
                    # sync that the other one receives.
                    $lagging replicaof $lagging_host [expr {$lagging_port + 10000}]
                    # A replica that changes master keeps the epoch of the data it has.
                    assert_equal 1 [synced $lagging h]
                    $master exhsync other [$master exhsyncbegin other] f v 1
                    wait_for_ofs_sync $master $replica

                    $replica replicaof no one
                    $lagging replicaof $replica_host $replica_port
                    tairhash_wait_converged $replica $lagging
                    # It got the rest of the old master's stream, that sync included, from
                    # the backlog of the promoted replica.
                    assert {[status $replica sync_partial_ok] > 0}
                    assert_equal {v 1} [$lagging exhgetwithver other f]
                    assert_equal {0 {}} [$replica exhsyncget h]
                    assert_equal {0 {}} [$lagging exhsyncget h]
                    assert_equal {0 {}} [$lagging exhsyncget other]

                    # What the new master syncs is synced on the replica, and nothing else.
                    $replica exhsync h [$replica exhsyncbegin h] f v 1
                    wait_for_ofs_sync $replica $lagging
                    assert_equal {1 {f v 1}} [$lagging exhsyncget h]
                    assert_equal {0 {}} [$lagging exhsyncget other]
                    assert_equal [$replica debug digest] [$lagging debug digest]
                }
            }
        }
    }
}

start_server {tags {"tairhash lww epoch"} overrides {bind 0.0.0.0}} {
    r module load $testmodule
    set leaf [srv 0 client]

    start_server {overrides {bind 0.0.0.0}} {
        r module load $testmodule
        set mid [srv 0 client]
        set mid_host [srv 0 host]
        set mid_port [srv 0 port]

        start_server {overrides {bind 0.0.0.0}} {
            r module load $testmodule
            set master [srv 0 client]

            if {[tairhash_server_at_least 6 0]} {
                test {A replica of a promoted replica ends at its epoch} {
                    tairhash_attach_replica $mid [srv 0 host] [srv 0 port]
                    tairhash_attach_replica $leaf $mid_host $mid_port
                    $master exhsync h [$master exhsyncbegin h] f v 1
                    wait_for_ofs_sync $master $mid
                    wait_for_ofs_sync $mid $leaf
                    assert_equal 1 [synced $leaf h]

                    $mid replicaof no one
                    tairhash_wait_converged $mid $leaf
                    assert_equal {0 {}} [$mid exhsyncget h]
                    assert_equal {0 {}} [$leaf exhsyncget h]
                    assert_equal {v 1} [$leaf exhgetwithver h f]
                }

                test {A replica follows the epoch its master starts when it reloads its RDB} {
                    $mid exhsync h [$mid exhsyncbegin h] f v 1
                    wait_for_ofs_sync $mid $leaf
                    assert_equal 1 [synced $leaf h]

                    $mid debug reload
                    tairhash_wait_converged $mid $leaf
                    assert_equal {0 {}} [$mid exhsyncget h]
                    assert_equal {0 {}} [$leaf exhsyncget h]
                    assert_equal {v 1} [$leaf exhgetwithver h f]
                }
            }

            # CLIENT PAUSE got its WRITE mode, in which a replica can still be promoted, in Redis 6.2.
            if {[tairhash_server_at_least 6 2]} {
                test {An epoch started while clients are paused is announced when the pause ends} {
                    tairhash_attach_replica $mid [srv 0 host] [srv 0 port]
                    tairhash_wait_converged $mid $leaf
                    $master exhsync h [$master exhsyncbegin h] f v 1
                    wait_for_ofs_sync $master $mid
                    wait_for_ofs_sync $mid $leaf
                    assert_equal 1 [synced $leaf h]

                    # Nothing may be propagated during the pause.
                    $mid client pause 1000 write
                    $mid replicaof no one
                    assert_equal {0 {}} [$mid exhsyncget h]
                    tairhash_wait_converged $mid $leaf
                    assert_equal {0 {}} [$leaf exhsyncget h]
                }
            }

            if {[tairhash_server_at_least 6 0]} {
                test {A replica follows the epoch its master starts when it reloads its AOF} {
                    $mid config set appendonly yes
                    # Older harnesses can only wait for the rewrite of the innermost server.
                    wait_for_condition 50 100 {
                        [status $mid aof_rewrite_in_progress] == 0 && [status $mid aof_rewrite_scheduled] == 0
                    } else {
                        fail "The AOF rewrite did not finish"
                    }
                    $mid exhsync h [$mid exhsyncbegin h] f v 1
                    wait_for_ofs_sync $mid $leaf
                    assert_equal 1 [synced $leaf h]

                    $mid debug loadaof
                    tairhash_wait_converged $mid $leaf
                    assert_equal {0 {}} [$mid exhsyncget h]
                    assert_equal {0 {}} [$leaf exhsyncget h]
                    $mid config set appendonly no
                }
            }
        }
    }
}

# restart_server takes these arguments since the Redis 6.2 test harness.
if {[info commands restart_server] ne {} && [llength [info args restart_server]] >= 3} {
    start_server [list tags {"tairhash lww epoch restart"} overrides [list bind 0.0.0.0 loadmodule $testmodule]] {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        start_server [list overrides [list bind 0.0.0.0 loadmodule $testmodule]] {
            test {A replica restarted from its RDB follows an epoch its master started meanwhile} {
                set replica [srv 0 client]
                tairhash_attach_replica $replica $master_host $master_port
                $master exhsync h [$master exhsyncbegin h] f v 1
                wait_for_ofs_sync $master $replica
                assert_equal 1 [synced $replica h]

                # Its configuration names the master, and its RDB holds the epoch as of now.
                $replica config rewrite
                $replica save
                # The master reloads while the replica is down.
                catch {$replica shutdown nosave}
                $master debug reload
                restart_server 0 true false
                set replica [srv 0 client]
                tairhash_wait_converged $master $replica
                assert {[status $master sync_partial_ok] > 0}
                assert_equal {0 {}} [$replica exhsyncget h]
                assert_equal {v 1} [$replica exhgetwithver h f]
            }
        }
    }
}

# A cluster replica loads its data at startup before the cluster bus gives it its master.
# The harness selects a database on every connection unless it is told the server has one,
# which it can be told since Redis 6.2.
if {[info commands restart_server] ne {} && [llength [info args restart_server]] >= 3 && [info exists ::singledb]} {
    set tairhash_singledb $::singledb
    set ::singledb 1
    start_server [list tags {"tairhash lww cluster"} overrides [list bind 0.0.0.0 cluster-enabled yes loadmodule $testmodule]] {
        set master [srv 0 client]
        set master_host [srv 0 host]
        set master_port [srv 0 port]

        start_server [list overrides [list bind 0.0.0.0 cluster-enabled yes loadmodule $testmodule]] {
            test {A cluster replica restarted from its RDB has the epoch of its master} {
                set replica [srv 0 client]
                set slots {}
                for {set slot 0} {$slot < 16384} {incr slot} {
                    lappend slots $slot
                }
                $master cluster addslots {*}$slots
                $replica cluster meet $master_host $master_port
                set master_id [$master cluster myid]
                wait_for_condition 100 100 {
                    ![catch {$replica cluster replicate $master_id}]
                } else {
                    fail "The node did not become a replica"
                }
                wait_for_condition 100 100 {
                    [string match {*master_link_status:up*} [$replica info replication]] &&
                    [string match {*cluster_state:ok*} [$master cluster info]]
                } else {
                    fail "The cluster did not form"
                }
                $master exhsync h [$master exhsyncbegin h] f v 1
                wait_for_ofs_sync $master $replica
                $replica readonly
                assert_equal {1 {f v 1}} [$replica exhsyncget h]

                $replica save
                catch {$replica shutdown nosave}
                restart_server 0 true false
                set replica [srv 0 client]
                $replica readonly
                tairhash_wait_converged $master $replica
                assert {[status $master sync_partial_ok] > 0}
                assert_equal {1 {f v 1}} [$replica exhsyncget h]

                $master exhsync g [$master exhsyncbegin g] f v 1
                wait_for_ofs_sync $master $replica
                assert_equal {1 {f v 1}} [$replica exhsyncget g]
            }
        }
    }
    set ::singledb $tairhash_singledb
}
