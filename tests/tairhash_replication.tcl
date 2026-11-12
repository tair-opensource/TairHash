set testmodule [file normalize your_path/tairhash_module.so]

# The suite runs under both the Redis and the Valkey test harness; Valkey renamed redis_deferring_client.
if {[llength [info commands valkey_deferring_client]]} {
    interp alias {} tairhash_deferring_client {} valkey_deferring_client
} else {
    interp alias {} tairhash_deferring_client {} redis_deferring_client
}

# Major and minor version of the server under test (Valkey reports a compatible redis_version).
proc tairhash_server_version {} {
    lrange [split [s redis_version] .] 0 1
}

proc tairhash_server_at_least {major minor} {
    lassign [tairhash_server_version] ma mi
    expr {$ma > $major || ($ma == $major && $mi >= $minor)}
}

# The clock of a server in milliseconds; the argument is its client, r or a client handle.
proc tairhash_ms {args} {
    lassign [{*}$args time] sec usec
    expr {$sec * 1000 + $usec / 1000}
}

# Applies commands to the current server through its AOF, as if it had logged them itself:
# what only the replication stream or the AOF may carry. The AOF is off again afterwards.
proc tairhash_load_from_aof {cmds} {
    r config set appendonly yes
    wait_for_condition 100 100 {
        [s aof_rewrite_scheduled] == 0 && [s aof_rewrite_in_progress] == 0
    } else {
        fail "The AOF did not start"
    }
    # Makes the server write what it logged so far before the commands are added behind it.
    set fsync [lindex [r config get appendfsync] 1]
    r config set appendfsync always
    set dir [lindex [r config get dir] 1]
    set name [lindex [r config get appendfilename] 1]
    # Redis 5.0 does not report appendfilename; it is then the default.
    if {$name eq {}} {
        set name appendonly.aof
    }
    set path [file join $dir $name]
    # Redis 7.0 and later keep the AOF as several files in a directory and log to the last
    # incremental one.
    set parts [glob -nocomplain [file join $dir [lindex [r config get appenddirname] 1] *.incr.aof]]
    if {[llength $parts]} {
        set path [lindex [lsort -dictionary $parts] end]
    }
    set fd [open $path a]
    fconfigure $fd -translation binary
    foreach cmd $cmds {
        puts -nonewline $fd "*[llength $cmd]\r\n"
        foreach arg $cmd {
            puts -nonewline $fd "\$[string length $arg]\r\n$arg\r\n"
        }
    }
    close $fd
    r debug loadaof
    # The server does not know that its AOF grew, and would log over the commands.
    r config set appendonly no
    r config set appendfsync $fsync
}

# Sets what a server does about an error it replies to its master or while loading the AOF,
# where it has the option (Redis 7.0 and later, whose test suites make it a panic), and
# returns what it was set to.
proc tairhash_propagation_error_behavior {client behavior} {
    set old [lindex [$client config get propagation-error-behavior] 1]
    if {$old ne {}} {
        $client config set propagation-error-behavior $behavior
    }
    return $old
}

# Whether the log of the server at that level has a line matching the pattern.
proc tairhash_logged {level pattern} {
    set fd [open [srv $level stdout]]
    set log [read $fd]
    close $fd
    string match "*$pattern*" $log
}

# The commands a server propagated up to the SET of the marker key that ends them, read from
# its replication stream. What the server puts around them is left out: SELECT, pings, and
# MULTI/EXEC, which servers before Redis 7.0 add to every write of a module.
proc tairhash_propagated_until {stream marker} {
    set cmds {}
    set idle 0
    while {$idle < 5} {
        set cmd [read_from_replication_stream $stream]
        if {$cmd eq {}} {
            incr idle
            continue
        }
        set idle 0
        set name [string tolower [lindex $cmd 0]]
        if {$name in {select multi exec ping}} continue
        if {$name eq {set} && [lindex $cmd 1] eq $marker} {
            return $cmds
        }
        lappend cmds [lreplace $cmd 0 0 $name]
    }
    fail "the replication stream ended before its marker, after: $cmds"
}

start_server {tags {"tairhash replication"} overrides {bind 0.0.0.0}} {
    # Without active expire a field whose TTL passed stays in the hash until a command
    # comes across it.
    r module load $testmodule enable_active_expire 0

    test {MODULE LIST reports the module version} {
        set version {}
        foreach m [r module list] {
            if {[dict get $m name] eq {tairhash}} {
                set version [dict get $m ver]
            }
        }
        assert_equal 20100 $version
    }

    test {EXHSETNX starts a new field at version 1} {
        r del k
        assert_equal 1 [r exhsetnx k f v]
        assert_equal 1 [r exhver k f]
    }

    if {[tairhash_server_at_least 6 2]} {
        test {EXHMSETREPL and EXHMETAREPL are rejected from clients} {
            r del k
            assert_error {*replication stream*} {r exhmsetrepl k 1 f v 1 0}
            assert_equal 0 [r exists k]
            r exhset k f v
            assert_error {*replication stream*} {r exhmetarepl k 1 f 9 0}
            assert_equal 1 [r exhver k f]
        }
    }

    test {EXHMSETREPL and EXHMETAREPL reject a negative version or expire} {
        r flushall
        set on_error [tairhash_propagation_error_behavior r ignore]
        r exhset neg keep v
        tairhash_load_from_aof {
            {select 9}
            {exhmsetrepl neg 1 a v -1 0}
            {exhmsetrepl neg 1 b v 1 -1}
            {exhmsetrepl neg 1 c v 1 0 d v 1 -5}
            {exhmetarepl neg 1 keep -2 0}
            {exhmetarepl neg 1 keep 5 -1}
            {exhmsetrepl neg 1 last v 7 0}
        }
        # The last command shows that the ones before it were read.
        assert_equal {keep last} [lsort [r exhkeys neg]]
        assert_equal 7 [r exhver neg last]
        assert_equal 1 [r exhver neg keep]
        assert_equal -1 [r exhttl neg keep]
        tairhash_propagation_error_behavior r $on_error
    }

    test {The internal LWW commands reject a negative or out-of-range version, deadline, sequence or epoch} {
        r flushall
        set on_error [tairhash_propagation_error_behavior r ignore]
        # One above the highest write sequence a node accepts, LLONG_MAX / 2.
        set over 4611686018427387904
        tairhash_load_from_aof [list \
            {select 9} \
            {exhmsetrepl neg 2 a v 1 0 -1} \
            [list exhmsetrepl neg 2 b v 1 0 $over] \
            {exhtombrepl neg 1 c -5 4102444800000} \
            {exhtombrepl neg 1 d 1 -1} \
            {exhsyncrepl neg -1 0} \
            [list exhsyncrepl neg $over 0] \
            {exhsyncepoch 0} \
            [list exhsyncepoch $over] \
            {exhmsetrepl neg 2 last v 7 0 1} \
        ]
        # The last command shows that the ones before it were read.
        assert_equal {last} [r exhkeys neg]
        # No tombstone lends a new field its version.
        r exhset neg c v
        assert_equal 1 [r exhver neg c]
        foreach cmd {EXHMSETREPL EXHTOMBREPL EXHSYNCREPL EXHSYNCEPOCH} {
            assert {[tairhash_logged 0 "$cmd rejected, the write it carries is lost: ERR syntax error"]}
        }
        if {[tairhash_server_at_least 6 0]} {
            # Nothing rejected was observed as a sequence: tokens stay below the bound.
            assert {[r exhsyncbegin probe] < $over}
        }
        tairhash_propagation_error_behavior r $on_error
    }

    test {EXHMSETREPL and EXHMETAREPL on a key of another type are logged} {
        r flushall
        set on_error [tairhash_propagation_error_behavior r ignore]
        r set str x
        tairhash_load_from_aof {
            {select 9}
            {exhmsetrepl str 1 f v 1 0}
            {exhmetarepl str 1 f 1 0}
        }
        assert_equal x [r get str]
        assert {[tairhash_logged 0 {EXHMSETREPL rejected, the write it carries is lost: WRONGTYPE}]}
        assert {[tairhash_logged 0 {EXHMETAREPL rejected, the write it carries is lost: WRONGTYPE}]}
        tairhash_propagation_error_behavior r $on_error
    }

    test {Full reads leave out a field whose TTL EXHMSETREPL or EXHMETAREPL set and has passed} {
        r flushall
        r exhmset full a 1 b 2 keep 3
        # Until here no field of the hash has a TTL. Expire 1 is a time long past.
        tairhash_load_from_aof {
            {select 9}
            {exhmsetrepl full 1 a 1 2 1}
            {exhmetarepl full 1 b 2 1}
        }
        assert_equal keep [r exhkeys full]
        assert_equal 3 [r exhvals full]
        assert_equal {keep 3} [r exhgetall full]
        assert_equal {keep 3 1} [r exhgetallwithver full]
    }

    test {Full reads leave out a field whose TTL EXHMSETREPL layout 2 set and has passed} {
        r flushall
        r exhmset full a 1 keep 3
        tairhash_load_from_aof {
            {select 9}
            {exhmsetrepl full 2 a 1 2 1 1}
        }
        assert_equal keep [r exhkeys full]
        assert_equal 3 [r exhvals full]
        assert_equal {keep 3} [r exhgetall full]
        assert_equal {keep 3 1} [r exhgetallwithver full]
    }

    if {[tairhash_server_at_least 7 0]} {
        test {replication_format is a module config} {
            assert_equal {tairhash.replication_format state} [r config get tairhash.replication_format]
            r config set tairhash.replication_format compat
            assert_equal {tairhash.replication_format compat} [r config get tairhash.replication_format]
            assert_error {*} {r config set tairhash.replication_format bogus}
            r config set tairhash.replication_format state
        }
    }
}

foreach format {state compat} {
    start_server [list tags {"tairhash replication"} overrides {bind 0.0.0.0}] {
        r module load $testmodule replication_format $format

        test "Versions and TTLs survive DEBUG RELOAD ($format)" {
            r flushall
            r exhset ver f v abs 7
            r exhset ttl f v ex 1000
            set digest [r debug digest]
            r debug reload
            assert_equal $digest [r debug digest]
        }

        test "Versions and TTLs survive AOF replay and AOF rewrite ($format)" {
            r flushall
            r config set aof-use-rdb-preamble no
            r config set appendonly yes
            waitForBgrewriteaof r
            assert_equal ok [s aof_last_bgrewrite_status]

            # Replayed from the incremental AOF, i.e. the propagated effects.
            r exhsetnx nx f v
            r exhset ttl f v ex 1000
            r exhset ttl f v2 keepttl
            r exhincrby ttl n 1 ex 1000
            r exhset ttl p v ex 1000
            r exhpersist ttl p
            r exhsetver ttl f 42
            # More than one rewrite command's worth of fields in the state format.
            for {set i 0} {$i < 150} {incr i} {
                r exhset big f$i v$i
            }
            r exhset big ttl v px 100000
            set digest [r debug digest]
            r debug loadaof
            assert_equal $digest [r debug digest]

            # Replayed from the rewritten base, i.e. TairHashTypeAofRewrite.
            r bgrewriteaof
            waitForBgrewriteaof r
            assert_equal ok [s aof_last_bgrewrite_status]
            r debug loadaof
            assert_equal $digest [r debug digest]
            assert_equal 1 [r exhver nx f]
            assert {[r exhttl ttl f] > 0}
            assert_equal -1 [r exhttl ttl p]

            r config set appendonly no
            r config set aof-use-rdb-preamble yes
        }

        test "A TTL that EXHPEXPIRE extended survives AOF replay after the old TTL passed ($format)" {
            r flushall
            r config set appendonly yes
            waitForBgrewriteaof r
            # The first TTLs end together, late enough to change them before that on a slow host too.
            set first_ttl_end [expr {[tairhash_ms r] + 1000}]
            r exhset ex f v pxat $first_ttl_end
            r exhset ex g v pxat $first_ttl_end
            r exhset ex h v pxat $first_ttl_end
            set changed [list [r exhpexpire ex f 600000] [r exhexpireat ex g 4000000000 abs 7] [r exhpersist ex h]]
            wait_for_condition 50 100 {
                [tairhash_ms r] > $first_ttl_end
            } else {
                fail "The first TTL did not pass"
            }
            assert_equal {1 1 1} $changed
            set digest [r debug digest]
            r debug loadaof
            assert_equal $digest [r debug digest]
            assert_equal v [r exhget ex f]
            assert {[r exhpttl ex f] > 500000}
            assert_equal {v 7} [r exhgetwithver ex g]
            assert_equal -1 [r exhttl ex h]
            r config set appendonly no
        }

        # Only a server that tells the module where a command comes from applies EXHSETNX at
        # version 0.
        if {[tairhash_server_at_least 6 2]} {
            test "A field at version 0 survives AOF replay and AOF rewrite ($format)" {
                r flushall
                r config set aof-use-rdb-preamble no
                # What TairHash 1.x logs for EXHSETNX, which leaves the field at version 0 there.
                tairhash_load_from_aof {
                    {select 9}
                    {exhsetnx zero f v}
                }
                assert_equal 0 [r exhver zero f]
                assert_equal 1 [r exhsetnx zero g v]
                assert_equal 1 [r exhver zero g]
                set digest [r debug digest]

                # Written by TairHashTypeAofRewrite this time.
                r config set appendonly yes
                waitForBgrewriteaof r
                assert_equal ok [s aof_last_bgrewrite_status]
                r debug loadaof
                assert_equal 0 [r exhver zero f]
                assert_equal 1 [r exhver zero g]
                assert_equal $digest [r debug digest]

                r config set appendonly no
                r config set aof-use-rdb-preamble yes
            }
        }

        test "Every write command propagates its result in the $format format" {
            r flushall
            set repl [attach_to_replication_stream]
            r exhset w f v
            r exhset w f v2 pxat 4000000000000
            r exhset w f v3 keepttl
            r exhsetnx w nx v
            r exhmset w m1 a m2 b
            r exhmsetwithopts w o v 0 1000
            r exhincrby w n 5 exat 4000000000
            r exhincrbyfloat w x 1.5
            r exhexpire w m1 1000
            r exhexpireat w m1 4000000000
            r exhpexpire w m2 100000
            r exhpexpireat w m2 4000000000001 abs 9
            r exhpersist w m2
            r exhsetver w m2 42
            r exhdel w nx
            r exhdelwithver w o 1
            r set w:end 1
            set propagated [tairhash_propagated_until $repl w:end]
            close_replication_stream $repl

            # Where a command gave a relative TTL the expire is an absolute time in milliseconds
            # all the same. Each field carries its write sequence, a time in microseconds.
            set abs {?????????????}
            set seq {????????????????}
            if {$format eq {state}} {
                set expected [list \
                    "exhmsetrepl w 2 f v 1 0 $seq" \
                    "exhmsetrepl w 2 f v2 2 4000000000000 $seq" \
                    "exhmsetrepl w 2 f v3 3 4000000000000 $seq" \
                    "exhmsetrepl w 2 nx v 1 0 $seq" \
                    "exhmsetrepl w 2 m1 a 1 0 $seq m2 b 1 0 $seq" \
                    "exhmsetrepl w 2 o v 1 $abs $seq" \
                    "exhmsetrepl w 2 n 5 1 4000000000000 $seq" \
                    "exhmsetrepl w 2 x 1.5 1 0 $seq" \
                    "exhmetarepl w 1 m1 2 $abs" \
                    {exhmetarepl w 1 m1 3 4000000000000} \
                    "exhmetarepl w 1 m2 2 $abs" \
                    {exhmetarepl w 1 m2 9 4000000000001} \
                    {exhmetarepl w 1 m2 9 0} \
                    {exhmetarepl w 1 m2 42 0} \
                    {exhdel w nx} \
                    {exhdel w o}]
            } else {
                set expected [list \
                    {exhset w f v ABS 1} \
                    {exhset w f v2 ABS 2 PXAT 4000000000000} \
                    {exhset w f v3 ABS 3 PXAT 4000000000000} \
                    {exhset w nx v ABS 1} \
                    {exhset w m1 a ABS 1} \
                    {exhset w m2 b ABS 1} \
                    "exhset w o v ABS 1 PXAT $abs" \
                    {exhset w n 5 ABS 1 PXAT 4000000000000} \
                    {exhset w x 1.5 ABS 1} \
                    "exhset w m1 a ABS 2 PXAT $abs" \
                    {exhset w m1 a ABS 3 PXAT 4000000000000} \
                    "exhset w m2 b ABS 2 PXAT $abs" \
                    {exhset w m2 b ABS 9 PXAT 4000000000001} \
                    {exhset w m2 b ABS 9} \
                    {exhset w m2 b ABS 42} \
                    {exhdel w nx} \
                    {exhdel w o}]
            }
            assert_equal [llength $expected] [llength $propagated] $propagated
            foreach pattern $expected cmd $propagated {
                assert_match $pattern $cmd
            }
        }
    }

    start_server [list tags {"tairhash replication"} overrides {bind 0.0.0.0}] {
        # Without active expire a field whose TTL passed stays in the hash until a command
        # comes across it.
        r module load $testmodule replication_format $format enable_active_expire 0

        test "AOF rewrite keeps a field that expired but was not removed yet ($format)" {
            r flushall
            r config set aof-use-rdb-preamble no
            r config set appendonly yes
            waitForBgrewriteaof r
            r exhset late f v px 100
            wait_for_condition 50 100 {
                [r exhlen late noexp] == 0
            } else {
                fail "The field did not expire"
            }
            assert_equal 1 [r exhlen late]
            set digest [r debug digest]

            r bgrewriteaof
            waitForBgrewriteaof r
            assert_equal ok [s aof_last_bgrewrite_status]
            r debug loadaof
            assert_equal 1 [r exhlen late]
            assert_equal $digest [r debug digest]

            r config set appendonly no
            r config set aof-use-rdb-preamble yes
        }

        # rdb-key-save-delay slows down AOF rewrite too from Redis 7.0 on.
        if {[tairhash_server_at_least 7 0]} {
            test "A TTL extended while AOF rewrite runs survives a reload ($format)" {
                r flushall
                r config set aof-use-rdb-preamble no
                r config set appendonly yes
                waitForBgrewriteaof r
                # The rewrite goes through database 0 first and sleeps after its key, so it gets
                # to the hash, in database 9, only once the field's first TTL has passed.
                r select 0
                r set pad x
                r select 9
                r config set rdb-key-save-delay 1500000
                r exhset slow f v px 1000
                r bgrewriteaof
                set extended [r exhpexpire slow f 600000]
                r config set rdb-key-save-delay 0
                waitForBgrewriteaof r
                assert_equal ok [s aof_last_bgrewrite_status]
                assert_equal 1 $extended
                set digest [r debug digest]
                r debug loadaof
                assert_equal $digest [r debug digest]
                assert_equal {v 2} [r exhgetwithver slow f]
                assert {[r exhpttl slow f] > 500000}

                r config set appendonly no
                r config set aof-use-rdb-preamble yes
            }
        }
    }

    start_server [list tags {"tairhash replication repl"} overrides {bind 0.0.0.0}] {
        r module load $testmodule
        set replica [srv 0 client]

        start_server {overrides {bind 0.0.0.0}} {
            r module load $testmodule replication_format $format
            set master [srv 0 client]

            $replica replicaof [srv 0 host] [srv 0 port]
            wait_for_condition 50 100 {
                [lindex [$replica role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$replica info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }

            test "Replicas converge on the versions and TTLs of every write command ($format)" {
                $master flushall
                $master exhset ttl keep v ex 1000
                $master exhset ttl keep v2 keepttl
                $master exhincrby ttl n 1 ex 1000
                $master exhincrbyfloat ttl x 1.5 px 100000
                $master exhset ttl p v ex 1000
                $master exhpersist ttl p
                $master exhsetnx ttl nx v
                $master exhmset ttl m1 v m2 v
                $master exhmset ttl new v m1 v2
                $master exhmsetwithopts ttl o v 0 1000
                $master exhsetver ttl m1 42

                wait_for_ofs_sync $master $replica
                assert_equal [$master debug digest] [$replica debug digest]
                assert_equal -1 [$replica exhttl ttl p]
                assert {[$replica exhttl ttl keep] > 0}
                assert {[$replica exhttl ttl n] <= 1000}
                assert_equal 1 [$replica exhver ttl nx]
            }

            test "A TTL that EXHPEXPIRE extended reaches a replica that applies it late ($format)" {
                $master flushall
                wait_for_ofs_sync $master $replica
                # Stopped until the field's first TTL has passed, the replica applies both
                # writes only after it.
                set replica_pid [srv -1 pid]
                exec kill -SIGSTOP $replica_pid
                set first_ttl_end [expr {[tairhash_ms $master] + 1000}]
                $master exhset lag f v pxat $first_ttl_end
                set extended [$master exhpexpire lag f 600000]
                wait_for_condition 50 100 {
                    [tairhash_ms $master] > $first_ttl_end
                } else {
                    exec kill -SIGCONT $replica_pid
                    fail "The first TTL did not pass"
                }
                exec kill -SIGCONT $replica_pid
                assert_equal 1 $extended
                wait_for_ofs_sync $master $replica
                assert_equal [$master debug digest] [$replica debug digest]
                assert {[$replica exhpttl lag f] > 500000}
            }

            test "A write of more fields than one replicated command holds ($format)" {
                $master flushall
                set args {}
                for {set i 0} {$i < 9000} {incr i} {
                    lappend args f$i v$i
                }
                $master exhmset big {*}$args
                wait_for_ofs_sync $master $replica
                assert_equal 9000 [$replica exhlen big]
                assert_equal [$master debug digest] [$replica debug digest]
            }

            # Only the state format has a command that updates a field without carrying it.
            if {$format eq {state}} {
                test {A replica that lacks a field EXHMETAREPL updates replies an error to its master} {
                    $master flushall
                    set on_error [tairhash_propagation_error_behavior $replica ignore]
                    $master exhset div f v ex 1000
                    $master exhset div g v ex 1000
                    wait_for_ofs_sync $master $replica
                    # A write of its own makes the replica differ from its master.
                    $replica config set replica-read-only no
                    assert_equal 1 [$replica exhdel div f]
                    $replica config set replica-read-only yes
                    $master exhpexpire div f 600000
                    $master exhpexpire div g 600000
                    wait_for_ofs_sync $master $replica
                    assert {[$replica exhpttl div g] > 500000}
                    # Servers before Redis 6.2 neither log nor count an error a module replies to
                    # its master.
                    if {[tairhash_server_at_least 6 2]} {
                        assert {[tairhash_logged -1 {== CRITICAL ==*the field to update does not exist}]}
                    }
                    tairhash_propagation_error_behavior $replica $on_error
                }
            }
        }
    }
}

# Switching the format of a running server needs module configs (Redis 7.0 and later).
start_server {tags {"tairhash replication"} overrides {bind 0.0.0.0}} {
    if {[tairhash_server_at_least 7 0]} {
        r module load $testmodule replication_format compat

        test {TTLs and versions changed after switching from compat to state survive AOF replay} {
            r flushall
            # A field that is missing when the AOF is replayed then shows in the checks below
            # and not as a panic of the server.
            set on_error [tairhash_propagation_error_behavior r ignore]
            r config set appendonly yes
            waitForBgrewriteaof r
            set first_ttl_end [expr {[tairhash_ms r] + 1000}]
            r exhset mix f v pxat $first_ttl_end
            r exhset mix p v pxat $first_ttl_end
            r exhset mix s v pxat $first_ttl_end
            # Later writes in the compat format: replaying one runs passive expiry, on its key
            # in scan mode and on the database in sort mode.
            r exhset mix other v
            r exhset mix another v
            r config set tairhash.replication_format state
            set changed [list [r exhpexpire mix f 600000] [r exhpersist mix p] [r exhsetver mix s 42] [r exhpexpire mix s 600000]]
            wait_for_condition 50 100 {
                [tairhash_ms r] > $first_ttl_end
            } else {
                fail "The first TTL did not pass"
            }
            assert_equal {1 1 1 1} $changed
            set digest [r debug digest]
            r debug loadaof
            assert_equal $digest [r debug digest]
            assert {[r exhpttl mix f] > 500000}
            assert_equal -1 [r exhpttl mix p]
            assert_equal {v 43} [r exhgetwithver mix s]
            r config set appendonly no
            tairhash_propagation_error_behavior r $on_error
        }
    }
}
