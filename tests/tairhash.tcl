set testmodule [file normalize your_path/tairhash_module.so]


start_server {tags {"tairhash"} overrides {bind 0.0.0.0}} {
    r module load $testmodule

    proc create_big_tairhash {key item} {
        r del $key
        for {set j 0} {$j < $item} {incr j} {
            r exhset $key $j $j
        }
    }

    proc create_big_tairhash_with_expire {key item expire} {
        r del $key
        for {set j 0} {$j < $item} {incr j} {
            r exhset $key $j $j ex $expire
        }
    }

    proc __consume_subscribe_messages {client type channels} {
        set numsub -1
        set counts {}

        for {set i [llength $channels]} {$i > 0} {incr i -1} {
            set msg [$client read]
            assert_equal $type [lindex $msg 0]

            # when receiving subscribe messages the channels names
            # are ordered. when receiving unsubscribe messages
            # they are unordered
            set idx [lsearch -exact $channels [lindex $msg 1]]
            if {[string match "*unsubscribe" $type]} {
                assert {$idx >= 0}
            } else {
                assert {$idx == 0}
            }
            set channels [lreplace $channels $idx $idx]

            # aggregate the subscription count to return to the caller
            lappend counts [lindex $msg 2]
        }

        # we should have received messages for channels
        assert {[llength $channels] == 0}
        return $counts
    }

    proc psubscribe {client channels} {
        $client psubscribe {*}$channels
        __consume_subscribe_messages $client psubscribe $channels
    }

    test {Exhset/exhget basic} {
        r del tairhashkey

        catch {r exhset tairhashkey field val xxxx} err
        assert_match {*ERR*syntax*error*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        catch {r exhset tairhashkey field} err
        assert_match {*ERR*wrong*number*of*arguments*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set new_field [r exhset tairhashkey field val]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val
    }

    test {Exhset/exhget NX/XX} {
        r del tairhashkey

        catch {r exhset tairhashkey field val XX NX} err
        assert_match {*ERR*syntax*error*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set new_field [r exhset tairhashkey field val]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        set ret_val [r exhset tairhashkey field val1 Nx]
        assert_equal -1 $ret_val

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        set ret_val [r exhset tairhashkey field_nx val Nx]
        assert_equal 1 $ret_val

        set ret_val [r exhget tairhashkey field_nx]
        assert_equal val $ret_val

        r del tairhashkey

        set ret_val [r exhset tairhashkey field_nx val Xx]
        assert_equal -1 $ret_val

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set ret_val [r exhset tairhashkey field_nx val1]
        assert_equal 1 $ret_val

        set ret_val [r exhget tairhashkey field_nx]
        assert_equal val1 $ret_val

        set ret_val [r exhset tairhashkey field_nx val2 XX]
        assert_equal 0 $ret_val

        set ret_val [r exhget tairhashkey field_nx]
        assert_equal val2 $ret_val
    }

    test {Exhset/exhget EX/EXAT/PX/PXAT with active expire } {
        r del tairhashkey

        catch {r exhset tairhashkey field val EX noint} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhset tairhashkey field val EX -1} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhset tairhashkey field val PX noint} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhset tairhashkey field val EXAT noint} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhset tairhashkey field val PXAT noint} err
        assert_match {*ERR*syntax*error*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        catch {r exhset tairhashkey field val EX 3 EXAT 10} err
        assert_match {*ERR*syntax*error*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set new_field [r exhset tairhashkey field val EX 1]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        set new_field [r exhset tairhashkey field val2 EX 2]
        assert_equal 0 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val2 $ret_val

        after 3000

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set ret_val [r exhget tairhashkey field]
        assert_equal "" $ret_val

        set new_field [r exhset tairhashkey field val PX 1000]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        set new_field [r exhset tairhashkey field val2 PX 2000]
        assert_equal 0 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val2 $ret_val

        after 1000

        set ret_val [r exists tairhashkey]
        assert_equal 1 $ret_val

        set ret_val [r exhget tairhashkey field]
        assert_equal val2 $ret_val

        after 2000

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set ret_val [r exhget tairhashkey field]
        assert_equal "" $ret_val
    }

    test {Exhset/exhget EX/EXAT/PX/PXAT with same time } {
        r del tairhashkey

        set abtime [expr [clock seconds] + 3]
        assert_equal 1 [r exhset tairhashkey field1 val EXAT $abtime]
        assert_equal 1 [r exhdel tairhashkey field1 ]
        assert_equal 1 [r exhset tairhashkey field1 val EXAT $abtime]
        assert_equal 1 [r exhset tairhashkey field2 val EXAT $abtime]
        assert_equal 1 [r exhdel tairhashkey field2 ]
        assert_equal 1 [r exhset tairhashkey field2 val EXAT $abtime]
        assert_equal 1 [r exhset tairhashkey field3 val EXAT $abtime]
        assert_equal 1 [r exhdel tairhashkey field3 ]
        assert_equal 1 [r exhset tairhashkey field3 val EXAT $abtime]
        assert_equal 3 [r exhlen tairhashkey]
        after 4000
        assert_equal 0 [r exhlen tairhashkey]
    }

    test {Exhset/exhget VER/ABS } {
        r del tairhashkey

        catch {r exhset tairhashkey field val VER 3 ABS 10} err
        assert_match {*ERR*syntax*error*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set new_field [r exhset tairhashkey field val ver 99999]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        catch {r exhset tairhashkey field val2 ver 99999} err
        assert_match {*ERR*update*version*is*stale*} $err

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        set new_field [r exhset tairhashkey field val2 ver 1]
        assert_equal 0 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val2 $ret_val

        set new_field [r exhset tairhashkey field val2 ver 2]
        assert_equal 0 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val2 $ret_val

        set new_field [r exhset tairhashkey field val3 ABS 1]
        assert_equal 0 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val3 $ret_val

        set new_field [r exhset tairhashkey field val4 ver 1]
        assert_equal 0 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val4 $ret_val

        r del tairhashkey

        set new_field [r exhset tairhashkey field val ABS 9999]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        catch {r exhset tairhashkey field val2 ver 1} err
        assert_match {*ERR*update*version*is*stale*} $err

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        set new_field [r exhset tairhashkey field val2 ver 9999]
        assert_equal 0 $new_field

        set ret_val [r exhget tairhashkey field]
        assert_equal val2 $ret_val
    }

    test {Active expire basic} {
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 1]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        set new_field [r exhset tairhashkey field2 val EX 3]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field2]
        assert_equal val $ret_val

        after 1000

        set h_len [r exhlen tairhashkey]
        assert {$h_len <= 2 && $h_len >=1 }

        set ret_val [r exhget tairhashkey field1]
        assert_equal "" $ret_val

        after 3000

        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field3]
        assert_equal "" $ret_val

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val
    }

    test {Active expire in multi db} {
        r select 0
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 1]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        set new_field [r exhset tairhashkey field2 val EX 3]
        assert_equal 1 $new_field

        r select 9
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 1]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        set new_field [r exhset tairhashkey field2 val EX 3]
        assert_equal 1 $new_field

        r select 15
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 1]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        set new_field [r exhset tairhashkey field2 val EX 3]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field2]
        assert_equal val $ret_val

        after 2000
        r select 0

        set h_len [r exhlen tairhashkey]
        assert {$h_len <= 2 && $h_len >=1 }

        set ret_val [r exhget tairhashkey field1]
        assert_equal "" $ret_val

        r select 9
        set h_len [r exhlen tairhashkey]
        assert {$h_len <= 2 && $h_len >=1 }

        set ret_val [r exhget tairhashkey field1]
        assert_equal "" $ret_val

        r select 15
        set h_len [r exhlen tairhashkey]
        assert {$h_len <= 2 && $h_len >=1 }

        set ret_val [r exhget tairhashkey field1]
        assert_equal "" $ret_val

        after 3000

        r select 0
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field3]
        assert_equal "" $ret_val

        r select 9
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field3]
        assert_equal "" $ret_val

        r select 15
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field3]
        assert_equal "" $ret_val
    }

    test {Active expire with repeat feilds} {
        r select 8
        r del tairhashkey
        r exhset tairhashkey f v ex 1
        r exhset tairhashkey f v ex 1
        r exhset tairhashkey f v ex 1

        assert_equal 1 [r exhlen tairhashkey]

        after 2000

        assert_equal 0 [r exhlen tairhashkey]
        assert_equal 0 [r exists tairhashkey]
        set info [r exhexpireinfo]
        assert { [string match "*db: 8, active_expired_fields: 1*" $info] }
    }

    test {Async flushall/flushdb/unlink} {
        create_big_tairhash_with_expire k1 10000 100
        create_big_tairhash_with_expire k2 10000 100
        create_big_tairhash_with_expire k3 10000 100

        r flushall async 

        assert_equal 0 [r dbsize]

        r select 8
        create_big_tairhash_with_expire k1 100000 100
        create_big_tairhash_with_expire k2 100000 100
        create_big_tairhash_with_expire k3 100000 100

        r flushdb async

        assert_equal 0 [r dbsize]

        create_big_tairhash_with_expire k1 100000 100
        create_big_tairhash_with_expire k2 100000 100
        create_big_tairhash_with_expire k3 100000 100

        r unlink k1
        r unlink k2
        r unlink k3

        assert_equal 0 [r dbsize]
    }

    test {Rename with active expire} {
        r flushall
        create_big_tairhash_with_expire exk1 10 2
        create_big_tairhash_with_expire exk2 10 2
        create_big_tairhash_with_expire exk3 10 2

        assert_equal 3 [r dbsize]

        r rename exk1 exk1_1
        r rename exk2 exk2_2

        after 3000
        assert_equal 0 [r dbsize]   
    }

    test {Move with active expire} {
        r select 8
        r flushall
        create_big_tairhash_with_expire exk1 10 2
        create_big_tairhash_with_expire exk2 10 2
        create_big_tairhash_with_expire exk3 10 2

        assert_equal 3 [r dbsize]

        r move exk1 9
        r move exk2 9

        assert_equal 1 [r dbsize]

        r select 9
        assert_equal 2 [r dbsize]

        after 3000
        assert_equal 0 [r dbsize]  
        r select 8 
        assert_equal 0 [r dbsize]  
    }

    test {Active expire rdb} {
        r del tairhashkey

        r select 0
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 3]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r select 9
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 3]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r select 15
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 3]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val
        
        r bgsave
        waitForBgsave r
        r debug reload

        r select 0
        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r select 9
        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r select 15
        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        after 4000
        r select 0
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field1]
        assert_equal "" $ret_val

        r select 9
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field1]
        assert_equal "" $ret_val

        r select 15
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field1]
        assert_equal "" $ret_val
    }

    # test {Swapdb in rdb save and load} {
    #     r select 7
    #     r del tairhashkey
    #     create_big_tairhash_with_expire tairhashkey 10 2
        
    #     r swapdb 7 13
    #     r swapdb 13 14 

    #     r select 14
    #     assert_equal 1 [r dbsize]

    #     r select 7
    #     assert_equal 0 [r dbsize]

    #     r bgsave
    #     waitForBgsave r
    #     r debug reload

    #     r select 14
    #     assert_equal 1 [r dbsize]

    #     after 3000

    #     assert_equal 0 [r dbsize]
    # }

    test {Active expire aof} {
        r config set aof-use-rdb-preamble no

        r select 0
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 3]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r select 9
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 3]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r select 15
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val EX 3]
        assert_equal 1 $new_field

        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof

        r select 0
        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r select 9
        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        r select 15
        set ret_val [r exhget tairhashkey field1]
        assert_equal val $ret_val

        after 4000

        r select 0
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field3]
        assert_equal "" $ret_val

        r select 9
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field3]
        assert_equal "" $ret_val

        r select 15
        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set ret_val [r exhget tairhashkey field3]
        assert_equal "" $ret_val

    }

    test {Exhlen} {
        r del tairhashkey

        set h_len [r exhlen tairhashkey]
        assert_equal 0 $h_len

        set new_field [r exhset tairhashkey field1 val]
        assert_equal 1 $new_field

        set new_field [r exhset tairhashkey field1 val2]
        assert_equal 0 $new_field

        set h_len [r exhlen tairhashkey]
        assert_equal 1 $h_len

        set new_field [r exhset tairhashkey field2 val]
        assert_equal 1 $new_field

        set h_len [r exhlen tairhashkey]
        assert_equal 2 $h_len

        set new_field [r exhset tairhashkey field3 val PX 100]
        assert_equal 1 $new_field

        after 100

        set h_len [r exhlen tairhashkey]
        assert_equal 3 $h_len
    }

    test {Exhdelwithver} {
        r del tairhashkey

        set new_field [r exhset tairhashkey field1 val]
        assert_equal 1 $new_field

        set new_field [r exhset tairhashkey field2 val]
        assert_equal 1 $new_field

        set new_field [r exhset tairhashkey field3 val]
        assert_equal 1 $new_field
        set del_num [r exhdelwithver tairhashkey field1 0 field2 1 field3 2]
        assert_equal 2 $del_num

        assert_equal 1 [r exhexists tairhashkey field3]
    }

    test {Exhexists} {
        r del tairhashkey

        set exist_num [r exhexists tairhashkey field]
        assert_equal 0 $exist_num

        set new_field [r exhset tairhashkey field1 val]
        assert_equal 1 $new_field

        set exist_num [r exhexists tairhashkey field1]
        assert_equal 1 $exist_num

        set new_field [r exhset tairhashkey field2 val PX 100]
        assert_equal 1 $new_field

        after 200

        set exist_num [r exhexists tairhashkey field2]
        assert_equal 0 $exist_num
    }

    test {Exhstrlen} {
        r del tairhashkey

        set str_len [r exhstrlen tairhashkey field2]
        assert_equal 0 $str_len

        set new_field [r exhset tairhashkey field val]
        assert_equal 1 $new_field

        set str_len [r exhstrlen tairhashkey field]
        assert_equal 3 $str_len

        set str_len [r exhstrlen tairhashkey field2]
        assert_equal 0 $str_len
    }

    test {Exhincrby} {
        r del tairhashkey

        catch {r exhincrby tairhashkey field} err
        assert_match {*ERR*wrong*number*of*arguments*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        catch {r exhincrby tairhashkey field 3 XX} err
        assert_match {*ERR*syntax*error*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        catch {r exhincrby tairhashkey field val} err
        assert_match {*ERR*value*is*not*an*integer*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set incr_val 9
        set new_cnt [r exhincrby tairhashkey field $incr_val]
        assert_equal $incr_val $new_cnt

        set new_field [r exhset tairhashkey field2 val]
        assert_equal 1 $new_field

        catch {r exhincrby tairhashkey field2 $incr_val} err
        assert_match {*ERR*value*is*not*an*integer*} $err
    }

    test {Exhincrby with boundary} {
        r del tairhashkey

        catch {r exhincrby tairhashkey f1 1 max } e
        assert_match {*ERR*syntax*error*} $e

        catch {r exhincrby tairhashkey f1 1 max xxx} e
        assert_match {ERR*not an integer*} $e

        catch {r exhincrby tairhashkey f2 1 min xxx} e
        assert_match {ERR*not an integer*} $e

        assert_equal 0 [r exhexists tairhashkey f1]
        assert_equal 0 [r exhexists tairhashkey f2]

        catch {r exhincrby tairhashkey f1 1 min 10 max 1} e
        assert_match {ERR*min value is bigger than max*} $e
        assert_equal 0 [r exhexists hash f1]

        catch {r exhincrby tairhashkey f1 11 min 1 max 10} e
        assert_match {ERR*increment or decrement would overflow*} $e
        assert_equal 0 [r exhexists hash f1]

        assert_equal 3 [r exhincrby tairhashkey f1 3 min 1 max 10]
        assert_equal -5 [r exhincrby tairhashkey f1 -8 min -5 max 10]
    }

    test {Exhincrbyfloat} {
        r del tairhashkey

        catch {r exhincrbyfloat tairhashkey field val} err
        assert_match {*ERR*value*is*not*an*float*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        catch {r exhincrbyfloat tairhashkey field 3.0 XX} err
        assert_match {*ERR*syntax*error*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        set float_incr_val 0.9
        set new_cnt [r exhincrbyfloat tairhashkey field $float_incr_val]
        assert_equal $float_incr_val $new_cnt

        set new_field [r exhset tairhashkey field2 val]
        assert_equal 1 $new_field

        catch {r exhincrbyfloat tairhashkey field2 $float_incr_val} err
        assert_match {*ERR*value*is*not*an*float*} $err
    }

    test {Exhincrbyfloat with boundary} {
        r del tairhashkey

        catch {r exhincrbyfloat tairhashkey f1 1.1 max } e
        assert_match {*ERR*syntax*error*} $e

        catch {r exhincrbyfloat tairhashkey f1 1.1 max xxx} e
        assert_match {ERR*not a float*} $e

        catch {r exhincrbyfloat tairhashkey f2 1.1 min xxx} e
        assert_match {ERR*not a float*} $e

        assert_equal 0 [r exhexists tairhashkey f1]
        assert_equal 0 [r exhexists tairhashkey f2]

        catch {r exhincrbyfloat tairhashkey f1 1.1 min 10.1 max 1.1} e
        assert_match {ERR*min value is bigger than max*} $e
        assert_equal 0 [r exhexists hash f1]

        catch {r exhincrbyfloat tairhashkey f1 11.1 min 1.1 max 10.1} e
        assert_match {ERR*increment or decrement would overflow*} $e
        assert_equal 0 [r exhexists hash f1]

        assert_equal 3.1 [r exhincrbyfloat tairhashkey f1 3.1 min 1.1 max 10.1]
        assert_equal -4.9 [r exhincrbyfloat tairhashkey f1 -8.0 min -5.1 max 10.1]
    }

    test {Exhincrby with params} {
        r del tairhashkey

        catch {r exhincrby tairhashkey field 2 VER -1} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrby tairhashkey field 2 ABS 0} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrby tairhashkey field 2 EX -2} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrby tairhashkey field 2 EX 2 EXAT 2} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrby tairhashkey field 2 PX 2 PXAT 2} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrby tairhashkey field 2 EX 2 PX 2} err
        assert_match {*ERR*syntax*error*} $err

        assert_equal 1 [r exhincrby tairhashkey field 1]
        assert_equal 1 [r exhver tairhashkey field ]
        catch {r exhincrby tairhashkey field 1 VER 2} err
        assert_match {*ERR*update*version*is*stale*} $err
        assert_equal 1 [r exhget tairhashkey field ]
        assert_equal 2 [r exhincrby tairhashkey field 1 VER 0]
        assert_equal 2 [r exhver tairhashkey field ]
        assert_equal 3 [r exhincrby tairhashkey field 1 VER 2]
        assert_equal 4 [r exhincrby tairhashkey field 1 ABS 10]
        assert_equal 10 [r exhver tairhashkey field ]
        assert_equal 5 [r exhincrby tairhashkey field 1 VER 10 EX 1]
        assert_equal 11 [r exhver tairhashkey field ]
        after 2000
        assert_equal 0 [r exhexists tairhashkey field]

        assert_equal 1 [r exhincrby tairhashkey field 1 EX 2]
        assert_equal 1 [r exhlen tairhashkey]
        after 4000
        assert_equal 0 [r exhlen tairhashkey]
    }

    test {Exhincrbyfloat with params} {
        r del tairhashkey

        catch {r exhincrbyfloat tairhashkey field 2 VER -1} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrbyfloat tairhashkey field 2 ABS 0} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrbyfloat tairhashkey field 2 EX -2} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrbyfloat tairhashkey field 2 EX 2 EXAT 2} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrbyfloat tairhashkey field 2 PX 2 PXAT 2} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrbyfloat tairhashkey field 2 EX 2 PX 2} err
        assert_match {*ERR*syntax*error*} $err

        assert_equal 1 [r exhincrbyfloat tairhashkey field 1]
        assert_equal 1 [r exhver tairhashkey field ]
        catch {r exhincrbyfloat tairhashkey field 1 VER 2} err
        assert_match {*ERR*update*version*is*stale*} $err
        assert_equal 1 [r exhget tairhashkey field ]
        assert_equal 2 [r exhincrbyfloat tairhashkey field 1 VER 0]
        assert_equal 2 [r exhver tairhashkey field ]
        assert_equal 3 [r exhincrbyfloat tairhashkey field 1 VER 2]
        assert_equal 4 [r exhincrbyfloat tairhashkey field 1 ABS 10]
        assert_equal 10 [r exhver tairhashkey field ]
        assert_equal 5 [r exhincrbyfloat tairhashkey field 1 VER 10 EX 1]
        assert_equal 11 [r exhver tairhashkey field ]
        after 2000
        assert_equal 0 [r exhexists tairhashkey field]

        assert_equal 1 [r exhincrbyfloat tairhashkey field 1 EX 2]
        assert_equal 1 [r exhlen tairhashkey]
        after 4000
        assert_equal 0 [r exhlen tairhashkey]
    }

    test {EXHINCRBY against non existing database key} {
        r del exhtest
        list [r exhincrby exhtest foo 2]
    } {2}

    test {EXHINCRBY against non existing hash key} {
        set rv {}
        r exhdel smallhash tmp
        r exhdel bighash tmp
        lappend rv [r exhincrby smallhash tmp 2]
        lappend rv [r exhget smallhash tmp]
        lappend rv [r exhincrby bighash tmp 2]
        lappend rv [r exhget bighash tmp]
    } {2 2 2 2}

    test {EXHINCRBY against hash key created by exhincrby itself} {
        set rv {}
        lappend rv [r exhincrby smallhash tmp 3]
        lappend rv [r exhget smallhash tmp]
        lappend rv [r exhincrby bighash tmp 3]
        lappend rv [r exhget bighash tmp]
    } {5 5 5 5}

    test {EXHINCRBY against hash key originally set with EXHSET} {
        r exhset smallhash tmp 100
        r exhset bighash tmp 100
        list [r exhincrby smallhash tmp 2] [r exhincrby bighash tmp 2]
    } {102 102}

    test {EXHINCRBY over 32bit value} {
        r exhset smallhash tmp 17179869184
        r exhset bighash tmp 17179869184
        list [r exhincrby smallhash tmp 1] [r exhincrby bighash tmp 1]
    } {17179869185 17179869185}

    test {EXHINCRBY over 32bit value with over 32bit increment} {
        r exhset smallhash tmp 17179869184
        r exhset bighash tmp 17179869184
        list [r exhincrby smallhash tmp 17179869184] [r exhincrby bighash tmp 17179869184]
    } {34359738368 34359738368}

    test {EXHINCRBY fails against hash value with spaces (left)} {
        r exhset smallhash str " 11"
        r exhset bighash str " 11"
        catch {r exhincrby smallhash str 1} smallerr
        catch {r exhincrby smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not an integer*" $smallerr]
        lappend rv [string match "ERR*not an integer*" $bigerr]
    } {1 1}

    test {EXHINCRBY fails against hash value with spaces (right)} {
        r exhset smallhash str "11 "
        r exhset bighash str "11 "
        catch {r exhincrby smallhash str 1} smallerr
        catch {r exhincrby smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not an integer*" $smallerr]
        lappend rv [string match "ERR*not an integer*" $bigerr]
    } {1 1}

    test {EXHINCRBY can detect overflows} {
        set e {}
        r exhset hash n -9223372036854775484
        assert {[r exhincrby hash n -1] == -9223372036854775485}
        catch {r exhincrby hash n -10000} e
        set e
    } {*overflow*}

    test {EXHINCRBYFLOAT against non existing database key} {
        r del htest
        list [r exhincrbyfloat htest foo 2.5]
    } {2.5}

    test {EXHINCRBYFLOAT against non existing hash key} {
        set rv {}
        r exhdel smallhash tmp
        r exhdel bighash tmp
        lappend rv [roundFloat [r exhincrbyfloat smallhash tmp 2.5]]
        lappend rv [roundFloat [r exhget smallhash tmp]]
        lappend rv [roundFloat [r exhincrbyfloat bighash tmp 2.5]]
        lappend rv [roundFloat [r exhget bighash tmp]]
    } {2.5 2.5 2.5 2.5}

    test {EXHINCRBYFLOAT against hash key created by hincrby itself} {
        set rv {}
        lappend rv [roundFloat [r exhincrbyfloat smallhash tmp 3.5]]
        lappend rv [roundFloat [r exhget smallhash tmp]]
        lappend rv [roundFloat [r exhincrbyfloat bighash tmp 3.5]]
        lappend rv [roundFloat [r exhget bighash tmp]]
    } {6 6 6 6}

    test {EXHINCRBYFLOAT against hash key originally set with EXHSET} {
        r exhset smallhash tmp 100
        r exhset bighash tmp 100
        list [roundFloat [r exhincrbyfloat smallhash tmp 2.5]] \
            [roundFloat [r exhincrbyfloat bighash tmp 2.5]]
    } {102.5 102.5}

    test {EXHINCRBYFLOAT over 32bit value} {
        r exhset smallhash tmp 17179869184
        r exhset bighash tmp 17179869184
        list [r exhincrbyfloat smallhash tmp 1] \
            [r exhincrbyfloat bighash tmp 1]
    } {17179869185 17179869185}

    test {EXHINCRBYFLOAT over 32bit value with over 32bit increment} {
        r exhset smallhash tmp 17179869184
        r exhset bighash tmp 17179869184
        list [r exhincrbyfloat smallhash tmp 17179869184] \
            [r exhincrbyfloat bighash tmp 17179869184]
    } {34359738368 34359738368}

    test {EXHINCRBYFLOAT fails against hash value with spaces (left)} {
        r exhset smallhash str " 11"
        r exhset bighash str " 11"
        catch {r exhincrbyfloat smallhash str 1} smallerr
        catch {r exhincrbyfloat smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not*float*" $smallerr]
        lappend rv [string match "ERR*not*float*" $bigerr]
    } {1 1}

    test {EXHINCRBYFLOAT fails against hash value with spaces (right)} {
        r exhset smallhash str "11 "
        r exhset bighash str "11 "
        catch {r exhincrbyfloat smallhash str 1} smallerr
        catch {r exhincrbyfloat smallhash str 1} bigerr
        set rv {}
        lappend rv [string match "ERR*not*float*" $smallerr]
        lappend rv [string match "ERR*not*float*" $bigerr]
    } {1 1}

    test {Exhkeys / exhvals / exhgetall basic} {
        r del tairhashkey

        array set tairhashkeyh {}
        for {set i 0} {$i < 8} {incr i} {
            set key __avoid_collisions__[randstring 0 8 alpha]
            set val __avoid_collisions__[randstring 0 8 alpha]
            if {[info exists tairhashkey($key)]} {
                incr i -1
                continue
            }
            r exhset tairhashkey $key $val
            set tairhashkey($key) $val
        }

        assert_equal [lsort [r exhkeys tairhashkey]] [lsort [array names tairhashkey *]]

        set expected_vals {}
        foreach {k v} [array get tairhashkey] {
            lappend expected_vals $v
        }
        assert_equal [lsort [r exhvals tairhashkey]] [lsort $expected_vals]
        assert_equal [lsort [r exhgetall tairhashkey]] [lsort [array get tairhashkey]]
        assert_equal [] [r exhgetall tairhashkey_noexists]
    }

    test {Exhkeys / exhvals / exhgetall while expired fields exist} {
        r del tairhashkey
        assert_equal 1 [r exhset tairhashkey field1 val1 PX 100]
        assert_equal 1 [r exhset tairhashkey field2 val2 PX 200]
        assert_equal 1 [r exhset tairhashkey field3 val3]

        assert_equal [lsort [r exhvals tairhashkey]] [lsort {val1 val2 val3}]
        assert_equal [lsort [r exhkeys tairhashkey]] [lsort {field1 field2 field3}]
        assert_equal [lsort [r exhgetall tairhashkey]] [lsort {field1 val1 field2 val2 field3 val3}]

        after 300

        assert_equal [lsort [r exhvals tairhashkey]] [lsort {val3}]
        assert_equal [lsort [r exhkeys tairhashkey]] [lsort {field3}]
        assert_equal [lsort [r exhgetall tairhashkey]] [lsort {field3 val3}]
    }

    test {Exhmget} {
        r del tairhashkey
        assert_equal 1 [r exhset tairhashkey field1 val1]
        assert_equal 1 [r exhset tairhashkey field2 val2]
        set result [r exhmget tairhashkey field1 field2 field-not-exist]
        assert_equal $result {val1 val2 {}}
    }

    test {Exhmget with expire} {
        r del tairhashkey
        r flushall
        assert_equal 1 [r exhset tairhashkey field1 val1 PX 100]
        assert_equal 1 [r exhset tairhashkey field2 val2 PX 100]
        assert_equal 1 [r exhset tairhashkey field3 val3 PX 100]
        set result [r exhmget tairhashkey field1 field2 field3]
        assert_equal $result {val1 val2 val3}

        after 200
        set result [r exhmget tairhashkey field1 field2 field3]
        assert_equal $result {{} {} {}}

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val
    }

    test {Exhsetnx} {
        r del tairhashkey

        assert_equal 1 [r exhsetnx tairhashkey field1 val1]
        assert_equal 1 [r exhsetnx tairhashkey field2 val2]
        assert_equal 0 [r exhsetnx tairhashkey field2 val2] "err while hsetnx"
    }

    test {Exhmset} {
        r del tairhashkey

        catch {r exhmset tairhashkey field1 val1 field2} err
        assert_match {*ERR*wrong*number*of*arguments*} $err

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        assert_equal OK [r exhmset tairhashkey field1 val1 field2 val2]
        set result [r exhmget tairhashkey field1 field2 field-not-exist]
        assert_equal $result {val1 val2 {}}
    }

    test {Exhver} {
        r del tairhashkey

        assert_equal 1 [r exhset tairhashkey field val]
        assert_equal 1 [r exhver tairhashkey field]

        assert_equal 0 [r exhset tairhashkey field val]
        assert_equal 2 [r exhver tairhashkey field]

        assert_equal -1 [r exhver tairhashkey-not-exist field1]
        assert_equal -2 [r exhver tairhashkey field-not-exist]
    }

    test {Exhttl} {
        r del tairhashkey

        set ttl 100
        assert_equal 1 [r exhset tairhashkey field1 val1 EX $ttl]
        assert_equal $ttl [r exhttl tairhashkey field1]

        assert_equal 1 [r exhset tairhashkey field2 val2]
        assert_equal -1 [r exhttl tairhashkey field2]

        assert_equal -2 [r exhttl tairhashkey-not-exist field1]
        assert_equal -3 [r exhttl tairhashkey field-not-exist]
    }

    test {Exhver} {
        r del tairhashkey

        assert_equal 1 [r exhset tairhashkey field val]
        assert_equal 1 [r exhver tairhashkey field]

        assert_equal 1 [r exhsetver tairhashkey field 10]
        assert_equal 10 [r exhver tairhashkey field]

        assert_equal 0 [r exhset tairhashkey field val]
        assert_equal 11 [r exhver tairhashkey field]

        assert_equal 0 [r exhsetver tairhashkey-not-exist field1 10]
        assert_equal 0 [r exhsetver tairhashkey field-not-exist 10]

        assert_equal -1 [r exhver tairhashkey-not-exist field1]
        assert_equal -2 [r exhver tairhashkey field-not-exist]

        catch {r exhsetver tairhashkey field -1} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhsetver tairhashkey field 0} err
        assert_match {*ERR*syntax*error*} $err
    }

    test {Exhver when expire} {
        r del tairhashkey

        assert_equal 1 [r exhset tairhashkey field val PX 100]
        assert_equal 1 [r exhver tairhashkey field]

        after 2000

        set ret_val [r exists tairhashkey]
        assert_equal 0 $ret_val

        assert_equal -1 [r exhver tairhashkey field]

        r del tairhashkey

        assert_equal 1 [r exhset tairhashkey field val EX 2]
        assert_equal 1 [r exhver tairhashkey field]

        assert_equal 1 [r exhsetver tairhashkey field 10]
        assert_equal 10 [r exhver tairhashkey field]

        after 3000

        assert_equal 0 [r exhsetver tairhashkey field 10]
        assert_equal -1 [r exhver tairhashkey field]
    }

    test {Exhexpire/exhexpireat} {
        r del tairhashkey

        assert_equal 1 [r exhset tairhashkey field val]
        assert_equal -1 [r exhttl tairhashkey field]

        assert_equal 1 [r exhexpire tairhashkey field 100]
        assert_equal 100 [r exhttl tairhashkey field]
        assert_equal 1 [r exhexists tairhashkey field]

        assert_equal 1 [r exhexpireat tairhashkey field 100]
        assert_equal 0 [r exhexists tairhashkey field]
    }

    test {Exhexpire/exhexpireat with params} {
        r del tairhashkey

        assert_equal 1 [r exhset tairhashkey field val]
        assert_equal 1 [r exhver tairhashkey field ]
        catch {r exhexpire tairhashkey field 100 VER 2} err
        assert_match {*ERR*update*version*is*stale*} $err
        assert_equal -1 [r exhttl tairhashkey field]
        assert_equal 1 [r exhexpire tairhashkey field 100 VER 1]
        assert_equal 100 [r exhttl tairhashkey field]
        assert_equal 2 [r exhver tairhashkey field ]
        assert_equal 1 [r exhexpire tairhashkey field 200 VER 0]
        assert_equal 200 [r exhttl tairhashkey field]
    }

    # test {SwapDB with active expire} {
    #     r flushall
    #     r select 10
    #     create_big_tairhash_with_expire exk1 10 2
    #     create_big_tairhash_with_expire exk2 10 2
    #     create_big_tairhash_with_expire exk3 10 2

    #     assert_equal 3 [r dbsize]
    #     r swapdb 10 11
    #     assert_equal 0 [r dbsize]
    #     r select 11
    #     assert_equal 3 [r dbsize]
    #     after 3000
    #     assert_equal 0 [r dbsize]   

    #     set info [r exhexpireinfo]
    #     assert { [string match "*db: 11, active_expired_fields: 30*" $info] }

    #     r select 10
    #     create_big_tairhash_with_expire exk1 20 2
    #     create_big_tairhash_with_expire exk2 20 2
    #     create_big_tairhash_with_expire exk3 20 2

    #     r swapdb 10 11
    #     r swapdb 11 12
    #     r swapdb 12 10

    #     after 3000
    #     assert_equal 0 [r dbsize]   
    #     set info [r exhexpireinfo]
    #     assert { [string match "*db: 10, active_expired_fields: 60*db: 12, active_expired_fields: 30*" $info] }
    # }

    # If you are run in SORT_MODE mode ,you can uncomment the following lines
    # test {Copy with active expire} {
    #     r del tairhashkey
    #     r del tairhashkey_new
    #     assert_equal 1 [r exhset tairhashkey field1 val1 ex 2]
    #     assert_equal 1 [r exhset tairhashkey field2 val2 ex 1]

    #     assert_equal 1 [r copy tairhashkey tairhashkey_new]

    #     set slave_ttl [r exhttl tairhashkey_new field1]
    #     assert {$slave_ttl <= 2 && $slave_ttl > 0 }

    #     set slave_ttl [r exhttl tairhashkey_new field2]
    #     assert {$slave_ttl <= 1 && $slave_ttl > 0 }

    #     after 3000

    #     assert_equal 0 [r exists tairhashkey_new]
    # }

    test {Reload after Exhset } {
        r del tairhashkey
        set val exhsetfieldvalue
        assert_equal 1 [r exhset tairhashkey field $val]
        r debug reload
        assert_equal 1 [r exhexists tairhashkey field]
        assert_equal $val [r exhget tairhashkey field]
        assert_equal -1 [r exhttl tairhashkey field]
        assert_equal -1 [r exhttl tairhashkey field]
    }

    test {Reload after tairhash field expire } {
        r del tairhashkey
        set val exhsetfieldvalue
        assert_equal 1 [r exhset tairhashkey field $val EXAT 5]
        r debug reload
        assert_equal 0 [r exhexists tairhashkey field]
    }

    test {tairhash aof} {
        r del tairhashkey
        set new_field [r exhset tairhashkey field val EX 50]
        assert_equal 1 $new_field

        set new_field [r exhset tairhashkey field val EX 3]
        assert_equal 0 $new_field

        set new_field [r exhset tairhashkey field2 val2]
        assert_equal 1 $new_field

        set ret_ver [r exhver tairhashkey field]
        assert_equal 2 $ret_ver

        r bgrewriteaof
        waitForBgrewriteaof r
        r debug loadaof

        set ret_val [r exhget tairhashkey field]
        assert_equal val $ret_val

        set ret_ver [r exhver tairhashkey field]
        assert_equal 2 $ret_ver

        after 4000

        set ret_ver [r exhver tairhashkey field]
        assert_equal -2 $ret_ver

        set ret_ver [r exhver tairhashkey field2]
        assert_equal 1 $ret_ver
    }

    test {tairhash type} {
        r del tairhashkey

        set new_field [r exhset tairhashkey field val]
        assert_equal 1 $new_field

        set res [r type tairhashkey]
        assert_equal $res "tairhash-"
    }

    test {tairhash get when last field expired} {
        r del tairhashkey

        set new_field [r exhset tairhashkey field val PX 500]
        assert_equal 1 $new_field

        set ret [r exhget tairhashkey field]
        assert_equal val $ret

        after 1000

        set ret [r exhget tairhashkey field]
        assert_equal "" $ret

        # set ret [r exhdel tairhashkey field]
        # assert_equal 0 $ret

        set ret [r exhlen tairhashkey]
        assert_equal 0 $ret

        set res [r exists tairhashkey]
        assert_equal 0 $res "assert fail, tairhash still exist"
    }

    test {tairhash active expired} {
        r del tairhashkey

        set new_field [r exhset tairhashkey field val EX 1]
        assert_equal 1 $new_field

        set ret [r exhget tairhashkey field]
        assert_equal val $ret

        after 3000

        set res [r exists tairhashkey]
        assert_equal 0 $res "assert fail, tairhash still exist"
    }

    test {Exhpersist} {
        r del tairhashkey

        assert_equal 1 [r exhset tairhashkey field1 val1 EX 1]
        assert_equal 1 [r exhset tairhashkey field2 val2 EX 1]
        
        assert_equal 1 [r exhpersist tairhashkey field1]

        after 3000

        assert_equal 1 [r exhexists tairhashkey field1]
        assert_equal 0 [r exhexists tairhashkey field2]

        assert_equal 0 [r exhpersist tairhashkey field2]
    }

    test {Exhgetwithver} {
        r del tairhashkey

        set new_field [r exhset tairhashkey field val]
        assert_equal 1 $new_field

        set ret [r exhgetwithver tairhashkey field]
        assert_equal "val 1" $ret
    }

    test {Exhmgetwithver} {
        r del tairhashkey

        assert_equal 1 [r exhset tairhashkey field1 val1]
        assert_equal 1 [r exhset tairhashkey field2 val2]

        set result [r exhmgetwithver tairhashkey field1 field2 field-not-exist]
        assert_equal $result {{val1 1} {val2 1} {}}

        set result [r exhmgetwithver tairhashkey-not-exist field1 field2 field-not-exist]
        assert_equal $result {{} {} {}}
    }

    test {Exhmsetwithopts} {
        r del tairhashkey

        assert_equal OK [r exhmsetwithopts tairhashkey field1 val1 4 10 field2 val2 4 10]
        catch {r exhmsetwithopts tairhashkey field1 val1 4 10 field2 val2 4 10} err
        assert_match {*ERR*update*version*is*stale} $err
        assert_equal OK [r exhmsetwithopts tairhashkey field1 val1 1 1000 field2 val2 1 3]

        after 1000
        assert_equal "val1 2" [r exhgetwithver tairhashkey field1]
        assert_equal "val2 2" [r exhgetwithver tairhashkey field2]

        after 2200
        assert_equal "val1 2" [r exhgetwithver tairhashkey field1]
        assert_equal "" [r exhgetwithver tairhashkey field2]
    }

    test {Exhmsetwithopts active expire} {
        r del tairhashkey

        assert_equal OK [r exhmsetwithopts tairhashkey field1 val1 4 10 field2 val2 4 10]
        assert_equal OK [r exhmsetwithopts tairhashkey field1 val1 1 1000 field2 val2 1 2]

        after 1000
        assert_equal "val1 2" [r exhgetwithver tairhashkey field1]
        assert_equal "val2 2" [r exhgetwithver tairhashkey field2]

        after 3000
        assert_equal 1 [r exhlen tairhashkey]
    }

    test {tairhash set ignore version} {
        r del tairhashkey

        set new_field [r exhset tairhashkey field val]
        assert_equal 1 $new_field

        set ret [r exhgetwithver tairhashkey field]
        assert_equal "val 1" $ret

        catch {r exhset tairhashkey field val1 VER 10} err
        assert_match {*ERR*update*version*is*stale} $err

        set new_field [r exhset tairhashkey field val1 VER 0]
        assert_equal 0 $new_field

        set ret [r exhgetwithver tairhashkey field]
        assert_equal "val1 2" $ret
    }

    test {tairhash dump/restore} {
        r del tairhashkey

        set new_field [r exhset tairhashkey field val]
        assert_equal 1 $new_field

        set dump [r dump tairhashkey]
        r del tairhashkey

        assert_equal "OK" [r restore tairhashkey 0 $dump]

        set ret [r exhgetwithver tairhashkey field]
        assert_equal "val 1" $ret
    }

    test "EXHSCAN" {
        # Create the Hash
        r del tairhashkey
        set count 1000
        set elements {}
        for {set j 0} {$j < $count} {incr j} {
            lappend elements key:$j $j
        }
        r exhmset tairhashkey {*}$elements


        # Test HSCAN
        set cur 0
        set keys {}
        while 1 {
            set res [r exhscan tairhashkey $cur]
            set cur [lindex $res 0]
            set k [lindex $res 1]
            lappend keys {*}$k
            if {$cur == 0} break
        }

        set keys2 {}
        foreach {k v} $keys {
            assert {$k eq "key:$v"}
            lappend keys2 $k
        }

        set keys2 [lsort -unique $keys2]
        assert_equal $count [llength $keys2]
    }

    test "EXHSCAN with PATTERN" {
        r del tairhashkey
        r exhmset tairhashkey foo 1 fab 2 fiz 3 foobar 10 1 a 2 b 3 c 4 d
        set res [r exhscan tairhashkey 0 MATCH foo* COUNT 10000]
        lsort -unique [lindex $res 1]
    } {1 10 foo foobar}

     test {Exhset keepttl} {
        r del exhashkey

        catch {r exhset exhashkey field val ex 3 keepttl} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhset exhashkey field val keepttl ex 3} err
        assert_match {*ERR*syntax*error*} $err

        r del exhashkey

        assert_equal 1 [r exhset exhashkey field val ex 3]
        assert_equal 0 [r exhset exhashkey field val]
        assert_equal -1 [r exhttl exhashkey field]

        r del exhashkey
        
        assert_equal 1 [r exhset exhashkey field val ex 3]
        assert_equal 0 [r exhset exhashkey field val keepttl]
        set ttl [r exhttl exhashkey field]
        assert {$ttl > 1}
    }

    test {Exhincrby keepttl} {
        r del exhashkey

        catch {r exhincrby exhashkey field 1 ex 3 keepttl} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrby exhashkey field 1 keepttl ex 3} err
        assert_match {*ERR*syntax*error*} $err

        r del exhashkey

        assert_equal 1 [r exhincrby exhashkey field 1 ex 3]
        assert_equal 2 [r exhincrby exhashkey field 1]
        assert_equal -1 [r exhttl exhashkey field]

        r del exhashkey
        
        assert_equal 1 [r exhincrby exhashkey field 1 ex 3]
        assert_equal 2 [r exhincrby exhashkey field 1 keepttl]
        set ttl [r exhttl exhashkey field]
        assert {$ttl > 1}
    }

    test {Exhincrbyfloat keepttl} {
        r del exhashkey

        catch {r exhincrbyfloat exhashkey field 1 ex 3 keepttl} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhincrbyfloat exhashkey field 1 keepttl ex 3} err
        assert_match {*ERR*syntax*error*} $err

        r del exhashkey

        assert_equal 1 [r exhincrbyfloat exhashkey field 1 ex 3]
        assert_equal 2 [r exhincrbyfloat exhashkey field 1]
        assert_equal -1 [r exhttl exhashkey field]

        r del exhashkey
        
        assert_equal 1 [r exhincrbyfloat exhashkey field 1 ex 3]
        assert_equal 2 [r exhincrbyfloat exhashkey field 1 keepttl]
        set ttl [r exhttl exhashkey field]
        assert {$ttl > 1}
    }

    test {Exhash with expire 0} {
        r del exhashkey

        assert_equal 1 [r exhset exhashkey field val ex 3]
        assert_equal 1 [r exhexpire exhashkey field 0]
        assert_equal -3 [r exhttl exhashkey field]
        assert_equal 0 [r exists exhashkey]

        assert_equal 1 [r exhincrby exhashkey field 1 ex 3]
        assert_equal 2 [r exhincrby exhashkey field 1 ex 0]
        assert_equal -3 [r exhttl exhashkey field]
        assert_equal 0 [r exists exhashkey]

        assert_equal 1 [r exhincrbyfloat exhashkey field 1 ex 3]
        assert_equal 2 [r exhincrbyfloat exhashkey field 1 ex 0]
        assert_equal -3 [r exhttl exhashkey field]
        assert_equal 0 [r exists exhashkey]

        assert_equal 1 [r exhset exhashkey field 1 ex 3]
        assert_equal 0 [r exhset exhashkey field 1 ex 0]
        assert_equal -3 [r exhttl exhashkey field]
        assert_equal 0 [r exists exhashkey]
    }

    test {Exhash field expired event notify} {
       r select 0
       r del exhashkey
       r select 1
       r del exhashkey2
       
       set rd1 [redis_deferring_client]
       set rd2 [redis_deferring_client]

       assert_equal {1} [psubscribe $rd1 {tairhash*}]
       assert_equal {1} [psubscribe $rd2 {tairhash@1@exhashkey2*}]

       r select 0
       assert_equal 1 [r exhset exhashkey foo bar ex 1]

       r select 1
       assert_equal 1 [r exhset exhashkey2 foo bar ex 1]
       
       after 2000
       assert_equal {pmessage tairhash* tairhash@0@exhashkey__:expired foo} [$rd1 read]
       assert_equal {pmessage tairhash* tairhash@1@exhashkey2__:expired foo} [$rd1 read]
       assert_equal {pmessage tairhash@1@exhashkey2* tairhash@1@exhashkey2__:expired foo} [$rd2 read]
       $rd1 close
       $rd2 close
    }

    test {Exhash GT version} {
        r del exhashkey

        catch {r exhset tairhashkey field 1 abs 10 gt 11} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhset tairhashkey field 1 ver 0 gt 11} err
        assert_match {*ERR*syntax*error*} $err

        catch {r exhset tairhashkey field 1 ver 0 abs 11} err
        assert_match {*ERR*syntax*error*} $err

        assert_equal 1 [r exhset tairhashkey field 1 abs 10]

        catch {r exhset tairhashkey field 2 gt 9} err
        assert_match {*ERR*update*version*is*stale*} $err
        assert_equal 0 [r exhset tairhashkey field 2 gt 13]
        assert_equal 13 [r exhver  tairhashkey field]

        catch {r exhincrby tairhashkey field 2 gt 13} err
        assert_match {*ERR*update*version*is*stale*} $err
        assert_equal 4 [r exhincrby tairhashkey field 2 gt 14]
        assert_equal 14 [r exhver  tairhashkey field]

        catch {r exhexpire tairhashkey field 1 gt 14} err
        assert_match {*ERR*update*version*is*stale*} $err
        assert_equal 4 [r exhexpire tairhashkey field 1 gt 20]
        assert_equal 20 [r exhver tairhashkey field]

        after 1000

        assert_equal 1 [r exhget tairhashkey field]
        assert_equal -1 [r exhver tairhashkey field]
    }
    
    start_server {tags {"tairhash repl"} overrides {bind 0.0.0.0}} {
        r module load $testmodule
        set slave [srv 0 client]
        set slave_host [srv 0 host]
        set slave_port [srv 0 port]
        set slave_log [srv 0 stdout]

        start_server { overrides {bind 0.0.0.0}} {
            r module load $testmodule
            set master [srv 0 client]
            set master_host [srv 0 host]
            set master_port [srv 0 port]

            $slave slaveof $master_host $master_port

            wait_for_condition 50 100 {
                [lindex [$slave role] 0] eq {slave} &&
                [string match {*master_link_status:up*} [$slave info replication]]
            } else {
                fail "Can't turn the instance into a replica"
            }

            test {Exhset/exhget master-slave} {
                $master del tairhashkey

                set new_field [$master exhset tairhashkey field val EX 2]
                assert_equal 1 $new_field

                set ret_val [$master exhget tairhashkey field]
                assert_equal val $ret_val

                $master WAIT 1 5000

                set ret_val [$slave exhget tairhashkey field]
                assert_equal val $ret_val

                after 4000

                set ret_val [$slave exhget tairhashkey field]
                assert_equal "" $ret_val

                set ret_val [$master exhget tairhashkey field]
                assert_equal "" $ret_val
            }

            test {Exhexpire/exhexpireat master-slave} {
                $master del tairhashkey

                assert_equal 1 [$master exhset tairhashkey field val]
                $master WAIT 1 5000
                assert_equal -1 [$slave exhttl tairhashkey field]

                assert_equal 1 [$master exhexpire tairhashkey field 100]

                $master WAIT 1 5000

                set slave_ttl [$slave exhttl tairhashkey field]
                assert {$slave_ttl <= 100 && $slave_ttl >=90 }
                assert_equal 1 [$slave exhexists tairhashkey field]
            }

            test {Exhsetver master-slave} {
                $master del tairhashkey

                assert_equal 1 [$master exhset tairhashkey field val]
                $master WAIT 1 5000
                assert_equal 1 [$slave exhver tairhashkey field]

                assert_equal 1 [$master exhsetver tairhashkey field 10]
                $master WAIT 1 5000
                assert_equal 10 [$slave exhver tairhashkey field]
            }

            test {Exhgetall master-slave} {
                $master del tairhashkey

                assert_equal 1 [$master exhset tairhashkey field1 val1 PX 100]
                assert_equal 1 [$master exhset tairhashkey field2 val2 PX 200]
                assert_equal 1 [$master exhset tairhashkey field3 val3]

                $master WAIT 1 5000

                assert_equal [lsort [$slave exhvals tairhashkey]] [lsort {val1 val2 val3}]
                assert_equal [lsort [$slave exhkeys tairhashkey]] [lsort {field1 field2 field3}]
                assert_equal [lsort [$slave exhgetall tairhashkey]] [lsort {field1 val1 field2 val2 field3 val3}]

                after 500

                assert_equal [lsort [$slave exhvals tairhashkey]] [lsort {val3}]
                assert_equal [lsort [$slave exhkeys tairhashkey]] [lsort {field3}]
                assert_equal [lsort [$slave exhgetall tairhashkey]] [lsort {field3 val3}]
            }

            test {Active expire master-slave} {
                $master select 0
                $master del tairhashkey

                set new_field [$master exhset tairhashkey field1 val EX 1]
                assert_equal 1 $new_field

                set ret_val [$master exhget tairhashkey field1]
                assert_equal val $ret_val

                set new_field [$master exhset tairhashkey field2 val EX 3]
                assert_equal 1 $new_field

                set ret_val [$master exhget tairhashkey field2]
                assert_equal val $ret_val

                $master select 9
                $master del tairhashkey

                set new_field [$master exhset tairhashkey field1 val EX 1]
                assert_equal 1 $new_field

                set ret_val [$master exhget tairhashkey field1]
                assert_equal val $ret_val

                set new_field [$master exhset tairhashkey field2 val EX 3]
                assert_equal 1 $new_field

                set ret_val [$master exhget tairhashkey field2]
                assert_equal val $ret_val

                $master select 15
                $master del tairhashkey

                set new_field [$master exhset tairhashkey field1 val EX 1]
                assert_equal 1 $new_field

                set ret_val [$master exhget tairhashkey field1]
                assert_equal val $ret_val

                set new_field [$master exhset tairhashkey field2 val EX 3]
                assert_equal 1 $new_field

                set ret_val [$master exhget tairhashkey field2]
                assert_equal val $ret_val

                $master WAIT 1 5000

                after 2000
                $slave select 0

                set h_len [$slave exhlen tairhashkey]
                assert {$h_len <= 1 && $h_len >=0 }

                set ret_val [$slave exhget tairhashkey field1]
                assert_equal "" $ret_val

                $slave select 9
                set h_len [$slave exhlen tairhashkey]
                assert {$h_len <= 1 && $h_len >=0 }

                set ret_val [$slave exhget tairhashkey field1]
                assert_equal "" $ret_val

                $slave select 15
                set h_len [$slave exhlen tairhashkey]
                assert {$h_len <= 1 && $h_len >=0 }

                set ret_val [$slave exhget tairhashkey field1]
                assert_equal "" $ret_val

                after 3000

                $slave select 0
                set h_len [$slave exhlen tairhashkey]
                assert_equal 0 $h_len

                set ret_val [$slave exhget tairhashkey field2]
                assert_equal "" $ret_val

                set ret_val [r exists tairhashkey]
                assert_equal 0 $ret_val

                $slave select 9
                set h_len [$slave exhlen tairhashkey]
                assert_equal 0 $h_len

                set ret_val [$slave exhget tairhashkey field2]
                assert_equal "" $ret_val

                set ret_val [r exists tairhashkey]
                assert_equal 0 $ret_val

                $slave select 15
                set h_len [$slave exhlen tairhashkey]
                assert_equal 0 $h_len

                set ret_val [$slave exhget tairhashkey field2]
                assert_equal "" $ret_val

                set ret_val [r exists tairhashkey]
                assert_equal 0 $ret_val
            }

            test {Exhincrby master-slave} {
                $master del tairhashkey

                set incr_val 9
                set new_cnt [$master exhincrby tairhashkey field $incr_val]
                assert_equal $incr_val $new_cnt

                $master WAIT 1 5000

                set val [$slave exhget tairhashkey field]
                assert_equal $incr_val $val
            }

            test {Exhincrbyfloat master-slave} {
                $master del tairhashkey

                set float_incr_val 0.9
                set new_cnt [$master exhincrbyfloat tairhashkey field $float_incr_val]
                assert_equal $float_incr_val $new_cnt

                $master WAIT 1 5000

                set val [$slave exhget tairhashkey field]
                assert_equal $float_incr_val $val
            }

            test {Exhgetwithver master-slave} {
                $master del tairhashkey

                set new_field [$master exhset tairhashkey field val EX 2]
                assert_equal 1 $new_field

                set ret [$master exhgetwithver tairhashkey field]
                assert_equal "val 1" $ret

                $master WAIT 1 5000

                set ret [$slave exhgetwithver tairhashkey field]
                assert_equal "val 1" $ret

                after 3000

                set ret [$slave exhgetwithver tairhashkey field]
                assert_equal "" $ret
            }

            test {Exhdel} {
                $master del tairhashkey

                set new_field [$master exhset tairhashkey field1 val1]
                assert_equal 1 $new_field

                set new_field [$master exhset tairhashkey field2 val2]
                assert_equal 1 $new_field

                $master WAIT 1 5000

                set ret_val [$slave exhget tairhashkey field1]
                assert_equal val1 $ret_val

                set ret_val [$slave exhget tairhashkey field2]
                assert_equal val2 $ret_val

                set del_num [$master exhdel tairhashkey field1]
                assert_equal 1 $del_num

                $master WAIT 1 5000

                set ret_val [$slave exhget tairhashkey field1]
                assert_equal "" $ret_val

                set del_num [$master exhdel tairhashkey field1 field2 field3]
                assert_equal 1 $del_num

                $master WAIT 1 5000

                set ret_val [$slave exhget tairhashkey field2]
                assert_equal "" $ret_val

                set exist_num [$slave exists tairhashkey]
                assert_equal 0 $exist_num
            }

            test {Exhexists} {
                $master del tairhashkey

                set exist_num [$master exhexists tairhashkey field]
                assert_equal 0 $exist_num

                set exist_num [$slave exhexists tairhashkey field]
                assert_equal 0 $exist_num

                set new_field [$master exhset tairhashkey field1 val]
                assert_equal 1 $new_field

                set exist_num [$master exhexists tairhashkey field1]
                assert_equal 1 $exist_num

                $master WAIT 1 5000

                set exist_num [$slave exhexists tairhashkey field1]
                assert_equal 1 $exist_num

                set new_field [r exhset tairhashkey field2 val EX 2]
                assert_equal 1 $new_field

                $master WAIT 1 5000

                set exist_num [$slave exhexists tairhashkey field2]
                assert_equal 1 $exist_num

                after 3000

                set exist_num [$slave exhexists tairhashkey field2]
                assert_equal 0 $exist_num
            }

            # test {SwapDB with active expire in replica} {
            #     $master flushall
            #     $master select 10
            #     $master exhset exk1 f v ex 2
            #     $master exhset exk2 f v ex 2
            #     $master exhset exk3 f v ex 2

            #     assert_equal 3 [$master dbsize]
            #     $master swapdb 10 11
            #     assert_equal 0 [$master dbsize]
            #     $master select 11
            #     assert_equal 3 [$master dbsize]

            #     $master WAIT 1 5000

            #     $slave select 11
            #     assert_equal 3 [$slave dbsize]

            #     after 3000
            #     assert_equal 0 [$master dbsize]   
            #     assert_equal 0 [$slave dbsize]  
            # }
        }
    }
}