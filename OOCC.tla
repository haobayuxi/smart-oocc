------------------------------- MODULE OOCC ----------------------------- 
(***************************************************************************)
(* This specification describes the OOCC protocol *)
(* This specification describes only the safety properties of the          *)
(* protocol--that is, what is allowed to happen.        *)
(***************************************************************************)
CONSTANT RM \* The set of resource managers

\* the set of server ids
CONSTANT Server, Coordinator
\* the set of keys
CONSTANT Keys

CONSTANT Lease
\* key lock state
CONSTANT UnLocked, IntentionLocked, Locked, UnFetched, Fetched

\* msg type
CONSTANT UpdateAndLock, IntentionLock, Read, Release
\* tx state
CONSTANT ExecutionPhase, ValidationPhase, FastValidationPhase, CommitPhase, ReleasePhase, Abort, Start
\* tx type
CONSTANT ReadOnly, ReadWrite



VARIABLES
    time,
    tx_start_ts,
    value_state,
    value_version,
    tx_state,
    tx_read_set,
    tx_write_set,
    tx_validation_set,
    lock_time,
    msgs       
    consistency,   

value_vars == <<value_state, value_version>>
tx_vars == <<tx_state,tx_read_set,tx_write_set,tx_validation_set, tx_start_ts>>


\* Helper for Send and Reply. Given a message m and bag of messages, return a
\* new bag of messages with one more m in it.
WithMessage(m, msgs) == msgs (+) SetToBag({m})

\* Helper for Discard and Reply. Given a message m and bag of messages, return
\* a new bag of messages with one less m in it.
WithoutMessage(m, msgs) == msgs (-) SetToBag({m})

\* Add a message to the bag of messages.
Send(m) == messages' = WithMessage(m, messages)

\* Remove a message from the bag of messages. Used when a server is done
\* processing a message.
Discard(m) == messages' = WithoutMessage(m, messages)

InitTxVars == /\ tx_start_ts = [ i in Coordinator |-> 0] 
              /\ lock_time = [ i in Coordinator |-> 0] 
              /\ tx_state= [ i in Coordinator |-> Start]  
                /\ tx_read_set= [ i in Coordinator |-> EmptyBag] 
                /\ tx_write_set= [ i in Coordinator |-> EmptyBag] 
                /\ tx_validation_set= [ i in Coordinator |-> EmptyBag] 
InitValueVars == /\ value_state= [ i in Keys |-> UnLocked] 
                 /\value_version = [ i in Keys |-> 0] 
    


Init == /\ messages = EmptyBag
        /\ time = 0
        /\ InitTxVars
        /\ InitValueVars
        /\ consistency = TRUE

InitTx(i) ==
    /\ tx_read_set' = [tx_read_set EXCEPT ![i] = EmtpyBag]
    /\ tx_write_set' = [tx_write_set EXCEPT ![i] = EmtpyBag]
    /\ tx_validation_set' = [tx_validation_set EXCEPT ![i] = EmtpyBag]
    /\ tx_start_ts' = time
    /\ lock_time' = [lock_time EXCEPT ![i] = 0]
    /\ \A key in Keys : CHOOSE T in 1..2 : \/  /\ T = 1
                                            /\ tx_read_set' = [tx_read_set EXCEPT ![i][key]= [key |-> key,
                                                                                            version |-> 0,
                                                                                            executed |-> FALSE,
                                                                                            success |-> FALSE]]
                                        \/  /\ T = 2
                                            /\ tx_write_set' = [tx_write_set EXCEPT ![i][key] = [key |-> key,
                                                                                            executed |-> FALSE,
                                                                                            success |-> FALSE,
                                                                                            applied |-> FALSE]]
    /\ UNCHANGED <<time, value_vars, msgs,lock_time, consistency>>

ExecutionAndLocking(i) ==
    /\ tx_state[i] = ExecutionPhase
    /\ \A write in tx_write_set[i] :
        Send([msource |-> i,
               mvalue |-> write.key,
                mtype|-> IntentionLock])
    /\ \A read in tx_read_set[i] : Send([msource |-> i,
                                        mvalue |-> read.key,
                                        mtype|-> Read])
    /\ UNCHANGED <<value_vars, tx_read_set, tx_write_set, tx_validation_set, time, lock_time, consistency>>


Validate(i) ==
    /\ tx_state[i] = ValidationPhase
    /\ IF time - tx_start_ts[i] <= Lease
        THEN /\ \E read in tx_validation_set[i] : value_version[read.key] > read.version
             /\ consistency' = FALSE
        ELSE \A read in tx_read_set[i] : Send([msource |-> i,
                                        mvalue |-> read.key,
                                        mtype|-> Read])
    /\ UNCHANGED <<value_vars, tx_read_set, tx_write_set, tx_validation_set, time, lock_time>>

Commit(i) ==
    /\ tx_state[i] = CommitPhase
    /\ IF time - lock_time[i] >= lease
       THEN \A write in tx_write_set[i] :
                Send([msource |-> i,
                        mvalue |-> write.key,
                        mversion |-> time,
                        mtype|-> UpdateAndLock])
       ELSE UNCHANGED <<msgs>>
    /\ UNCHANGED <<value_vars, tx_read_set, tx_write_set, tx_validation_set, time, lock_time, consistency>>

Release(i) ==
    /\ tx_state[i] = ReleasePhase
    /\ \A write in tx_write_set[i] :
        Send([msource |-> i,
               mvalue |-> write.key,
                mtype|-> IntentionLock])
    /\ UNCHANGED <<value_vars, tx_read_set, tx_write_set, tx_validation_set, time, lock_time, consistency>>

Abort(i) ==
/\ tx_state[i] = Abort
    /\ \A write in tx_write_set[i] :
        Send([msource |-> i,
               mvalue |-> write.key,
                mtype|-> UnLocked])
    /\ UNCHANGED <<value_vars, tx_read_set, tx_write_set, tx_validation_set, time, lock_time, consistency>>

RDMAMsg(m) ==
    /\ Discard(m)
    /\ \/ /\ m.type = IntentionLock
          /\ \/ /\ value_state[m.mvalue] = UnLocked
                /\ value_state' = [value_state EXCEPT ![m.mvalue] = IntentionLocked]
                /\ tx_write_set' = [tx_write_set EXCEPT  ![m.msource][m.mvalue]= [key |-> key,
                                                                        executed |-> TRUE,
                                                                        success |-> TRUE,
                                                                        applied |-> FALSE]]
                /\ lock_time' = [lock_time EXCEPT ![m.source] = time]
             \/ /\ value_state[m.mvalue] /= UnLocked
                /\ tx_write_set' = [tx_write_set EXCEPT  ![m.msource][m.mvalue]= [key |-> key,
                                                                        executed |-> TRUE,
                                                                        success |-> FALSE,
                                                                        applied |-> FALSE]]
          /\ IF \A write in tx_write_set[m.mvalue] : write.executed = TRUE /\ \A read in tx_read_set[m.mvalue] : read.executed = TRUE
             THEN IF \A write in tx_write_set[m.mvalue] : write.success = TRUE /\ \A read in tx_read_set[m.mvalue] : read.executed = TRUE
                  THEN Validate(m.msource)
                  ELSE Abort(m.msource)
             ELSE TRUE
          /\ UNCHANGED <<tx_validation_set, tx_state, tx_read_set,value_version, time, consistency>>
       \/ /\ m.type = Read
          /\ \/ /\ value_state[m.mvalue] = UnLocked
                /\ value_state' = [value_state EXCEPT ![m.mvalue] = IntentionLocked]
                /\ tx_read_set' = [tx_read_set EXCEPT  ![m.msource][m.mvalue]= [key |-> key,
                                                                        executed |-> TRUE,
                                                                        version |-> value_version[m.mvalue],
                                                                        success |-> TRUE]]
             \/ /\ value_state[m.mvalue] /= UnLocked
                /\ tx_read_set' = [tx_read_set EXCEPT  ![m.msource][m.mvalue]= [key |-> key,
                                                                        executed |-> TRUE,
                                                                        version |-> 0,
                                                                        success |-> FALSE]]
          /\ IF \A write in tx_write_set[m.mvalue] : write.executed = TRUE /\ \A read in tx_read_set[m.mvalue] : read.executed = TRUE
             THEN IF \A write in tx_write_set[m.mvalue] : write.success = TRUE /\ \A read in tx_read_set[m.mvalue] : read.executed = TRUE
                  THEN Validate(m.msource)
                  ELSE Abort(m.msource)
             ELSE TRUE
          /\ UNCHANGED <<tx_validation_set, tx_state, tx_write_set,value_version, time, lock_time, consistency>>
        \/ /\ m.type = UpdateAndLock
           /\ value_state' = [value_state EXCEPT ![m.mvalue] = Locked]
           /\ value_version' = [value_version EXCEPT ![m.mvalue] = m.mversion]
           /\ tx_write_set' = [tx_write_set EXCEPT  ![m.msource][m.mvalue]= [key |-> key,
                                                                        executed |-> TRUE,
                                                                        success |-> TRUE,
                                                                        applied |-> TRUE]]
           /\ IF \A write in tx_write_set[m.mvalue] : write.applied = TRUE 
              THEN Release(m.msource)
              ELSE TRUE
           /\ UNCHANGED <<tx_validation_set, tx_state,tx_read_set, time, lock_time, consistency>>
        \/ /\ m.type = Release
           /\ value_state' = [value_state EXCEPT ![m.mvalue] = UnLocked]
           /\ InitTx(m.msource)
           /\ UNCHANGED <<tx_validation_set, tx_read_set,tx_write_set,tx_state,tx_read_set, time, lock_time, consistency>>

TimeAdvance() == 
    /\ time' = time + 1
    /\ UNCHANGED <<tx_vars,value_vars, consistency,lock_time>>

\* Defines how the variables may transition.
Next == \/ TimeAdvance
        \/ CHOOSE c \in Coordinator : InitTx(c)
        \/ CHOOSE m \in msgs : RDMAMsg(m)
        \/ CHOOSE c \in Coordinator : Commit(c)

\* The specification must start with the initial state and transition according
\* to Next.
Spec == Init /\ [][Next]_vars

Consistency == consistency
