---------------------------- MODULE TaskGraphSpecBase ----------------------------
\* TLA+ specification for the Boilermaker TaskGraph coordination protocol.
\*
\* Models the DAG-based task runner with:
\*   - Azure Blob Storage (per-task blobs with ETags for optimistic concurrency)
\*   - Azure Service Bus (at-least-once message delivery)
\*   - Multiple concurrent workers consuming from the same SB queue
\*   - A per-blob lease used by continue_graph for scheduling (publish-before-store)
\*
\* Corresponds to:
\*   boilermaker/evaluators/task_graph.py   (message_handler, continue_graph)
\*   boilermaker/app.py                     (publish_graph)
\*   boilermaker/task/graph.py              (TaskGraph, generate_ready_tasks, schedule_task)
\*   boilermaker/storage/blob_storage.py    (load_graph, store_task_result, load_task_result)
\*   boilermaker/storage/base.py            (try_acquire_lease, release_lease)
\*
\* Protocol summary:
\*
\* SCHEDULING (continue_graph):
\*   1. Load graph (read all blobs + etags)
\*   2. For each ready task (Pending + all antecedents Success):
\*      a. Acquire lease WITH etag precondition (fail if blob changed OR already leased)
\*      b. Publish SB message
\*      c. Write Scheduled (lease holder only)
\*      d. Release lease (always, in finally)
\*
\* WORKER (message_handler):
\*   1. Read blob (idempotency check), capture etag
\*      - If terminal: run continue_graph, complete message, return
\*   2. Write Started with ETag CAS
\*      - On success: proceed to execution
\*      - On 412: re-read blob, branch on status:
\*          None      -> complete message, return Failure (should not happen; always settle)
\*          terminal  -> complete message, return
\*          Started   -> complete message, return
\*          Scheduled -> RETRY Started write using re-read etag
\*          other     -> complete message, return Failure (always settle)
\*      - On non-412 error: proceed with execution (fail-open)
\*   2a. RETRY Started write (only entered from Scheduled re-read case):
\*      - CAS succeeds: blob -> Started, proceed to execution (normal path)
\*      - CAS 412 again: re-read a second time:
\*          Started   -> complete message, return (another worker won)
\*          terminal  -> complete message, return terminal result
\*          Pending/Scheduled -> abandon_message() for immediate redelivery, return Scheduled
\*      - Non-412 storage error: proceed with execution anyway (fail-open)
\*   3. Execute task (nondeterministic)
\*   4. Write result unconditionally
\*   5. Run continue_graph
\*   6. Complete (settle) message
\*
\* KEY INVARIANT: settle the SB message on the normal forward path. For the
\* double-412 Pending/Scheduled case, abandon the message for immediate SB
\* redelivery — this is the mechanism that prevents permanent stalls when the
\* blob is unclaimed after two consecutive CAS misses.
\*
\* RETRIES-EXHAUSTED ON RECEIVE:
\*   A task can arrive at the worker already retry-exhausted: record_attempt
\*   has pushed it past max_tries on a prior delivery, and the current
\*   delivery sees `not self.task.can_retry` at message_handler.py:498.  The
\*   handler must write RetriesExhausted to the blob, dispatch any failure
\*   callbacks via continue_graph, and only then settle the parent message
\*   (deadletter).  The corresponding code lives in:
\*       boilermaker/evaluators/task_graph.py:498-547   (the `not can_retry`
\*           branch of message_handler)
\*
\*   The model captures both the FIXED and BUGGY orderings of that branch:
\*     - CONSTANT BuggyRetriesExhaustedOrdering:
\*         FALSE -> NEW (fixed) ordering: write blob, run continue_graph,
\*                  settle (deadletter) ONLY if continue_graph succeeded;
\*                  on ContinueGraphError abandon for redelivery.
\*         TRUE  -> OLD (buggy) ordering: settle (deadletter) the parent
\*                  message FIRST, then write the blob and run
\*                  continue_graph.  If continue_graph then fails, the
\*                  parent message is already deadlettered and failure
\*                  callbacks are orphaned with no recovery path.
\*     - VARIABLE attemptsExhausted[t] \in BOOLEAN: set non-deterministically
\*       at Init and stays constant.  Indicates that any delivery of t
\*       arrives already retry-exhausted (TLC enumerates 2^|Tasks|
\*       assignments).
\*     - VARIABLE retriesExhaustedDispatched[t]: monotonically becomes TRUE
\*       when a worker reaches FinishAcquiringSuccess on a dispatch pass
\*       entered via RetriesExhasted-on-receive (worker flag
\*       workerRetriesExhaustedDispatch[w]).
\*     - VARIABLE buggyPreSettledParent[t]: TRUE iff the OLD-buggy
\*       pre-settle ran for t; distinguishes "deadlettered by the bug"
\*       from "deadlettered by lock-expiry exhaustion" so the safety
\*       invariant can detect the bug but ignore the orthogonal DLQ fault.
\*
\*   Properties:
\*     - SAFETY INVARIANT NoOrphanedRetriesExhausted: a task in
\*       RetriesExhausted whose SB message is permanently gone (settled by
\*       the bug, not by ordinary lock-expiry) must have its failure
\*       callbacks dispatched.  Holds on FALSE; violated on TRUE.
\*     - LIVENESS PROPERTY RetriesExhaustedCallbacksDispatched: absent
\*       dead-lettering, any task reaching RetriesExhausted eventually has
\*       its failure callbacks dispatched.  Holds on FALSE.

EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Edges,              \* Dependency edges: set of <<parent, child>> pairs
    MaxDeliveryCount,   \* SB maximum delivery count before dead-lettering
    Nil,                \* Sentinel value for "nothing"
    Workers,            \* Set of worker identifiers, e.g. {w1, w2}
    Tasks,              \* Set of all task IDs in the graph, e.g. {t1, t2, t3}
    RootTasks,          \* Set of tasks with no antecedents (initially ready)
    BuggyRetriesExhaustedOrdering   \* BOOLEAN: TRUE models OLD buggy ordering
                                    \* (settle-then-dispatch); FALSE models NEW
                                    \* fixed ordering (write-blob-then-dispatch-
                                    \* then-conditionally-settle).

\* ---------------------------------------------------------------------------
\* Derived helpers from the graph structure
\* ---------------------------------------------------------------------------

\* Antecedents(t) = set of tasks that t depends on
Antecedents(t) == {e[1] : e \in {edge \in Edges : edge[2] = t}}

\* Successors(t) = set of tasks that depend on t
Successors(t) == {e[2] : e \in {edge \in Edges : edge[1] = t}}

\* The set of terminal (finished) statuses
TerminalStatuses == {"Success", "Failure", "RetriesExhausted"}

\* All valid task statuses
AllStatuses == {"Pending", "Scheduled", "Started", "Success", "Failure",
                "Retry", "RetriesExhausted"}

\* ---------------------------------------------------------------------------
\* State variables
\* ---------------------------------------------------------------------------

VARIABLES
    \* --- Blob Storage state (the durable source of truth) ---
    blobStatus,     \* blobStatus[t] \in AllStatuses: current status per task blob
    blobEtag,       \* blobEtag[t] \in Nat: monotonic ETag counter per task blob

    \* --- Blob lease state ---
    \* blobLeased[t] is TRUE when a scheduler holds the Azure blob lease on task t.
    \* The lease is a mutex: only the lease holder can write Scheduled.
    \* Modeled as a simple boolean; the lease_id is elided since it is just
    \* a capability token — the key property is mutual exclusion.
    blobLeased,     \* blobLeased[t] \in BOOLEAN

    \* --- Service Bus state ---
    sbMessages,     \* Set of records [taskId |-> t, deliveryCount |-> n]
                    \* Models the SB queue as an unordered bag

    \* --- Dead-letter tracking ---
    \* blobDeadLettered[t] is TRUE when the SB message for task t was dead-lettered
    \* (delivery count reached MaxDeliveryCount at LockExpiry).
    \* Tasks in this set are excluded from NoPermStuck because they are handled
    \* by a separate dead-letter reprocessing process.
    blobDeadLettered,  \* blobDeadLettered[t] \in BOOLEAN

    \* --- Per-worker local state ---
    \* Worker phases:
    \*   Idle                  -- waiting for a message
    \*   ReadForIdempotency    -- reading blob before writing Started
    \*   WroteStarted          -- Started write succeeded; proceeding to execute
    \*   Executing             -- task executing
    \*   WroteResult           -- result written; entering continue_graph
    \*   LoadingGraph          -- loading all blobs for continue_graph
    \*   CheckingMismatch      -- verifying loaded status matches written result
    \*   AcquiringLease        -- trying to acquire blob lease for a ready task
    \*   PublishingTask        -- publishing SB message for the leased task
    \*   WritingScheduled      -- writing Scheduled blob (lease held)
    \*   ReleasingLease        -- releasing the blob lease
    \*   Completing                  -- settling the SB message
    \*   RetryStartedAfterScheduled  -- 412 re-read saw Scheduled; retry Started write with re-read etag
    \*   RereadAfterRetry412         -- second 412 on retry; settle if claimed/terminal, redeliver if unclaimed
    workerPhase,    \* workerPhase[w]

    workerMsg,      \* workerMsg[w]: the SB message the worker holds, or Nil
    workerTask,     \* workerTask[w]: the task ID being processed by the worker, or Nil
    workerResult,   \* workerResult[w]: the execution outcome ("Success"/"Failure"), or Nil

    \* ETag captured during ReadForIdempotency (for the ETag CAS on Started write).
    \* 0 means "no etag captured" (transient read failure or no graph_id).
    workerCapturedEtag,  \* workerCapturedEtag[w] \in Nat \cup {0}

    \* Graph snapshot loaded during continue_graph (load_graph)
    workerSnapshot,         \* workerSnapshot[w]: Tasks -> AllStatuses \cup {Nil}
    workerSnapshotEtags,    \* workerSnapshotEtags[w]: Tasks -> Nat \cup {0}

    \* The set of ready tasks remaining to schedule in continue_graph
    workerReadySet,         \* workerReadySet[w]: set of task IDs

    \* The task currently being scheduled (lease acquired, publish pending / Scheduled pending)
    workerCurrentReady,     \* workerCurrentReady[w]: task ID or Nil

    \* TRUE if any dispatch in the current continue_graph pass returned FAILED.
    \* When TRUE at the end of the scheduling loop, the parent message MUST NOT
    \* be settled — it is returned to SB for redelivery (ContinueGraphError).
    workerDispatchFailed,   \* workerDispatchFailed[w] \in BOOLEAN

    \* --- Retries-exhausted-on-receive state ---
    \* attemptsExhausted[t]: TRUE if any delivery of task t arrives already
    \* retry-exhausted (record_attempt has driven attempts past max_tries on a
    \* prior delivery).  Set non-deterministically at Init for each task; once
    \* TRUE it stays TRUE (a task that exhausted retries cannot un-exhaust).
    attemptsExhausted,      \* attemptsExhausted[t] \in BOOLEAN

    \* retriesExhaustedDispatched[t]: TRUE iff some worker has successfully
    \* completed a continue_graph dispatch pass after observing/writing
    \* RetriesExhausted for t.  Used to express the NoOrphanedRetriesExhausted
    \* invariant: a permanently-settled RetriesExhausted message must have its
    \* failure callbacks dispatched at least once.
    retriesExhaustedDispatched,  \* retriesExhaustedDispatched[t] \in BOOLEAN

    \* workerRetriesExhaustedDispatch[w]: TRUE iff the current dispatch pass on
    \* worker w was entered via the RetriesExhausted-on-receive path (so a
    \* successful FinishAcquiringSuccess should mark retriesExhaustedDispatched
    \* for workerTask[w]).
    workerRetriesExhaustedDispatch,   \* workerRetriesExhaustedDispatch[w] \in BOOLEAN

    \* buggyPreSettledParent[t]: TRUE iff the OLD (buggy) RetriesExhausted-on-
    \* receive path ran for task t and settled the parent message before
    \* dispatch.  Distinguishes "deadlettered by the bug" from "deadlettered by
    \* lock expiry" so that NoOrphanedRetriesExhausted can detect the bug.
    \* Always FALSE in the NEW (fixed) ordering.
    buggyPreSettledParent   \* buggyPreSettledParent[t] \in BOOLEAN

vars == <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages,
          workerPhase, workerMsg, workerTask, workerResult,
          workerCapturedEtag,
          workerSnapshot, workerSnapshotEtags,
          workerReadySet, workerCurrentReady, workerDispatchFailed,
          attemptsExhausted, retriesExhaustedDispatched,
          workerRetriesExhaustedDispatch, buggyPreSettledParent>>

\* Helper tuples for the retries-exhausted-on-receive variables.  Most actions
\* leave these UNCHANGED via `reVars`; actions that internally call WorkerReset
\* assign workerRetriesExhaustedDispatch' themselves and use `reVarsNoWorker`.
reVars == <<attemptsExhausted, retriesExhaustedDispatched,
            workerRetriesExhaustedDispatch, buggyPreSettledParent>>
reVarsNoWorker == <<attemptsExhausted, retriesExhaustedDispatched,
                    buggyPreSettledParent>>

\* ---------------------------------------------------------------------------
\* Type invariant
\* ---------------------------------------------------------------------------

WorkerPhases == {"Idle", "ReadForIdempotency",
                 "WroteStarted", "Executing", "WroteResult",
                 "LoadingGraph", "CheckingMismatch",
                 "AcquiringLease", "PublishingTask", "WritingScheduled", "ReleasingLease",
                 "Completing",
                 "RetryStartedAfterScheduled", "RereadAfterRetry412"}

TypeOK ==
    /\ blobStatus \in [Tasks -> AllStatuses]
    /\ blobEtag \in [Tasks -> Nat]
    /\ blobLeased \in [Tasks -> BOOLEAN]
    /\ blobDeadLettered \in [Tasks -> BOOLEAN]
    /\ \A msg \in sbMessages :
        /\ msg.taskId \in Tasks
        /\ msg.deliveryCount \in 1..MaxDeliveryCount
    /\ workerPhase \in [Workers -> WorkerPhases]
    /\ \A w \in Workers : workerTask[w] \in Tasks \cup {Nil}
    /\ \A w \in Workers : workerResult[w] \in {"Success", "Failure", "RetriesExhausted", Nil}
    /\ \A w \in Workers : workerCapturedEtag[w] \in Nat \cup {0}
    /\ \A w \in Workers : workerCurrentReady[w] \in Tasks \cup {Nil}
    /\ workerDispatchFailed \in [Workers -> BOOLEAN]
    /\ attemptsExhausted \in [Tasks -> BOOLEAN]
    /\ retriesExhaustedDispatched \in [Tasks -> BOOLEAN]
    /\ workerRetriesExhaustedDispatch \in [Workers -> BOOLEAN]
    /\ buggyPreSettledParent \in [Tasks -> BOOLEAN]

\* ---------------------------------------------------------------------------
\* Initial state
\* ---------------------------------------------------------------------------

\* publish_graph has already run: root tasks are in Scheduled status with
\* SB messages published; all other tasks are Pending.
\* The publish-before-store ordering in publish_graph means root tasks have
\* SB messages AND Scheduled blobs from the start (no window of exposure here
\* since publish_graph is a single atomic setup step).
Init ==
    /\ blobStatus = [t \in Tasks |-> IF t \in RootTasks THEN "Scheduled" ELSE "Pending"]
    /\ blobEtag   = [t \in Tasks |-> IF t \in RootTasks THEN 2 ELSE 1]
          \* ETag 1 = initial Pending write; ETag 2 = Scheduled write for roots
    /\ blobLeased  = [t \in Tasks |-> FALSE]
    /\ blobDeadLettered = [t \in Tasks |-> FALSE]
    /\ sbMessages = {[taskId |-> t, deliveryCount |-> 1] : t \in RootTasks}
    /\ workerPhase = [w \in Workers |-> "Idle"]
    /\ workerMsg  = [w \in Workers |-> Nil]
    /\ workerTask = [w \in Workers |-> Nil]
    /\ workerResult = [w \in Workers |-> Nil]
    /\ workerCapturedEtag = [w \in Workers |-> 0]
    /\ workerSnapshot = [w \in Workers |-> [t \in Tasks |-> Nil]]
    /\ workerSnapshotEtags = [w \in Workers |-> [t \in Tasks |-> 0]]
    /\ workerReadySet = [w \in Workers |-> {}]
    /\ workerCurrentReady = [w \in Workers |-> Nil]
    /\ workerDispatchFailed = [w \in Workers |-> FALSE]
    \* Each task is non-deterministically marked exhausted or not at Init.
    \* TLC enumerates all 2^|Tasks| assignments, which produces the necessary
    \* counterexamples on the OLD buggy ordering.  Root tasks may be exhausted
    \* (they have in-flight SB messages at Init); non-root tasks may also be
    \* exhausted if a later dispatch will publish them (the flag predates the
    \* actual message delivery).
    /\ attemptsExhausted \in [Tasks -> BOOLEAN]
    /\ retriesExhaustedDispatched = [t \in Tasks |-> FALSE]
    /\ workerRetriesExhaustedDispatch = [w \in Workers |-> FALSE]
    /\ buggyPreSettledParent = [t \in Tasks |-> FALSE]

\* ---------------------------------------------------------------------------
\* Helper: reset worker to Idle
\* ---------------------------------------------------------------------------

WorkerReset(w) ==
    /\ workerPhase' = [workerPhase EXCEPT ![w] = "Idle"]
    /\ workerMsg' = [workerMsg EXCEPT ![w] = Nil]
    /\ workerTask' = [workerTask EXCEPT ![w] = Nil]
    /\ workerResult' = [workerResult EXCEPT ![w] = Nil]
    /\ workerCapturedEtag' = [workerCapturedEtag EXCEPT ![w] = 0]
    /\ workerSnapshot' = [workerSnapshot EXCEPT ![w] = [t \in Tasks |-> Nil]]
    /\ workerSnapshotEtags' = [workerSnapshotEtags EXCEPT ![w] = [t \in Tasks |-> 0]]
    /\ workerReadySet' = [workerReadySet EXCEPT ![w] = {}]
    /\ workerCurrentReady' = [workerCurrentReady EXCEPT ![w] = Nil]
    /\ workerDispatchFailed' = [workerDispatchFailed EXCEPT ![w] = FALSE]
    /\ workerRetriesExhaustedDispatch' =
            [workerRetriesExhaustedDispatch EXCEPT ![w] = FALSE]

\* ---------------------------------------------------------------------------
\* Actions
\* ---------------------------------------------------------------------------

\* ===== ReceiveMessage =====
\* Worker dequeues an arbitrary message from the SB queue.
ReceiveMessage(w) ==
    /\ workerPhase[w] = "Idle"
    /\ sbMessages /= {}
    /\ \E msg \in sbMessages :
        /\ sbMessages' = sbMessages \ {msg}
        /\ workerPhase' = [workerPhase EXCEPT ![w] = "ReadForIdempotency"]
        /\ workerMsg'   = [workerMsg   EXCEPT ![w] = msg]
        /\ workerTask'  = [workerTask  EXCEPT ![w] = msg.taskId]
        /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, workerResult,
                        workerCapturedEtag,
                        workerSnapshot, workerSnapshotEtags,
                        workerReadySet, workerCurrentReady, workerDispatchFailed>>
        /\ UNCHANGED reVars

\* ===== ReadForIdempotency =====
\* Worker reads the blob before writing Started.
\* Captures the etag for the subsequent ETag CAS.
\*
\* If the status is terminal: run continue_graph, complete, return.
\* If non-terminal: record the etag and proceed to WroteStarted.
\* If the read fails (transient, fail-open): proceed with etag=0 (unconditional write).
ReadForIdempotency(w) ==
    /\ workerPhase[w] = "ReadForIdempotency"
    /\ LET t == workerTask[w]
           currentStatus == blobStatus[t]
           currentEtag   == blobEtag[t]
       IN
       \/ \* Read succeeds and status is terminal: skip execution, go to continue_graph
          /\ currentStatus \in TerminalStatuses
          /\ workerResult' = [workerResult EXCEPT ![w] = currentStatus]
          /\ workerPhase'  = [workerPhase  EXCEPT ![w] = "LoadingGraph"]
          /\ workerCapturedEtag' = [workerCapturedEtag EXCEPT ![w] = currentEtag]
          \* If the terminal status we observe is RetriesExhausted, mark this
          \* worker as performing the RetriesExhausted dispatch so a successful
          \* dispatch completion records retriesExhaustedDispatched.
          /\ workerRetriesExhaustedDispatch' =
                IF currentStatus = "RetriesExhausted"
                THEN [workerRetriesExhaustedDispatch EXCEPT ![w] = TRUE]
                ELSE workerRetriesExhaustedDispatch
          /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                          workerSnapshot, workerSnapshotEtags,
                          workerReadySet, workerCurrentReady, workerDispatchFailed>>
          /\ UNCHANGED <<attemptsExhausted, retriesExhaustedDispatched,
                         buggyPreSettledParent>>
       \/ \* Read succeeds and status is non-terminal: capture etag, proceed to Started write
          /\ currentStatus \notin TerminalStatuses
          \* Guard: the RetriesExhausted-on-receive branch fires from this same
          \* state, so we exclude the case where the task arrives exhausted to
          \* avoid spuriously entering the Started write path.
          /\ ~attemptsExhausted[t]
          /\ workerCapturedEtag' = [workerCapturedEtag EXCEPT ![w] = currentEtag]
          /\ workerPhase' = [workerPhase EXCEPT ![w] = "WroteStarted"]
          /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                          workerResult, workerSnapshot, workerSnapshotEtags,
                          workerReadySet, workerCurrentReady, workerDispatchFailed>>
          /\ UNCHANGED reVars
       \/ \* Read fails (transient): proceed with etag=0 (unconditional write, fail-open)
          \* Same guard as above: not an exhausted-on-receive path.
          /\ ~attemptsExhausted[t]
          /\ workerCapturedEtag' = [workerCapturedEtag EXCEPT ![w] = 0]
          /\ workerPhase' = [workerPhase EXCEPT ![w] = "WroteStarted"]
          /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                          workerResult, workerSnapshot, workerSnapshotEtags,
                          workerReadySet, workerCurrentReady, workerDispatchFailed>>
          /\ UNCHANGED reVars

\* ===== WriteStarted =====
\* Worker attempts to write Started with an ETag CAS.
\* capturedEtag=0 means "no etag" (unconditional write).
\*
\* On success: proceed to Executing.
\* On 412 (etag mismatch): re-read blob and branch.
\* On non-412 error: proceed to Executing (fail-open).
\*
\* We split this into three sub-actions for clarity:
\*   WriteStartedSuccess, WriteStarted412, WriteStartedNon412Error

\* Sub-action: CAS succeeds (etag matches or unconditional)
WriteStartedSuccess(w) ==
    /\ workerPhase[w] = "WroteStarted"
    /\ LET t    == workerTask[w]
           cEtag == workerCapturedEtag[w]
       IN
       \* CAS condition: either unconditional (cEtag=0) or etag still matches
       /\ (cEtag = 0 \/ blobEtag[t] = cEtag)
       /\ blobStatus' = [blobStatus EXCEPT ![t] = "Started"]
       /\ blobEtag'   = [blobEtag   EXCEPT ![t] = blobEtag[t] + 1]
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "Executing"]
       /\ UNCHANGED <<blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask, workerResult,
                       workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED reVars

\* Sub-action: 412 — etag mismatch; re-read blob and branch
\* We model the re-read as instantaneous (reading current blobStatus).
\* The 412 branch covers all cases:
\*   None      -> Completing (blob vanished — unexpected; always settle)
\*   terminal  -> Completing (complete message)
\*   Started   -> Completing (complete message; another worker won)
\*   Scheduled -> RetryStartedAfterScheduled (retry with re-read etag)
\*   Pending   -> RetryStartedAfterScheduled (retry with re-read etag; same as Scheduled)
\*               NOTE: under corrected Azure semantics lease acquire does NOT bump the
\*               ETag, so this Pending branch is unreachable in normal operation —
\*               the consumer's etag still matches while the blob is leased and the
\*               write gets 409, not 412.  The branch is retained defensively.
\*   other     -> Completing (always settle)
\*
\* Since blobStatus[t] is always defined (never None) in this model, the
\* "blob vanished" branch is unreachable.
\*
\* IMPORTANT: when transitioning to RetryStartedAfterScheduled, we capture the
\* current blob etag into workerCapturedEtag.  The retry will use this etag for
\* the second CAS attempt.
WriteStarted412(w) ==
    /\ workerPhase[w] = "WroteStarted"
    /\ LET t    == workerTask[w]
           cEtag == workerCapturedEtag[w]
       IN
       \* 412 condition: etag was set AND it no longer matches
       /\ cEtag /= 0
       /\ blobEtag[t] /= cEtag
       \* Re-read current status to decide branch
       /\ LET rereadStatus == blobStatus[t]
              rereadEtag   == blobEtag[t]
          IN
          \/ \* Re-read is terminal or Started: complete message (yield/skip)
             /\ rereadStatus \in TerminalStatuses \cup {"Started"}
             /\ workerPhase' = [workerPhase EXCEPT ![w] = "Completing"]
             /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages,
                             workerMsg, workerTask, workerResult, workerCapturedEtag,
                             workerSnapshot, workerSnapshotEtags,
                             workerReadySet, workerCurrentReady, workerDispatchFailed>>
             /\ UNCHANGED reVars
          \/ \* Re-read is Scheduled or Pending: no worker owns Started yet —
             \* retry the Started write with the re-read etag.
             \* Pending:   Under corrected Azure semantics (lease acquire does NOT bump
             \*             ETag) this branch is unreachable in normal operation: the
             \*             consumer's etag matches while the blob is leased, so the
             \*             write gets 409 instead.  Retained defensively.
             \* Scheduled: normal publish-before-store window.
             /\ rereadStatus \in {"Scheduled", "Pending"}
             /\ workerPhase' = [workerPhase EXCEPT ![w] = "RetryStartedAfterScheduled"]
             \* Capture the re-read etag for the retry CAS
             /\ workerCapturedEtag' = [workerCapturedEtag EXCEPT ![w] = rereadEtag]
             /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages,
                             workerMsg, workerTask, workerResult,
                             workerSnapshot, workerSnapshotEtags,
                             workerReadySet, workerCurrentReady, workerDispatchFailed>>
             /\ UNCHANGED reVars
          \/ \* Re-read is some other non-terminal, non-Started, non-Scheduled, non-Pending status:
             \* always settle (complete message, return Failure)
             /\ rereadStatus \notin TerminalStatuses \cup {"Started", "Scheduled", "Pending"}
             /\ workerPhase' = [workerPhase EXCEPT ![w] = "Completing"]
             /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages,
                             workerMsg, workerTask, workerResult, workerCapturedEtag,
                             workerSnapshot, workerSnapshotEtags,
                             workerReadySet, workerCurrentReady, workerDispatchFailed>>
             /\ UNCHANGED reVars

\* ===== RetryStartedWrite =====
\* Worker retries the Started write using the etag captured from the re-read.
\* Entered from WriteStarted412 when the re-read status is Scheduled or Pending.
\* In both cases no other worker holds Started, so this worker is the legitimate
\* claimant and should try to make progress.
\*
\* Three outcomes:
\*   CAS succeeds (etag still matches re-read etag): blob -> Started, proceed to Executing.
\*   CAS 412 again (someone else mutated blob): proceed to RereadAfterRetry412.
\*   Non-412 storage error: proceed to Executing anyway (fail-open, same as original).
RetryStartedWriteSuccess(w) ==
    /\ workerPhase[w] = "RetryStartedAfterScheduled"
    /\ LET t    == workerTask[w]
           cEtag == workerCapturedEtag[w]
       IN
       \* CAS condition: etag matches the Scheduled blob's etag we captured
       /\ blobEtag[t] = cEtag
       /\ blobStatus' = [blobStatus EXCEPT ![t] = "Started"]
       /\ blobEtag'   = [blobEtag   EXCEPT ![t] = blobEtag[t] + 1]
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "Executing"]
       /\ UNCHANGED <<blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask, workerResult,
                       workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED reVars

RetryStartedWrite412(w) ==
    /\ workerPhase[w] = "RetryStartedAfterScheduled"
    /\ LET t    == workerTask[w]
           cEtag == workerCapturedEtag[w]
       IN
       \* 412: etag no longer matches (another worker moved the blob forward)
       /\ blobEtag[t] /= cEtag
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "RereadAfterRetry412"]
       /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages,
                       workerMsg, workerTask, workerResult, workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED reVars

RetryStartedWriteNon412Error(w) ==
    /\ workerPhase[w] = "RetryStartedAfterScheduled"
    \* Non-412 storage error: proceed with execution anyway (fail-open)
    /\ workerPhase' = [workerPhase EXCEPT ![w] = "Executing"]
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages,
                    workerMsg, workerTask, workerResult, workerCapturedEtag,
                    workerSnapshot, workerSnapshotEtags,
                    workerReadySet, workerCurrentReady, workerDispatchFailed>>
    /\ UNCHANGED reVars

\* ===== RereadAfterRetry =====
\* Worker got a second 412 on the retry. Re-reads the blob to determine action.
\*
\* Two variants, matching the code fix:
\*   Started/terminal -> RereadAfterRetrySettle: proceed to Completing (settle msg)
\*   Pending/Scheduled -> RereadAfterRetryNoSettle: do NOT settle; return message
\*     to the SB queue so a fresh worker can retry (mirrors LockExpiry).
\*
\* Settling when the blob is unclaimed would stall the graph permanently.

\* Settle variant: another worker owns the task or it is already terminal.
RereadAfterRetrySettle(w) ==
    /\ workerPhase[w] = "RereadAfterRetry412"
    /\ LET t == workerTask[w]
       IN blobStatus[t] \in {"Started"} \cup TerminalStatuses
    /\ workerPhase' = [workerPhase EXCEPT ![w] = "Completing"]
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages,
                    workerMsg, workerTask, workerResult, workerCapturedEtag,
                    workerSnapshot, workerSnapshotEtags,
                    workerReadySet, workerCurrentReady, workerDispatchFailed>>
    /\ UNCHANGED reVars

\* No-settle variant: blob is unclaimed (Pending or Scheduled).
\* Return the SB message to the queue and reset the worker, exactly like LockExpiry.
RereadAfterRetryNoSettle(w) ==
    /\ workerPhase[w] = "RereadAfterRetry412"
    /\ LET t   == workerTask[w]
           msg == workerMsg[w]
       IN
       /\ blobStatus[t] \in {"Pending", "Scheduled"}
       /\ \/ /\ msg.deliveryCount < MaxDeliveryCount
             /\ sbMessages' = sbMessages \cup
                   {[taskId |-> msg.taskId,
                     deliveryCount |-> msg.deliveryCount + 1]}
             /\ UNCHANGED blobDeadLettered
          \/ /\ msg.deliveryCount = MaxDeliveryCount
             /\ UNCHANGED sbMessages
             /\ blobDeadLettered' = [blobDeadLettered EXCEPT ![msg.taskId] = TRUE]
    /\ WorkerReset(w)
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased>>
    /\ UNCHANGED reVarsNoWorker

\* Sub-action: non-412 storage error on Started write — proceed anyway (fail-open)
WriteStartedNon412Error(w) ==
    /\ workerPhase[w] = "WroteStarted"
    /\ LET t    == workerTask[w]
           cEtag == workerCapturedEtag[w]
       IN
       \* Can fire whenever (nondeterministic storage error regardless of etag state)
       \* The implementation proceeds with execution even without writing Started.
       \* Model: blob is NOT written (storage error), but worker proceeds to Executing.
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "Executing"]
       /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                       workerResult, workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED reVars

\* NOTE: Settlement is now conditional on blob state.
\* RereadAfterRetrySettle leads to Completing (settle) when the blob is claimed/terminal.
\* RereadAfterRetryNoSettle returns the SB message and resets the worker (no settle)
\* when the blob is unclaimed (Pending/Scheduled) — stalling would otherwise be permanent.

\* ===== ExecuteTask =====
\* Nondeterministic task execution outcome: Success or Failure.
ExecuteTask(w) ==
    /\ workerPhase[w] = "Executing"
    /\ \E outcome \in {"Success", "Failure"} :
        /\ workerResult' = [workerResult EXCEPT ![w] = outcome]
        /\ workerPhase'  = [workerPhase  EXCEPT ![w] = "WroteResult"]
        /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                        workerCapturedEtag,
                        workerSnapshot, workerSnapshotEtags,
                        workerReadySet, workerCurrentReady, workerDispatchFailed>>
        /\ UNCHANGED reVars

\* ===== WriteResult =====
\* Worker writes the execution result to blob storage (unconditional overwrite).
\* In real Azure Blob Storage, a non-lease write against a leased blob is rejected (409).
\* The lease guard here makes that constraint explicit: the write can only succeed when
\* the blob lease is not held.  In the correct protocol the scheduler always releases
\* the lease before any worker can reach this point (see ResultWriteNotBlockedByLease).
WriteResult(w) ==
    /\ workerPhase[w] = "WroteResult"
    /\ LET t      == workerTask[w]
           result == workerResult[w]
       IN
       /\ ~blobLeased[t]          \* lease must not be held (409 if it is)
       /\ blobStatus' = [blobStatus EXCEPT ![t] = result]
       /\ blobEtag'   = [blobEtag   EXCEPT ![t] = blobEtag[t] + 1]
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "LoadingGraph"]
       /\ UNCHANGED <<blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask, workerResult,
                       workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED reVars

\* ===== LoadGraph =====
\* Worker reads all task statuses from blob storage.
\* Atomically snapshots blob state (sound over-approximation; see original spec note).
\* Enters continue_graph: proceed to status mismatch check.
LoadGraph(w) ==
    /\ workerPhase[w] = "LoadingGraph"
    /\ workerSnapshot' = [workerSnapshot EXCEPT ![w] = blobStatus]
    /\ workerSnapshotEtags' = [workerSnapshotEtags EXCEPT ![w] = blobEtag]
    /\ workerPhase' = [workerPhase EXCEPT ![w] = "CheckingMismatch"]
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                    workerResult, workerCapturedEtag,
                    workerReadySet, workerCurrentReady, workerDispatchFailed>>
    /\ UNCHANGED reVars

\* ===== StatusMismatchCheck =====
\* Verify that the loaded status for the completed task matches the written result.
\* On mismatch: ContinueGraphError — suppress settlement (return without completing).
\* On match: compute ready set and proceed to schedule successors.
StatusMismatchCheck(w) ==
    /\ workerPhase[w] = "CheckingMismatch"
    /\ LET t        == workerTask[w]
           expected == workerResult[w]
           loaded   == workerSnapshot[w][t]
       IN
       \/ \* Match: compute ready set, proceed to scheduling
          /\ loaded = expected
          /\ LET readySet == {child \in Tasks :
                    /\ workerSnapshot[w][child] = "Pending"
                    /\ \A parent \in Antecedents(child) :
                        workerSnapshot[w][parent] = "Success"}
             IN
             /\ workerReadySet' = [workerReadySet EXCEPT ![w] = readySet]
             /\ workerPhase' = [workerPhase EXCEPT ![w] = "AcquiringLease"]
             /\ workerCurrentReady' = [workerCurrentReady EXCEPT ![w] = Nil]
             \* Initialize dispatch_failed to FALSE at the start of the scheduling loop
             /\ workerDispatchFailed' = [workerDispatchFailed EXCEPT ![w] = FALSE]
          /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                          workerResult, workerCapturedEtag,
                          workerSnapshot, workerSnapshotEtags>>
          /\ UNCHANGED reVars
       \/ \* Mismatch: ContinueGraphError — suppress settlement (message returns to SB)
          /\ loaded /= expected
          /\ LET msg == workerMsg[w]
             IN
             /\ \/ /\ msg.deliveryCount < MaxDeliveryCount
                   /\ sbMessages' = sbMessages \cup
                         {[taskId |-> msg.taskId,
                           deliveryCount |-> msg.deliveryCount + 1]}
                \/ /\ msg.deliveryCount = MaxDeliveryCount
                   /\ UNCHANGED sbMessages
             /\ WorkerReset(w)
          /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered>>
          /\ UNCHANGED reVarsNoWorker

\* ===== AcquireLeaseForReady =====
\* For the continue_graph scheduling loop: pick one ready task, try to acquire its lease.
\*
\* Lease acquire with etag precondition:
\*   - Succeeds if: blob is still Pending AND etag matches snapshot AND not already leased.
\*   - Fails (409): blob is already leased -> skip this task (another worker is scheduling it).
\*   - Fails (412): blob etag changed since snapshot -> skip this task.
\*
\* On success: move to PublishingTask with workerCurrentReady set.
\* On failure: remove from readySet (skip), stay in AcquiringLease.
\* When readySet is empty: transition to Completing.
\*
\* ETag on lease acquire: Azure Blob Storage does NOT advance the blob's ETag when
\* BlobLeaseClient.acquire() succeeds — lease state is separate from blob content.
\* We therefore leave blobEtag unchanged in AcquireLeaseSuccess.
\* Consequence: a worker that captured the blob's ETag before the lease acquire
\* will still hold a valid ETag and its Started write will receive 409 (blob is
\* leased, non-lease write rejected) rather than 412 (ETag mismatch).  The
\* 412+Pending scenario modeled in WriteStarted412 is therefore unreachable under
\* correct Azure semantics; the branch is retained defensively.

AcquireLeaseSuccess(w) ==
    /\ workerPhase[w] = "AcquiringLease"
    /\ workerReadySet[w] /= {}
    /\ \E child \in workerReadySet[w] :
        LET snapshotEtag == workerSnapshotEtags[w][child]
        IN
        \* Lease succeeds: blob unchanged (etag matches) and not already leased
        /\ blobEtag[child] = snapshotEtag
        /\ blobStatus[child] = "Pending"
        /\ ~blobLeased[child]
        /\ blobLeased' = [blobLeased EXCEPT ![child] = TRUE]
        \* Lease acquire does NOT advance the ETag (Azure docs: only content writes do).
        /\ workerCurrentReady' = [workerCurrentReady EXCEPT ![w] = child]
        /\ workerReadySet' = [workerReadySet EXCEPT ![w] = workerReadySet[w] \ {child}]
        /\ workerPhase' = [workerPhase EXCEPT ![w] = "PublishingTask"]
        /\ UNCHANGED <<blobStatus, blobEtag, blobDeadLettered, sbMessages, workerMsg, workerTask,
                        workerResult, workerCapturedEtag,
                        workerSnapshot, workerSnapshotEtags, workerDispatchFailed>>
        /\ UNCHANGED reVars

AcquireLeaseFail(w) ==
    /\ workerPhase[w] = "AcquiringLease"
    /\ workerReadySet[w] /= {}
    /\ \E child \in workerReadySet[w] :
        \* Lease fails: either already leased (409) or etag mismatch (412)
        /\ \/ blobLeased[child]
           \/ blobEtag[child] /= workerSnapshotEtags[w][child]
        /\ workerReadySet' = [workerReadySet EXCEPT ![w] = workerReadySet[w] \ {child}]
        /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerPhase,
                        workerMsg, workerTask, workerResult, workerCapturedEtag,
                        workerSnapshot, workerSnapshotEtags, workerCurrentReady, workerDispatchFailed>>
        /\ UNCHANGED reVars

\* ===== AcquireLeaseStorageError =====
\* try_acquire_lease raises a storage error (not 409/412 lease contention, but
\* a genuine infrastructure failure). The dispatch is lost.
\* Maps to _DispatchOutcome.FAILED in the implementation.
\* Sets workerDispatchFailed so that FinishAcquiring suppresses settlement.
AcquireLeaseStorageError(w) ==
    /\ workerPhase[w] = "AcquiringLease"
    /\ workerReadySet[w] /= {}
    /\ \E child \in workerReadySet[w] :
        /\ workerReadySet' = [workerReadySet EXCEPT ![w] = workerReadySet[w] \ {child}]
        /\ workerDispatchFailed' = [workerDispatchFailed EXCEPT ![w] = TRUE]
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerPhase,
                    workerMsg, workerTask, workerResult, workerCapturedEtag,
                    workerSnapshot, workerSnapshotEtags, workerCurrentReady>>
    /\ UNCHANGED reVars

\* ===== FinishAcquiring =====
\* All ready tasks have been attempted. If any dispatch failed
\* (workerDispatchFailed is TRUE), the parent message MUST NOT be settled:
\* ContinueGraphError is raised, causing the message to be returned to
\* SB for redelivery. Otherwise, settle the message normally.
FinishAcquiringSuccess(w) ==
    /\ workerPhase[w] = "AcquiringLease"
    /\ workerReadySet[w] = {}
    /\ workerCurrentReady[w] = Nil
    /\ ~workerDispatchFailed[w]
    \* No failures: settle the message
    /\ workerPhase' = [workerPhase EXCEPT ![w] = "Completing"]
    \* If this dispatch was entered via the RetriesExhausted-on-receive path,
    \* mark the task's failure callbacks as dispatched: continue_graph
    \* completed successfully and any failure tasks have been published to SB.
    /\ retriesExhaustedDispatched' =
            IF workerRetriesExhaustedDispatch[w]
            THEN [retriesExhaustedDispatched EXCEPT ![workerTask[w]] = TRUE]
            ELSE retriesExhaustedDispatched
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                    workerResult, workerCapturedEtag,
                    workerSnapshot, workerSnapshotEtags,
                    workerReadySet, workerCurrentReady, workerDispatchFailed>>
    /\ UNCHANGED <<attemptsExhausted, workerRetriesExhaustedDispatch,
                   buggyPreSettledParent>>

\* Dispatch-failed variant: at least one ready task could not be published.
\* ContinueGraphError is raised: do NOT settle the parent message.
\* Return it to SB for redelivery (bounded by MaxDeliveryCount).
\* On redelivery, the idempotent guard skips re-executing the finished parent
\* task and retries continue_graph; SB duplicate-detection suppresses
\* re-publishing tasks that already succeeded.
FinishAcquiringDispatchFailed(w) ==
    /\ workerPhase[w] = "AcquiringLease"
    /\ workerReadySet[w] = {}
    /\ workerCurrentReady[w] = Nil
    /\ workerDispatchFailed[w]
    \* Dispatch failed: behavior diverges between OLD and NEW orderings on the
    \* RetriesExhausted path.
    \*   NEW: parent message is still in flight (not yet settled); return it to
    \*        SB for redelivery exactly like a normal failed dispatch.
    \*   OLD (buggy): parent message was already deadlettered by the pre-settle
    \*        step in RetriesExhaustedOnReceiveBuggy.  It is therefore gone from
    \*        sbMessages permanently — we cannot requeue.  The worker just
    \*        resets, the dispatch is lost, and retriesExhaustedDispatched stays
    \*        FALSE.  This is the orphaning we want TLC to find.
    /\ LET msg == workerMsg[w]
           buggyPreSettled == BuggyRetriesExhaustedOrdering
                              /\ workerRetriesExhaustedDispatch[w]
       IN
       \/ \* OLD-buggy path: message was pre-settled, no requeue.
          /\ buggyPreSettled
          /\ UNCHANGED sbMessages
          /\ UNCHANGED blobDeadLettered
       \/ \* NEW path: requeue the message (or DLQ if delivery count exhausted).
          /\ ~buggyPreSettled
          /\ \/ /\ msg.deliveryCount < MaxDeliveryCount
                /\ sbMessages' = sbMessages \cup
                      {[taskId |-> msg.taskId,
                        deliveryCount |-> msg.deliveryCount + 1]}
                /\ UNCHANGED blobDeadLettered
             \/ /\ msg.deliveryCount = MaxDeliveryCount
                /\ UNCHANGED sbMessages
                /\ blobDeadLettered' = [blobDeadLettered EXCEPT ![msg.taskId] = TRUE]
    /\ WorkerReset(w)
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased>>
    /\ UNCHANGED reVarsNoWorker

\* ===== PublishTask =====
\* Publish SB message for the leased task (publish-before-store).
\* On success: proceed to writing Scheduled.
\* On failure: release lease, skip this task (it remains Pending, re-discovered later).
PublishTaskSuccess(w) ==
    /\ workerPhase[w] = "PublishingTask"
    /\ workerCurrentReady[w] /= Nil
    /\ LET child == workerCurrentReady[w]
       IN
       /\ sbMessages' = sbMessages \cup {[taskId |-> child, deliveryCount |-> 1]}
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "WritingScheduled"]
       /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, workerMsg, workerTask,
                       workerResult, workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED reVars

PublishTaskFail(w) ==
    /\ workerPhase[w] = "PublishingTask"
    /\ workerCurrentReady[w] /= Nil
    /\ LET child == workerCurrentReady[w]
       IN
       \* Publish failed: release lease (in finally), set dispatch_failed,
       \* return to acquiring next task (best-effort: try remaining tasks).
       /\ blobLeased' = [blobLeased EXCEPT ![child] = FALSE]
       /\ workerCurrentReady' = [workerCurrentReady EXCEPT ![w] = Nil]
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "AcquiringLease"]
       /\ workerDispatchFailed' = [workerDispatchFailed EXCEPT ![w] = TRUE]
       /\ UNCHANGED <<blobStatus, blobEtag, blobDeadLettered, sbMessages, workerMsg, workerTask,
                       workerResult, workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags, workerReadySet>>
       /\ UNCHANGED reVars

\* ===== WriteScheduled =====
\* Write Scheduled status to blob (lease held, so no etag needed).
\* The lease prevents concurrent writes: only the lease holder can write.
\* On success or failure: proceed to ReleasingLease.
WriteScheduledSuccess(w) ==
    /\ workerPhase[w] = "WritingScheduled"
    /\ workerCurrentReady[w] /= Nil
    /\ LET child == workerCurrentReady[w]
       IN
       /\ blobLeased[child]   \* lease must still be held
       /\ blobStatus' = [blobStatus EXCEPT ![child] = "Scheduled"]
       /\ blobEtag'   = [blobEtag   EXCEPT ![child] = blobEtag[child] + 1]
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "ReleasingLease"]
       /\ UNCHANGED <<blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                       workerResult, workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED reVars

WriteScheduledFail(w) ==
    /\ workerPhase[w] = "WritingScheduled"
    /\ workerCurrentReady[w] /= Nil
    \* Storage error on Scheduled write — blob stays Pending. Proceed to release.
    /\ workerPhase' = [workerPhase EXCEPT ![w] = "ReleasingLease"]
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                    workerResult, workerCapturedEtag,
                    workerSnapshot, workerSnapshotEtags,
                    workerReadySet, workerCurrentReady, workerDispatchFailed>>
    /\ UNCHANGED reVars

\* ===== ReleaseLease =====
\* Release the blob lease (always, in finally block).
\* Return to AcquiringLease to process next ready task.
ReleaseLease(w) ==
    /\ workerPhase[w] = "ReleasingLease"
    /\ workerCurrentReady[w] /= Nil
    /\ LET child == workerCurrentReady[w]
       IN
       /\ blobLeased' = [blobLeased EXCEPT ![child] = FALSE]
       /\ workerCurrentReady' = [workerCurrentReady EXCEPT ![w] = Nil]
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "AcquiringLease"]
       /\ UNCHANGED <<blobStatus, blobEtag, blobDeadLettered, sbMessages, workerMsg, workerTask,
                       workerResult, workerCapturedEtag,
                       workerSnapshot, workerSnapshotEtags, workerReadySet, workerDispatchFailed>>
       /\ UNCHANGED reVars

\* ===== CompleteMessage =====
\* Settle the SB message. Message was already removed from sbMessages on receive.
\* This action is only reachable when workerDispatchFailed is FALSE
\* (FinishAcquiringDispatchFailed handles the failed case by returning the message).
CompleteMessage(w) ==
    /\ workerPhase[w] = "Completing"
    /\ WorkerReset(w)
    /\ UNCHANGED <<blobStatus, blobEtag, blobLeased, blobDeadLettered, sbMessages>>
    /\ UNCHANGED reVarsNoWorker

\* ===== LockExpiry =====
\* SB message lock expires at any point while a worker holds it.
\* The message returns to the queue with incremented delivery count.
\* Worker's local state is reset.
\*
\* If the worker holds a blob lease (in AcquiringLease/PublishingTask/WritingScheduled/ReleasingLease),
\* the lease is also released (Azure lease has its own TTL, but we model crash = lease released).
LockExpiry(w) ==
    /\ workerPhase[w] /= "Idle"
    /\ workerTask[w] /= Nil   \* task is set iff msg is set; avoids record/string equality error
    /\ LET msg   == workerMsg[w]
           child == workerCurrentReady[w]
           buggyPreSettled == BuggyRetriesExhaustedOrdering
                              /\ workerRetriesExhaustedDispatch[w]
       IN
       \* Release blob lease if held
       /\ blobLeased' = IF child /= Nil
                        THEN [blobLeased EXCEPT ![child] = FALSE]
                        ELSE blobLeased
       \* OLD-buggy path: the parent SB message was already settled to DLQ before
       \* dispatch began.  A lock-expiry on the in-flight worker cannot return the
       \* message to SB (there is no message held by the SB client any more);
       \* nor can it advance blobDeadLettered (the message already is in DLQ).
       /\ \/ /\ buggyPreSettled
             /\ UNCHANGED sbMessages
             /\ UNCHANGED blobDeadLettered
          \/ /\ ~buggyPreSettled
             /\ \/ /\ msg.deliveryCount < MaxDeliveryCount
                   /\ sbMessages' = sbMessages \cup
                         {[taskId |-> msg.taskId,
                           deliveryCount |-> msg.deliveryCount + 1]}
                   /\ UNCHANGED blobDeadLettered
                \/ /\ msg.deliveryCount = MaxDeliveryCount
                   /\ UNCHANGED sbMessages
                   /\ blobDeadLettered' = [blobDeadLettered EXCEPT ![msg.taskId] = TRUE]
       /\ WorkerReset(w)
       /\ UNCHANGED <<blobStatus, blobEtag>>
       /\ UNCHANGED reVarsNoWorker

\* ===== Crash =====
\* Worker crashes: observationally identical to LockExpiry.
Crash(w) == LockExpiry(w)

\* ===== RetriesExhaustedOnReceive (NEW / fixed ordering) =====
\* Fires from ReadForIdempotency when the task arrives with attemptsExhausted=TRUE
\* (i.e., the worker sees `not self.task.can_retry` after recording the attempt).
\*
\* NEW (fixed) protocol (boilermaker/evaluators/task_graph.py:498-547):
\*   1. Write RetriesExhausted to the blob.
\*   2. Enter LoadingGraph -> CheckingMismatch -> AcquiringLease -> dispatch.
\*   3. On dispatch success (FinishAcquiringSuccess): mark
\*      retriesExhaustedDispatched[t] and proceed to Completing (settle msg).
\*   4. On dispatch failure (FinishAcquiringDispatchFailed): leave the message
\*      unsettled — Service Bus redelivers it.  On redelivery the existing
\*      terminal-status read branch in ReadForIdempotency re-enters dispatch.
\*
\* Preconditions: attemptsExhausted[t]=TRUE, blob status non-terminal (otherwise
\* the existing terminal-status branch handles it).
RetriesExhaustedOnReceive(w) ==
    /\ ~BuggyRetriesExhaustedOrdering
    /\ workerPhase[w] = "ReadForIdempotency"
    /\ LET t == workerTask[w]
       IN
       /\ attemptsExhausted[t]
       /\ blobStatus[t] \notin TerminalStatuses
       \* Same lease guard as WriteResult: Azure Blob Storage rejects non-lease
       \* writes (409) against a leased blob.  The scheduler will release the
       \* lease before this worker can proceed.
       /\ ~blobLeased[t]
       \* Write RetriesExhausted to the blob (unconditional; we already own
       \* execution from the prior Started write of an earlier delivery).
       /\ blobStatus' = [blobStatus EXCEPT ![t] = "RetriesExhausted"]
       /\ blobEtag'   = [blobEtag   EXCEPT ![t] = blobEtag[t] + 1]
       /\ workerResult' = [workerResult EXCEPT ![w] = "RetriesExhausted"]
       /\ workerCapturedEtag' = [workerCapturedEtag EXCEPT ![w] = blobEtag[t] + 1]
       /\ workerRetriesExhaustedDispatch' =
             [workerRetriesExhaustedDispatch EXCEPT ![w] = TRUE]
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "LoadingGraph"]
       /\ UNCHANGED <<blobLeased, blobDeadLettered, sbMessages, workerMsg, workerTask,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED <<attemptsExhausted, retriesExhaustedDispatched,
                      buggyPreSettledParent>>

\* ===== RetriesExhaustedOnReceiveBuggy (OLD / pre-fix ordering) =====
\* Models the buggy ordering that existed before the fix on this branch:
\*   1. Settle (deadletter) the parent message FIRST.
\*   2. Write RetriesExhausted blob.
\*   3. Enter LoadingGraph -> ... -> dispatch.
\*   4. On dispatch failure: the parent message is gone permanently (deadlettered);
\*      failure callbacks are orphaned.  retriesExhaustedDispatched stays FALSE
\*      and there is no recovery path.
\*
\* This action is only enabled when CONSTANT BuggyRetriesExhaustedOrdering=TRUE.
\* It is used to validate that the spec exercises the orphaning counterexample:
\* with the OLD ordering, TLC must find a reachable state violating
\* NoOrphanedRetriesExhausted; with the NEW ordering, the invariant holds.
\*
\* We model the pre-settle as removing the message from sbMessages permanently
\* (it goes to DLQ) and setting blobDeadLettered[t]=TRUE.
RetriesExhaustedOnReceiveBuggy(w) ==
    /\ BuggyRetriesExhaustedOrdering
    /\ workerPhase[w] = "ReadForIdempotency"
    /\ LET t == workerTask[w]
       IN
       /\ attemptsExhausted[t]
       /\ blobStatus[t] \notin TerminalStatuses
       \* Same lease guard as the NEW variant (Azure 409 on non-lease writes).
       /\ ~blobLeased[t]
       \* Step 1: pre-settle (deadletter) the parent message.  In the real OLD
       \* code this is `deadletter_or_complete_task("ProcessingError", ...)`.
       \* In the model the message was already removed from sbMessages at
       \* ReceiveMessage; we model the deadletter by setting blobDeadLettered
       \* AND by setting buggyPreSettledParent (which distinguishes "settled by
       \* the bug" from "settled by lock-expiry exhaustion").
       /\ blobDeadLettered' = [blobDeadLettered EXCEPT ![t] = TRUE]
       /\ buggyPreSettledParent' = [buggyPreSettledParent EXCEPT ![t] = TRUE]
       \* Step 2: write RetriesExhausted blob.
       /\ blobStatus' = [blobStatus EXCEPT ![t] = "RetriesExhausted"]
       /\ blobEtag'   = [blobEtag   EXCEPT ![t] = blobEtag[t] + 1]
       /\ workerResult' = [workerResult EXCEPT ![w] = "RetriesExhausted"]
       /\ workerCapturedEtag' = [workerCapturedEtag EXCEPT ![w] = blobEtag[t] + 1]
       /\ workerRetriesExhaustedDispatch' =
             [workerRetriesExhaustedDispatch EXCEPT ![w] = TRUE]
       \* Step 3: enter the dispatch pipeline.
       /\ workerPhase' = [workerPhase EXCEPT ![w] = "LoadingGraph"]
       /\ UNCHANGED <<blobLeased, sbMessages, workerMsg, workerTask,
                       workerSnapshot, workerSnapshotEtags,
                       workerReadySet, workerCurrentReady, workerDispatchFailed>>
       /\ UNCHANGED <<attemptsExhausted, retriesExhaustedDispatched>>
       \* buggyPreSettledParent[t] is set above; no UNCHANGED for it here.

\* ---------------------------------------------------------------------------
\* Next-state relation
\* ---------------------------------------------------------------------------

\* PublishTaskFail and AcquireLeaseStorageError are now included in Next.
\*
\* Previously, PublishTaskFail was excluded because publish failures were
\* silently swallowed — the parent message was settled and the child task
\* was orphaned in Pending forever.  Recovery relied on a dead-letter
\* reprocessor outside this spec's scope.
\*
\* With the _DispatchOutcome change, publish failures and lease-acquisition
\* storage errors now set workerDispatchFailed=TRUE.  After the scheduling
\* loop completes, FinishAcquiringDispatchFailed returns the parent message
\* to Service Bus (ContinueGraphError) instead of settling it.  On
\* redelivery, the idempotent guard skips re-executing the finished parent
\* and retries continue_graph; SB duplicate-detection suppresses re-publishing
\* tasks that already succeeded.  This makes PublishTaskFail a recoverable
\* fault within the protocol rather than a permanent stall.
Next ==
    \E w \in Workers :
        \/ ReceiveMessage(w)
        \/ ReadForIdempotency(w)
        \/ RetriesExhaustedOnReceive(w)
        \/ RetriesExhaustedOnReceiveBuggy(w)
        \/ WriteStartedSuccess(w)
        \/ WriteStarted412(w)
        \/ WriteStartedNon412Error(w)
        \/ RetryStartedWriteSuccess(w)
        \/ RetryStartedWrite412(w)
        \/ RetryStartedWriteNon412Error(w)
        \/ RereadAfterRetrySettle(w)
        \/ RereadAfterRetryNoSettle(w)
        \/ ExecuteTask(w)
        \/ WriteResult(w)
        \/ LoadGraph(w)
        \/ StatusMismatchCheck(w)
        \/ AcquireLeaseSuccess(w)
        \/ AcquireLeaseFail(w)
        \/ AcquireLeaseStorageError(w)
        \/ FinishAcquiringSuccess(w)
        \/ FinishAcquiringDispatchFailed(w)
        \/ PublishTaskSuccess(w)
        \/ PublishTaskFail(w)
        \/ WriteScheduledSuccess(w)
        \/ WriteScheduledFail(w)
        \/ ReleaseLease(w)
        \/ CompleteMessage(w)
        \/ LockExpiry(w)

\* ---------------------------------------------------------------------------
\* Safety Invariants
\* ---------------------------------------------------------------------------

\* NoSchedulingWithPendingAntecedent: a task cannot be Scheduled (or later) if
\* any antecedent is still Pending.  The ready-task filter in StatusMismatchCheck
\* enforces this via the snapshot; we verify the global state never violates it.
NoSchedulingWithPendingAntecedent ==
    \A t \in Tasks :
        blobStatus[t] \in {"Scheduled", "Started", "Success", "Failure", "RetriesExhausted"}
        => \A parent \in Antecedents(t) :
            blobStatus[parent] /= "Pending"

\* NoPermStuck: no task is permanently Scheduled with no SB message and no worker
\* actively processing it — UNLESS the task was dead-lettered.
\*
\* Dead-letter exhaustion (MaxDeliveryCount lock expiries) leaves the blob in
\* Scheduled with no SB message. This is handled by a separate dead-letter
\* reprocessing process and is intentionally excluded from this invariant.
\*
\* We express this as a safety invariant on the current state:
\* If a task is Scheduled, has no SB message in the queue, is not dead-lettered,
\* and no worker is in a phase that will either deliver a message for it or
\* transition it forward, then the task is stuck.
NoPermStuck ==
    \A t \in Tasks :
        /\ blobStatus[t] = "Scheduled"
        /\ ~blobDeadLettered[t]
        /\ ~\E msg \in sbMessages : msg.taskId = t
        /\ ~blobLeased[t]
        /\ ~\E w \in Workers : workerCurrentReady[w] = t
        => \* Some worker is in a phase that will eventually deliver a message for t
           \* or transition t forward. If none, the task is stuck.
           \E w \in Workers :
               /\ workerTask[w] = t
               /\ workerPhase[w] \notin {"Idle"}

\* ResultWriteNotBlockedByLease: if a worker is in the Executing phase and
\* the blob lease is held on its task, then some scheduler (possibly the same
\* worker, possibly another) must be in a phase that will release that lease.
\*
\* The publish-before-store ordering in continue_graph means a worker can
\* receive and start processing a task (Pending -> Started via CAS) while
\* the scheduler still holds the lease (WritingScheduled or ReleasingLease).
\* This is safe because:
\*   1. WriteResult guards on ~blobLeased[t] (Azure returns 409 on non-lease
\*      writes to a leased blob), so the worker blocks at WroteResult.
\*   2. The scheduler will release the lease via ReleaseLease (or LockExpiry
\*      if it crashes), after which WriteResult becomes enabled.
\*   3. WF_vars(WriteResult(w)) ensures progress once the lease is released.
\*
\* The invariant verifies that whenever this race window is open, the lease
\* WILL be released: some worker is in WritingScheduled or ReleasingLease
\* for that task (i.e., workerCurrentReady[scheduler] = t).
ResultWriteNotBlockedByLease ==
    \A w \in Workers :
        workerPhase[w] = "Executing"
        => \/ ~blobLeased[workerTask[w]]
           \/ \* Lease is held, but a scheduler is in a phase that will
              \* eventually release it (PublishingTask -> WritingScheduled
              \* -> ReleasingLease, or LockExpiry from any of these).
              \E scheduler \in Workers :
                  /\ workerCurrentReady[scheduler] = workerTask[w]
                  /\ workerPhase[scheduler] \in
                      {"PublishingTask", "WritingScheduled", "ReleasingLease"}

\* NoOrphanedRetriesExhausted: a task whose blob status is RetriesExhausted
\* must EITHER have its failure callbacks already dispatched
\* (retriesExhaustedDispatched[t] = TRUE), OR have its parent SB message
\* still available for redelivery (in sbMessages, or in flight at a worker
\* that has not yet given up).
\*
\* Equivalently (contrapositive — the bad state we want to assert is
\* unreachable): there is no task t such that:
\*   - blob is RetriesExhausted,
\*   - failure callbacks were never dispatched,
\*   - no SB message is queued for t,
\*   - no worker holds t's message in a non-Idle phase.
\*
\* The OLD (buggy) RetriesExhausted-on-receive ordering settles the parent
\* message BEFORE running continue_graph.  If continue_graph then fails
\* (e.g., load_graph transient error), the parent is already gone and the
\* failure callbacks are orphaned.  TLC must find this state as a reachable
\* counterexample when BuggyRetriesExhaustedOrdering=TRUE.
\*
\* The NEW (fixed) ordering writes RetriesExhausted, then runs
\* continue_graph, then settles only on success.  This invariant must hold
\* when BuggyRetriesExhaustedOrdering=FALSE.
\*
\* As with NoPermStuck/NoOrphanedScheduledFinal, blobDeadLettered is the
\* spec's representation of "permanent loss of the SB message".  The OLD
\* ordering sets blobDeadLettered[t] = TRUE in RetriesExhaustedOnReceiveBuggy
\* — so a violation under the OLD ordering manifests as
\* blobDeadLettered[t] /\ blobStatus[t]="RetriesExhausted" /\
\* ~retriesExhaustedDispatched[t] with no in-flight worker.
NoOrphanedRetriesExhausted ==
    \A t \in Tasks :
        ~ ( /\ blobStatus[t] = "RetriesExhausted"
            /\ ~retriesExhaustedDispatched[t]
            \* Exclude tasks that ended up dead-lettered via ordinary
            \* lock-expiry exhaustion of MaxDeliveryCount — this is the
            \* same fault that NoOrphanedScheduledFinal excludes.  A
            \* lock-expiry orphaning is recoverable via the separate
            \* dead-letter reprocessor.  The orphaning we want to detect
            \* here is the BUGGY pre-settle, which is unrecoverable
            \* because no SB message ever existed for reprocessing once
            \* it was deadlettered by the bug.
            /\ ~( blobDeadLettered[t] /\ ~buggyPreSettledParent[t] )
            /\ ~(\E msg \in sbMessages : msg.taskId = t)
            /\ ~(\E w \in Workers :
                    /\ workerTask[w] = t
                    /\ workerPhase[w] /= "Idle") )

\* ---------------------------------------------------------------------------
\* Liveness / Temporal Properties
\* ---------------------------------------------------------------------------

Fairness ==
    \A w \in Workers :
        /\ WF_vars(ReceiveMessage(w))
        /\ WF_vars(ReadForIdempotency(w))
        /\ WF_vars(RetriesExhaustedOnReceive(w))
        /\ WF_vars(RetriesExhaustedOnReceiveBuggy(w))
        /\ WF_vars(WriteStartedSuccess(w))
        /\ WF_vars(WriteStarted412(w))
        /\ WF_vars(WriteStartedNon412Error(w))
        /\ WF_vars(RetryStartedWriteSuccess(w))
        /\ WF_vars(RetryStartedWrite412(w))
        /\ WF_vars(RetryStartedWriteNon412Error(w))
        /\ WF_vars(RereadAfterRetrySettle(w))
        /\ WF_vars(RereadAfterRetryNoSettle(w))
        /\ WF_vars(ExecuteTask(w))
        /\ WF_vars(WriteResult(w))
        /\ WF_vars(LoadGraph(w))
        /\ WF_vars(StatusMismatchCheck(w))
        /\ WF_vars(AcquireLeaseSuccess(w))
        /\ WF_vars(AcquireLeaseFail(w))
        /\ WF_vars(FinishAcquiringSuccess(w))
        /\ WF_vars(FinishAcquiringDispatchFailed(w))
        /\ WF_vars(PublishTaskSuccess(w))
        /\ WF_vars(WriteScheduledSuccess(w))
        /\ WF_vars(ReleaseLease(w))
        /\ WF_vars(CompleteMessage(w))
        \* No fairness on fault actions:
        \*   LockExpiry/Crash            -- SB lock expiry or worker crash
        \*   WriteScheduledFail          -- blob write failure (SB message already published
        \*                                  so the task is reachable; real impl retries)
        \*   PublishTaskFail             -- SB publish failure (sets dispatch_failed;
        \*                                  parent redelivered on FinishAcquiringDispatchFailed)
        \*   AcquireLeaseStorageError    -- lease acquisition storage error (sets dispatch_failed)

\* EventualCompletion: absent dead-letter exhaustion, every task eventually
\* reaches a terminal state or is blocked by a failed antecedent.
\*
\* Dead-letter exhaustion breaks the liveness contract: a dead-lettered
\* message can prevent continue_graph from ever scheduling downstream tasks
\* (even if the task itself succeeded).  A separate reprocessing process
\* (outside this spec) recovers from dead-letter exhaustion.  We therefore
\* condition the property on NO dead-lettering occurring in the run.
\*
\* Within non-dead-lettered runs, tasks blocked by a non-Success antecedent
\* stay Pending by design (the graph only schedules children of Success).
\* "Blocked" is transitive through the DAG.
EventualCompletion ==
    [](\A t \in Tasks : ~blobDeadLettered[t])
    => <>(\A t \in Tasks :
            \/ blobStatus[t] \in TerminalStatuses
            \/ /\ blobStatus[t] = "Pending"
               /\ \E parent \in Antecedents(t) :
                      blobStatus[parent] /= "Success")

\* EventualScheduling: absent dead-letter exhaustion, if all antecedents of
\* a task have succeeded and remain in Success, the task is eventually
\* scheduled (reaches at least Scheduled status) or reaches terminal.
\*
\* The antecedent must hold continuously ([] not just once) because
\* WriteResult is unconditional -- a redelivered message can overwrite a
\* Success result with Failure, invalidating the scheduling assumption.
\* We use []P ~> Q to express "if P holds forever after some point, Q".
\*
\* Dead-letter exhaustion can prevent continue_graph from completing the
\* scheduling pass, so this property is conditioned on no dead-lettering.
\*
\* COVERAGE OF FAILURE CALLBACKS (RetriesExhausted dispatch):
\* The dependency-edge ready-set computation in StatusMismatchCheck only
\* yields children whose antecedents are all "Success", so this property as
\* originally stated does not cover failure-callback scheduling when a
\* parent terminates via "Failure" or "RetriesExhausted".  The companion
\* property RetriesExhaustedCallbacksDispatched below covers that case via
\* the retriesExhaustedDispatched flag, which is set when a
\* RetriesExhausted-on-receive dispatch pass reaches FinishAcquiringSuccess.
EventualScheduling ==
    [](\A t \in Tasks : ~blobDeadLettered[t])
    => \A t \in Tasks \ RootTasks :
        [](\A parent \in Antecedents(t) : blobStatus[parent] = "Success")
        ~> blobStatus[t] \in {"Scheduled", "Started", "Success", "Failure", "RetriesExhausted"}

\* RetriesExhaustedCallbacksDispatched: if a task ever reaches RetriesExhausted
\* blob status, then absent dead-letter exhaustion, the failure callbacks for
\* that task must eventually be dispatched (retriesExhaustedDispatched[t]
\* becomes TRUE).
\*
\* Conditioned on no dead-lettering, exactly like EventualScheduling /
\* NoOrphanedScheduled: a lock-expiry exhaustion can leave the task without
\* a deliverable SB message and recovery requires the separate dead-letter
\* reprocessor.  The orphaning bug we want to expose with this property is
\* DIFFERENT — it is the buggy pre-settle, which the safety invariant
\* NoOrphanedRetriesExhausted detects directly.
\*
\* On the NEW (fixed) code path with no dead-lettering: redelivery + the
\* idempotent terminal-status branch eventually drive a worker through
\* FinishAcquiringSuccess.  Property holds.
RetriesExhaustedCallbacksDispatched ==
    [](\A t \in Tasks : ~blobDeadLettered[t])
    => \A t \in Tasks :
           blobStatus[t] = "RetriesExhausted"
           ~> retriesExhaustedDispatched[t]

\* NoOrphanedScheduled: absent dead-letter exhaustion, every task that
\* reaches Scheduled eventually gets processed (reaches Started or terminal).
\*
\* Dead-lettering a Scheduled task's message leaves it orphaned until the
\* reprocessor intervenes.  Conditioned on no dead-lettering.
NoOrphanedScheduled ==
    [](\A t \in Tasks : ~blobDeadLettered[t])
    => \A t \in Tasks :
        blobStatus[t] = "Scheduled"
        ~> blobStatus[t] \in {"Started", "Success", "Failure", "RetriesExhausted"}

\* NoOrphanedScheduledFinal: same as NoOrphanedScheduled but expressed as a
\* safety invariant that TLC can find as a reachable bad state.
\* A task is "orphaned" if it is Scheduled with no SB message, no active lease,
\* and no worker is actively working on it in a way that will progress it.
\* Dead-lettered tasks are excluded: their SB message was exhausted and a
\* separate dead-letter reprocessing process handles them.
NoOrphanedScheduledFinal ==
    ~\E t \in Tasks :
        /\ blobStatus[t] = "Scheduled"
        /\ ~blobDeadLettered[t]
        /\ ~\E msg \in sbMessages : msg.taskId = t
        /\ ~blobLeased[t]
        /\ ~\E w \in Workers :
               \/ (workerTask[w] = t /\ workerPhase[w] /= "Idle")
               \/ workerCurrentReady[w] = t

\* EventualResultWrite: every worker that reaches Executing eventually reaches
\* WroteResult (i.e., the result write is never permanently blocked by a lease).
\* This holds because ResultWriteNotBlockedByLease guarantees the lease is not
\* held when a worker enters Executing, and WF_vars(WriteResult(w)) in Fairness
\* ensures the action eventually fires once enabled.
EventualResultWrite ==
    \A w \in Workers :
        workerPhase[w] = "Executing"
        ~> workerPhase[w] = "WroteResult"

\* ---------------------------------------------------------------------------
\* State constraint for bounded model checking
\* ---------------------------------------------------------------------------

\* Etag values are protocol-bounded.  With lease acquire no longer bumping the
\* ETag, the maximum number of ETag advances per task is:
\*   1  (Pending write in Init or continue_graph)
\* + 1  (Scheduled write by continue_graph)
\* + MaxDeliveryCount * 2  (Started + result write per delivery attempt)
\* + MaxDeliveryCount * 1  (RetriesExhausted-on-receive write per delivery
\*                          attempt; only on the exhausted-on-receive path,
\*                          replaces the Started+result writes in that path)
\* <= 3 * MaxDeliveryCount + 2  (overapproximate)
\*
\* The bound of 3 * MaxDeliveryCount + 4 retains buffer.
EtagBound == \A t \in Tasks : blobEtag[t] <= 3 * MaxDeliveryCount + 4

\* ---------------------------------------------------------------------------
\* Specification
\* ---------------------------------------------------------------------------

Spec == Init /\ [][Next]_vars /\ Fairness

=============================================================================
