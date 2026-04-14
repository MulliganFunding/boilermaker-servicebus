---------------------------- MODULE TaskGraph ----------------------------
EXTENDS TaskGraphSpecBase

WorkerSet == {"w1", "w2"}
TaskSet == {"t1", "t2", "t3"}
RootTaskSet == {"t1"}
DefaultEdges == {<<"t1", "t2">>, << "t2", "t3">>}
=============================================================================
