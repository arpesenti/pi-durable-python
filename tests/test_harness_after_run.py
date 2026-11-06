"""Deferral record for upstream ``test/harness-after-run.test.ts``: this port runs none of it.

No case here is translated, so this module holds no test. It exists because these 29 cases are what
#48's run-end criterion is written against -- run-end tasks run after the answer and record no failure
however the run ends -- and an absence this size must be named, not left silent.

What every case needs is the mechanism none of them can be written without, none of it in this port:
the generation's ``finish`` and ``drain`` phases (``generation.ts:361``-``:393``), the commit A that
keeps a run which answered running its own tasks (``holdRun``, ``generation.ts:905``), the read of
whether the phase's agent selects an ``afterRun`` hook (``asksAfterRun``, ``generation.ts:901``), the
off-the-line check of what a hook asked for (``prepareRunEndTasks``, ``generation.ts:929``), commit B
that places the final boundary once they are terminal (``endAnsweredRun``, ``generation.ts:953``), and
the ``runEndTask`` value a hook returns (``types.py`` has no ``run_end_task`` yet). The port has the
other side of the shape already: ``end_run`` takes the run's end, so a run that answered and is left
running only its run-end obligations records no ``runFailure`` however it ends, which
test_harness_run_failure.py asserts, and ``GLOSSARY.md`` defines the term.

The cases, in upstream order. Each needs the mechanism above; these also need what is named after them:
a run that continues itself through ``onYield`` before it answers; a terminate round's handoff or reset
as the run's last entry; a queue that fills while the tasks run; a crash before the tasks exist or in
``finish``; a Stop or an abort reaching the run or one of its tasks; a task that cannot run until Stop
orphans it; a follow-up a task admits; a compaction that summarized the start of the run; a fork run
idle on its after-run tasks; the restart cutoff reached in ``finish`` or in an after-run task; a limit
lowered so no ``task_interrupted`` is owed; a run whose ``first`` was never recorded; and a failed run's
history handed to a continue. The last two are unit cases on the run's end itself -- the second is
covered by test_harness_run_failure.py, the first is the run the port must not touch.

"waits for the tasks an answer's hooks ask for: inputs done at the answer, the run busy until they end",
"asks after a terminate round, the run's last entry its newest result or its handoff's reset",
"ends the run in one commit without an afterRun hook, and after the hook when it asks for nothing",
"queues input during the tasks and places it after them: writes first, then steers and follow-ups",
"places a reset queued during the tasks after them",
"asks once a run really ends: not while onYield continues it, and when a queued steer drops the continuation",
"asks the hooks again after a crash before the tasks exist, and never once they do",
"ends an answered run without a failure when Stop comes while the hooks run or the tasks run",
"ends the run normally when one of its tasks is aborted on its own",
"reports broken requests and hooks and creates the rest",
"creates a requested task from the installed definition, calling its initial() once, off the line",
"lets a hook read the run's history, tool results included",
"lets a hook read the run's history after a compaction summarized the start of the run",
"ends an answered run completed, without a failure, once finish was cut off restartLimit times",
"keeps the run busy while an after-run task cannot run, until Stop orphans it",
"starts a fix run with a follow-up a task admits, after the tasks",
"gives a continue after a failed run the failed run's history",
"ends the run without asking when the hooks are deselected before finish runs",
"starts a fork of a conversation running its after-run tasks idle",
"gives a run without a recorded first entry its answer as first",
"ends the run normally when its after-run task is cut off restartLimit times",
"reports no task_interrupted for a task that ends normally after the limit was lowered",
"reports no task_interrupted for a generation that ends normally after the limit was lowered in finish",
"asks after a terminate round at the turn limit: terminate wins",
"starts a continue's history at its first tool round when nothing failed and nothing was placed",
"leaves a summary queued during the tasks stale behind a terminate round's handoff",
"stores the answer's held progress with commit A, and asks again after an orderly close in finish",
"leaves a run it does not own alone, and ends an answered run without a failure",
"copies a failed run's first entry into runFailure",
"""
