package worker

// The elastic resize operation is a durable record plus a pure decision function over it. The record
// exists because a resize cannot be recovered by asking the engine what it thinks is happening: a
// request that timed out, a gateway that answered 408 or 500, and a request that was never sent at
// all are the same three states to every observer, and answering them differently is how a resize
// gets replayed against an engine that has already applied it.
//
// So the command intent is written BEFORE the request leaves, and a record that says a command was
// sent is never evidence that it may be sent again. Reconstruction after a restart reads that
// record and the fresh facts, and derives the same next action it would have derived before.

// elasticOperationLabel marks the stored object so it is findable and cannot be mistaken for an
// ordinary ConfigMap.
const elasticOperationLabel = "gpustack.ai/elastic-operation"
