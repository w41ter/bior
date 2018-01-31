// Package core provides a basic implementions of raft consensus algorithm.
//
// It provides a `Raft` interface to operation raft state machine. caller
// must implement `NodeApplication` interface call by raft. On the same time,
// caller must periodic call `Raft.Periodic` in stable time interval, call
// `Raft.Ready` to achieve ready datas, and dispatch them. Such as persistence
// unstabled raft log entries, send raft entries to other nodes.
//
// Basic usage for `Raft` must be `Propose`, call it and pass binary data,
// and data will appear at `Ready.commitEntries` when majority nodes has been
// response. After this, you would safty apply it to state machine, and do not
// worry about a few nodes hang up the lost data.
//
// `Raft` provides read-only queries that are not distributed through the log,
// you can call `Raft.Read` pass unique ID as `context` for the read-only query.
//
// Of course, there will be recieved some data from others node, should call
// `Raft.Step` to handle it.
//
// Finaly, there are two situations should be cautious:
// 	- use `ProposeConfChange` instead of `Propose` when propose configuration change,
// 	when change has been reached state machine, then call `ApplyConfChange` notice raft
// 	apply change.
// 	- when raft call `NodeApplication.applySnapshot`, after persistence snapshot,
// 	should call `Raft.ApplySnapshot`, let it rebuild log informations.
package core
