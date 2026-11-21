package devicemanager

import "gpustack.ai/gpustack/pkg/devicemanager/detector"

// MonitorSnapshotPath is the webserver path that serves the latest accelerator monitor snapshot.
const MonitorSnapshotPath = "/monitor/snapshot"

// MonitorSnapshot is the JSON envelope served at MonitorSnapshotPath: the latest accelerator
// metrics sample plus the time the device manager stored it.
type MonitorSnapshot = detector.MonitorSnapshot
