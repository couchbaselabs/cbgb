package cbgb

type VBState uint8

const (
	_ = VBState(iota)
	VBActive
	VBReplica
	VBPending
	VBDead
)

var vbStateNames = []string{
	VBActive:  "active",
	VBReplica: "replica",
	VBPending: "pending",
	VBDead:    "dead",
}

func (v VBState) String() string {
	if v < VBActive || v > VBDead {
		panic("Invalid vb state")
	}
	return vbStateNames[v]
}

func parseVBState(s string) VBState {
	for i, v := range vbStateNames {
		if v != "" && v == s {
			return VBState(uint8(i))
		}
	}
	return VBDead
}
