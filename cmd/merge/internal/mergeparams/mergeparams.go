package mergeparams

type MergeParams struct {
	prefixes []string
	force    bool
}

func NewMergeParams(prefixes []string, force bool) *MergeParams {
	return &MergeParams{
		prefixes: prefixes,
		force:    force,
	}
}

func (m *MergeParams) GetPrefixes() []string {
	return m.prefixes
}

func (m *MergeParams) IsForce() bool {
	return m.force
}
