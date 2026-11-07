package data

import "testing"

func TestActivityNodeRetryPolicies(t *testing.T) {
	repo := newNodeRepo(&Data{})
	nodes := append(repo.GetActivityNodes(),
		&saySomethingNode{},
		&exampleNode{},
	)
	for _, node := range nodes {
		if got := node.RetryPolicy().MaximumAttempts; got != 3 {
			t.Errorf("%s RetryPolicy().MaximumAttempts = %d, want 3", node.Type(), got)
		}
	}
}
