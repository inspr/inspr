package tree

import "gitlab.inspr.dev/inspr/core/pkg/meta"

// MockAppManager to Mock App Manager
type MockAppManager struct {
	*MockManager
	err error
}

// Get Mock
func (mock *MockAppManager) Get(query string) (*meta.App, error) {
	if mock.err != nil {
		return nil, mock.err
	}
	return mock.root, nil
}

// CreateApp Mock
func (mock *MockAppManager) CreateApp(context string, app *meta.App) error {
	return nil
}

// DeleteApp Mock
func (mock *MockAppManager) DeleteApp(query string) error {
	return nil
}

// UpdateApp Mock
func (mock *MockAppManager) UpdateApp(query string, app *meta.App) error {
	return nil
}
