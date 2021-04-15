package ierrors

// HasCode checks if an error is an InsprError and if it is,
//  checks if it has the error code given
func HasCode(target error, code InsprErrorCode) bool {
	t, ok := target.(*InsprError)
	if !ok {
		return false
	}
	return t.Code&code > 0
}

// IsIerror checks if an error is an InsprError and if it is,
//  checks if it has any type of error
func IsIerror(target error) bool {
	t, ok := target.(*InsprError)
	if !ok {
		return false
	}

	// going through all the errors of the ierror pkg
	if t.Code > 0 {
		return true
	}
	return false
}
