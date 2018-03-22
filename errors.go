package backends

import "fmt"

// BackendErrorInfo holds the info for an error that occurred in the backend.
// It contains the error message - this is usually a code string - like "not found" or "duplicate".
// It also contains the error details - detailed error messages.
type BackendErrorInfo struct {
	Message string
	details string
}

// error interface
// Error returns the error message.
func (e *BackendErrorInfo) Error() string {
	if e != nil {
		return e.Message
	}
	return ""
}

// Details returns the detailed error message.
func (e *BackendErrorInfo) Details() string {
	if e != nil {
		return e.details
	}
	return ""
}

// BackendErrorFactory is a factory function for generating error objects.
type BackendErrorFactory func(...interface{}) error

// ErrorClass defines a backend error class with the specified message.
// Returns a BackendErrorFactory function for generating errors of this class.
// This function captures the message for the error class.
func ErrorClass(message string) BackendErrorFactory {
	return func(args ...interface{}) error {
		return &BackendErrorInfo{
			Message: message,
			details: toString(args),
		}
	}
}

func toString(args ...interface{}) string {
	strArgs := []string{}

	for _, arg := range args {
		strval := ""
		if argErr, ok := arg.(error); ok {
			strval = argErr.Error()
		} else if argStr, ok := arg.(string); ok {
			strval = argStr
		} else {
			strval = fmt.Sprintf("%v", arg)
		}
		strArgs = append(strArgs, strval)
	}

	return fmt.Sprint(strArgs)
}

// Some common errors

// ErrNotFound is the error class for errors returned when the desired enityt is not found.
var ErrNotFound = ErrorClass("not found")

// ErrAlreadyExists is an error class that captures duplication errors.
var ErrAlreadyExists = ErrorClass("already exists")

// ErrInvalidInput is a generic error class related to invalid input parameters specified on a backend function.
var ErrInvalidInput = ErrorClass("invalid input")

// ErrBackendError is a genering error class capturing errors that happened during processing in the backend.
var ErrBackendError = func(args ...interface{}) error {
	return &BackendErrorInfo{
		Message: toString(args),
	}
}

// IsErrorOfType checks if the suplied err is of the same type (backend error class) as some backend error.
func IsErrorOfType(err error, backendErr error) bool {
	return err.Error() == backendErr.Error()
}

// IsErrNotFound check of the error is of the ErrNotFound class.
func IsErrNotFound(err error) bool {
	return IsErrorOfType(err, ErrNotFound(""))
}

// IsErrAlreadyExistis check of the error is of the ErrAlreadyExists class.
func IsErrAlreadyExistis(err error) bool {
	return IsErrorOfType(err, ErrAlreadyExists(""))
}

// IsErrInvalidInput check of the error is of the ErrInvalidInput class.
func IsErrInvalidInput(err error) bool {
	return IsErrorOfType(err, ErrInvalidInput(""))
}
