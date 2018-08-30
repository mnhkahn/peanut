// Package api
package api

func InitApi() error {
	var err error

	err = InitRouter()
	if err != nil {
		return err
	}

	return nil
}
