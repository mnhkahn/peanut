// Package peanut
package peanut

import (
	"net"

	"github.com/mnhkahn/gogogo/app"
	"github.com/mnhkahn/gogogo/logger"
)

func InitPeanut() {
	limit := app.Int("handle_limit")
	if limit > 0 {
		app.LimitServe(app.Int("handle_limit"))
	} else {
		l, err := net.Listen("tcp", ":"+app.String("port"))
		if err != nil {
			logger.Errorf("Listen: %v", err)
			return
		}
		app.Serve(l)
	}
}
