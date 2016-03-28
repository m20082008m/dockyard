package handler

import (
	"encoding/json"
	"net/http"

	"github.com/astaxie/beego/logs"
	"gopkg.in/macaron.v1"

	"github.com/containerops/dockyard/models"
	"github.com/containerops/dockyard/module"
)

func GetCatalogV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	r := new(models.Repository)

	repolist, err := r.List()
	if err != nil {
		log.Error("[REGISTRY API V2] Failed to list repositories: %v", err)

		result, _ := module.ReportError(module.UNKNOWN, err.Error())
		return http.StatusInternalServerError, result
	}

	result, _ := json.Marshal(map[string]interface{}{"repositories": repolist})
	return http.StatusOK, result
}
