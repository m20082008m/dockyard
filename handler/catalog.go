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

	var repolist []string
	r := new(models.Repository)
	repolist, err := r.List()
	if err != nil {
		detail := map[string]string{}
		result, _ := module.FormatErr(module.BLOB_UNKNOWN, "failed to find repositories", detail)
		return http.StatusBadRequest, result
	}
	result, _ := json.Marshal(map[string]interface{}{"repositories": repolist})
	return http.StatusOK, result
}
