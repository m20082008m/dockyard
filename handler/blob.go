package handler

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/astaxie/beego/logs"
	"github.com/satori/go.uuid"
	"gopkg.in/macaron.v1"

	"github.com/containerops/dockyard/models"
	"github.com/containerops/dockyard/module"
	"github.com/containerops/dockyard/utils"
	"github.com/containerops/dockyard/utils/setting"
)

func HeadBlobsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	digest := ctx.Params(":digest")
	tarsum := strings.Split(digest, ":")[1]

	ctx.Resp.Header().Set("Content-Type", "application/json; charset=utf-8")
	i := new(models.Image)
	if exists, err := i.Get(tarsum); err != nil {
		log.Info("[REGISTRY API V2] Failed to get blob %v: %v", tarsum, err.Error())

		result, _ := module.FormatErr(module.DIGEST_INVALID, "blob invalid", digest)
		return http.StatusBadRequest, result
	} else if !exists {
		log.Info("[REGISTRY API V2] Not found blob: %v", tarsum)

		result, _ := module.FormatErr(module.DIGEST_INVALID, "blob invalid", digest)
		return http.StatusNotFound, result
	}

	ctx.Resp.Header().Set("Content-Type", "application/octet-stream")
	ctx.Resp.Header().Set("Docker-Content-Digest", digest)
	ctx.Resp.Header().Set("Content-Length", fmt.Sprint(i.Size))

	return http.StatusOK, []byte{}
}

func PostBlobsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	namespace := ctx.Params(":namespace")
	repository := ctx.Params(":repository")

	uuid := utils.MD5(uuid.NewV4().String())
	state := utils.MD5(fmt.Sprintf("%s/%s/%s", namespace, repository, time.Now().UnixNano()/int64(time.Millisecond)))
	random := fmt.Sprintf("%s://%s/v2/%s/%s/blobs/uploads/%s?_state=%s",
		setting.ListenMode,
		setting.Domains,
		namespace,
		repository,
		uuid,
		state)

	ctx.Resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
	ctx.Resp.Header().Set("Docker-Upload-Uuid", uuid)
	ctx.Resp.Header().Set("Location", random)
	ctx.Resp.Header().Set("Range", "0-0")

	return http.StatusAccepted, []byte{}
}

func PatchBlobsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	namespace := ctx.Params(":namespace")
	repository := ctx.Params(":repository")

	desc := ctx.Params(":uuid")
	uuid := strings.Split(desc, "?")[0]

	imagePathTmp := module.GetImagePath(uuid, setting.APIVERSION_V2)
	layerPathTmp := module.GetLayerPath(uuid, "layer", setting.APIVERSION_V2)

	//saving specific tarsum every times is in order to split the same tarsum in HEAD handler
	if !utils.IsDirExist(imagePathTmp) {
		os.MkdirAll(imagePathTmp, os.ModePerm)
	}

	if _, err := os.Stat(layerPathTmp); err == nil {
		os.Remove(layerPathTmp)
	}

	data, _ := ctx.Req.Body().Bytes()
	if err := ioutil.WriteFile(layerPathTmp, data, 0777); err != nil {
		log.Error("[REGISTRY API V2] Failed to save layer %v: %v", layerPathTmp, err.Error())

		detail := map[string]string{"Name": namespace + "/" + repository}
		result, _ := module.FormatErr(module.BLOB_UPLOAD_INVALID, "blob upload invalid", detail)
		return http.StatusInternalServerError, result
	}

	state := utils.MD5(fmt.Sprintf("%s/%s/%s", namespace, repository, time.Now().UnixNano()/int64(time.Millisecond)))
	random := fmt.Sprintf("%s://%s/v2/%s/%s/blobs/uploads/%s?_state=%s",
		setting.ListenMode,
		setting.Domains,
		namespace,
		repository,
		uuid,
		state)

	ctx.Resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
	ctx.Resp.Header().Set("Docker-Upload-Uuid", uuid)
	ctx.Resp.Header().Set("Location", random)
	ctx.Resp.Header().Set("Range", fmt.Sprintf("0-%v", len(data)-1))

	return http.StatusAccepted, []byte{}
}

func PutBlobsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	desc := ctx.Params(":uuid")
	uuid := strings.Split(desc, "?")[0]

	digest := ctx.Query("digest")
	tarsum := strings.Split(digest, ":")[1]

	imagePathTmp := module.GetImagePath(uuid, setting.APIVERSION_V2)
	layerPathTmp := module.GetLayerPath(uuid, "layer", setting.APIVERSION_V2)
	imagePath := module.GetImagePath(tarsum, setting.APIVERSION_V2)
	layerPath := module.GetLayerPath(tarsum, "layer", setting.APIVERSION_V2)

	reqbody, _ := ctx.Req.Body().Bytes()
	layerlen, err := module.SaveLayerLocal(imagePathTmp, layerPathTmp, imagePath, layerPath, reqbody)
	if err != nil {
		log.Error("[REGISTRY API V2] Failed to save layer %v: %v", layerPath, err.Error())

		result, _ := module.FormatErr(module.BLOB_UPLOAD_INVALID, "blob upload invalid", err.Error())
		return http.StatusInternalServerError, result
	}

	//saving specific tarsum every times is in order to split the same tarsum in HEAD handler
	i := new(models.Image)
	i.Path, i.Size = layerPath, int64(layerlen)
	if err := i.Save(tarsum); err != nil {
		log.Error("[REGISTRY API V2] Failed to save blob description %v: %v", tarsum, err.Error())

		result, _ := module.FormatErr(module.BLOB_UPLOAD_INVALID, "blob upload invalid", err.Error())
		return http.StatusBadRequest, result
	}

	random := fmt.Sprintf("%s://%s/v2/%s/%s/blobs/%s",
		setting.ListenMode,
		setting.Domains,
		ctx.Params(":namespace"),
		ctx.Params(":repository"),
		digest)

	ctx.Resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
	ctx.Resp.Header().Set("Docker-Content-Digest", digest)
	ctx.Resp.Header().Set("Location", random)

	return http.StatusCreated, []byte{}
}

func GetBlobsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	digest := ctx.Params(":digest")
	tarsum := strings.Split(digest, ":")[1]

	i := new(models.Image)
	if exists, err := i.Get(tarsum); err != nil {
		log.Error("[REGISTRY API V2] Failed to get blob %v: %v", tarsum, err.Error())

		result, _ := module.FormatErr(module.BLOB_UNKNOWN, "blob unknown to Dockyard", digest)
		return http.StatusBadRequest, result
	} else if !exists {
		log.Error("[REGISTRY API V2] Not found blob: %v: %v", tarsum, err.Error())

		result, _ := module.FormatErr(module.BLOB_UNKNOWN, "blob not found", digest)
		return http.StatusNotFound, result
	}

	layer, err := module.DownloadLayer(i.Path)
	if err != nil {
		log.Error("[REGISTRY API V2] Failed to get layer: %v", err.Error())

		result, _ := module.FormatErr(module.BLOB_UNKNOWN, "blob donwload failed", digest)
		return http.StatusInternalServerError, result
	}

	ctx.Resp.Header().Set("Content-Type", "application/octet-stream")
	ctx.Resp.Header().Set("Docker-Content-Digest", digest)
	ctx.Resp.Header().Set("Content-Length", fmt.Sprint(len(layer)))

	return http.StatusOK, layer
}

func DeleteBlobsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	digest := ctx.Params(":digest")
	tarsum := strings.Split(digest, ":")[1]

	i := new(models.Image)
	if exists, err := i.Get(tarsum); err != nil {
		log.Error("[REGISTRY API V2] Failed to get blob %v: %v", tarsum, err.Error())

		result, _ := module.FormatErr(module.BLOB_UNKNOWN, "blob unknown to Dockyard", digest)
		return http.StatusBadRequest, result
	} else if !exists {
		log.Error("[REGISTRY API V2] Not found blob %v", tarsum)

		result, _ := module.FormatErr(module.BLOB_UNKNOWN, "blob not found", digest)
		return http.StatusBadRequest, result
	}

	i.Count = i.Count - 1
	//delete the blob when reference counting is 0
	if i.Count == 0 {
		//delete blob description in db
		if err := i.Delete(tarsum); err != nil {
			log.Error("[REGISTRY API V2] Failed to delete blob %v in db", tarsum)

			result, _ := module.FormatErr(module.BLOB_UNKNOWN, "delete blob description failed", digest)
			return http.StatusInternalServerError, result
		}
		//delete blob layer in local fs
		module.CleanCache(tarsum, setting.APIVERSION_V2)
	} else if i.Count > 0 { //reduce reference counting when the blob is used by the other repository
		if err := i.Save(tarsum); err != nil {
			log.Error("[REGISTRY API V2] Failed to update blob %v in db: %v", tarsum, err.Error())

			result, _ := module.FormatErr(module.BLOB_UNKNOWN, "update blob failed", digest)
			return http.StatusInternalServerError, result
		}
	} else {
		log.Error("[REGISTRY API V2] Exception in blob content", tarsum)

		result, _ := module.FormatErr(module.BLOB_UNKNOWN, "blob content invalid", digest)
		return http.StatusInternalServerError, result
	}

	ctx.Resp.Header().Set("Docker-Content-Digest", digest)
	ctx.Resp.Header().Set("Content-Length", "0")

	return http.StatusOK, []byte{}
}
