package handler

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/astaxie/beego/logs"
	"gopkg.in/macaron.v1"

	"github.com/containerops/dockyard/models"
	"github.com/containerops/dockyard/module"
	"github.com/containerops/dockyard/utils/setting"
	"github.com/containerops/dockyard/utils/signature"
)

var ManifestCtx []byte

func PutManifestsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	//TODO: to consider parallel situation
	manifest := ManifestCtx
	defer func() {
		ManifestCtx = []byte{}
	}()

	namespace := ctx.Params(":namespace")
	repository := ctx.Params(":repository")
	agent := ctx.Req.Header.Get("User-Agent")
	tag := ctx.Params(":tag")

	if len(manifest) == 0 {
		manifest, _ = ctx.Req.Body().Bytes()
	}

	tarsumlist, err := module.GetTarsumlist(manifest)
	if err != nil {
		log.Error("[REGISTRY API V2] Failed to get tarsum in manifest")

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag}
		msg := "Failed to get tarsum info"
		result, _ := module.FormatErr(module.DIGEST_INVALID, msg, detail)
		return http.StatusBadRequest, result
	}

	if err := module.UpdateImgRefCnt(namespace, repository, tag, tarsumlist); err != nil {
		log.Error("[REGISTRY API V2] Failed to update image reference counting")

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag}
		msg := "Failed to update image reference counting"
		result, _ := module.FormatErr(module.DIGEST_INVALID, msg, detail)
		return http.StatusBadRequest, result
	}

	digest, err := signature.DigestManifest(manifest)
	if err != nil {
		log.Error("[REGISTRY API V2] Failed to get manifest digest: %v", err.Error())

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag, "Digest": digest}
		msg := "provided digest did not match uploaded content"
		result, _ := module.FormatErr(module.DIGEST_INVALID, msg, detail)
		return http.StatusBadRequest, result
	}

	r := new(models.Repository)
	if err := r.Put(namespace, repository, "", agent, setting.APIVERSION_V2); err != nil {
		log.Error("[REGISTRY API V2] Failed to save repository %v/%v: %v", namespace, repository, err.Error())

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag}
		result, _ := module.FormatErr(module.MANIFEST_BLOB_UNKNOWN, err.Error(), detail)
		return http.StatusInternalServerError, result
	}

	err, schema := module.ParseManifest(manifest, namespace, repository, tag)
	if err != nil {
		log.Error("[REGISTRY API V2] Failed to decode manifest: %v", err.Error())

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag}
		result, _ := module.FormatErr(module.MANIFEST_INVALID, err.Error(), detail)
		return http.StatusBadRequest, result
	}

	if err := module.UpdateTag(namespace, repository, tag, digest); err != nil {
		log.Error("[REGISTRY API V2] Failed to decode manifest: %v", err.Error())

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag}
		result, _ := module.FormatErr(module.MANIFEST_INVALID, err.Error(), detail)
		return http.StatusBadRequest, result
	}

	random := fmt.Sprintf("%s://%s/v2/%s/%s/manifests/%s",
		setting.ListenMode,
		setting.Domains,
		namespace,
		repository,
		digest)

	ctx.Resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
	ctx.Resp.Header().Set("Docker-Content-Digest", digest)
	ctx.Resp.Header().Set("Location", random)

	if err := module.UploadLayer(tarsumlist); err != nil {
		log.Error("[REGISTRY API V2] Failed to upload layer: %v", err)

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag}
		result, _ := module.FormatErr(module.BLOB_UPLOAD_INVALID, err.Error(), detail)
		return http.StatusBadRequest, result
	}

	var status = []int{http.StatusBadRequest, http.StatusAccepted, http.StatusCreated}
	return status[schema], []byte{}
}

func GetTagsListV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	namespace := ctx.Params(":namespace")
	repository := ctx.Params(":repository")

	r := new(models.Repository)
	if _, err := r.Get(namespace, repository); err != nil {
		log.Error("[REGISTRY API V2] Failed to get repository %v/%v: %v", namespace, repository, err.Error())

		detail := map[string]string{"Name": namespace + "/" + repository}
		result, _ := module.FormatErr(module.TAG_INVALID, err.Error(), detail)
		return http.StatusBadRequest, result
	}

	data := map[string]interface{}{}

	data["name"] = fmt.Sprintf("%s/%s", namespace, repository)

	tagslist := r.GetTagslist()
	if len(tagslist) <= 0 {
		log.Error("[REGISTRY API V2] Repository %v/%v tags not found", namespace, repository)

		detail := map[string]string{"Name": namespace + "/" + repository}
		result, _ := module.FormatErr(module.NAME_UNKNOWN, "repository name not known to registry", detail)
		return http.StatusNotFound, result
	}
	data["tags"] = tagslist

	result, _ := json.Marshal(data)
	return http.StatusOK, result
}

func GetManifestsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	namespace := ctx.Params(":namespace")
	repository := ctx.Params(":repository")
	tag := ctx.Params(":tag")

	t := new(models.Tag)
	if exists, err := t.Get(namespace, repository, tag); err != nil || !exists {
		log.Error("[REGISTRY API V2] Not found manifest: %v", err)

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag}
		result, _ := module.FormatErr(module.MANIFEST_UNKNOWN, "manifest unknown", detail)
		return http.StatusNotFound, result
	}

	digest, err := signature.DigestManifest([]byte(t.Manifest))
	if err != nil {
		log.Error("[REGISTRY API V2] Failed to get manifest digest: %v", err.Error())

		detail := map[string]string{"Name": namespace + "/" + repository, "Tag": tag}
		msg := "provided digest did not match uploaded content"
		result, _ := module.FormatErr(module.DIGEST_INVALID, msg, detail)
		return http.StatusInternalServerError, result
	}

	contenttype := []string{"", "application/json; charset=utf-8", "application/vnd.docker.distribution.manifest.v2+json"}
	ctx.Resp.Header().Set("Content-Type", contenttype[t.Schema])

	ctx.Resp.Header().Set("Content-Type", "application/json; charset=utf-8")
	ctx.Resp.Header().Set("Docker-Content-Digest", digest)
	ctx.Resp.Header().Set("Content-Length", fmt.Sprint(len(t.Manifest)))

	return http.StatusOK, []byte(t.Manifest)
}

func DeleteManifestsV2Handler(ctx *macaron.Context, log *logs.BeeLogger) (int, []byte) {
	//TODO: to consider parallel situation
	namespace := ctx.Params(":namespace")
	repository := ctx.Params(":repository")
	reference := ctx.Params(":reference")

	t := new(models.Tag)
	if exists, err := t.GetReference(namespace, repository, reference); err != nil || !exists {
		log.Info("[REGISTRY API V2] Failed to get tag : %v", err.Error())

		detail := map[string]string{"Name": namespace + "/" + repository, "Reference": reference}
		result, _ := module.FormatErr(module.MANIFEST_UNKNOWN, "manifest unknown", detail)
		return http.StatusBadRequest, result
	} else if !exists {
		detail := map[string]string{"Name": namespace + "/" + repository, "Reference": reference}
		result, _ := module.FormatErr(module.MANIFEST_UNKNOWN, "manifest unknown", detail)
		return http.StatusBadRequest, result
	}

	if err := module.UpdateTaglist(namespace, repository, t.Tag, t.Manifest); err != nil {
		detail := map[string]string{"Name": namespace + "/" + repository, "Reference": reference}
		result, _ := module.FormatErr(module.MANIFEST_UNKNOWN, "manifest unknown", detail)
		return http.StatusBadRequest, result
	}
	if err := t.DeleteReference(namespace, repository, reference); err != nil {
		detail := map[string]string{"Name": namespace + "/" + repository, "Reference": reference}
		result, _ := module.FormatErr(module.MANIFEST_INVALID, err.Error(), detail)
		return http.StatusBadRequest, result
	}

	result, _ := json.Marshal(map[string]string{})
	return http.StatusOK, result
}
