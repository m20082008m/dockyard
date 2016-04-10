package rados

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/ceph/go-ceph/rados"
	"github.com/containerops/dockyard/backend/driver"
	"github.com/containerops/wrench/setting"
)

const defaultChunkSize = 4 << 20
const defaultXattrTotalSizeName = "total-size"

func init() {
	driver.Register("rados", InitFunc)
}

func InitFunc() {
	driver.InjectReflect.Bind("rados", radossave)
}

type radosDriver struct {
	Conn      *rados.Conn
	Ioctx     *rados.IOContext
	chunksize uint64
}

type driverParameters struct {
	poolname  string
	username  string
	chunksize uint64
}

func new(d *radosDriver) error {
	var conn *rados.Conn
	var err error

	chunksize := uint64(defaultChunkSize)
	if setting.Chunksize != "" {
		tmp, err := strconv.Atoi(setting.Chunksize)
		if err != nil {
			return fmt.Errorf("The chunksize parameter should be a number")
		}
		chunksize = uint64(tmp)
	}
	params := driverParameters{
		fmt.Sprint(setting.Poolname),
		fmt.Sprint(setting.Username),
		chunksize,
	}

	if params.username != "" {
		fmt.Println("Opening connection to pool %s using user %s", params.poolname, params.username)
		conn, err = rados.NewConnWithUser(params.username)
	} else {
		fmt.Println("Opening connection to pool %s", params.poolname)
		conn, err = rados.NewConn()
	}

	if err != nil {
		return err
	}

	err = conn.ReadDefaultConfigFile()
	if err != nil {
		return err
	}

	err = conn.Connect()
	if err != nil {
		return err
	}

	ioctx, err := conn.OpenIOContext(params.poolname)
	fmt.Println("Connected to pool %s", params.poolname)

	if err != nil {
		return err
	}

	d = &radosDriver{
		Ioctx:     ioctx,
		Conn:      conn,
		chunksize: params.chunksize,
	}

	return nil
}

func radossave(file string) (string, error) {

	var key string

	for _, key = range strings.Split(file, "/") {

	}

	fin, err := os.Open(file)
	if err != nil {
		return "", err
	}
	defer fin.Close()

	d := &radosDriver{}
	err = new(d)
	if err != nil {
		return "", err
	}

	if _, err = d.WriteStream(key, 0, fin); err != nil {
		return "", err
	}

	return "", nil
}

func radosget(path string) ([]byte, error) {
	d := &radosDriver{}
	err := new(d)
	if err != nil {
		return nil, err
	}

	rc, err := d.ReadStream(path, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	p, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	return p, nil
}

// ReadStream retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
type readStreamReader struct {
	driver *radosDriver
	oid    string
	size   uint64
	offset uint64
}

func (r *readStreamReader) Close() error {
	return nil
}

func (d *radosDriver) ReadStream(path string, offset int64) (io.ReadCloser, error) {
	// get oid from filename
	oid, err := d.getOid(path)

	if err != nil {
		return nil, err
	}

	// get object stat
	stat, err := d.Stat(path)

	if err != nil {
		return nil, err
	}

	if offset > stat.Size() {
		return nil, driver.InvalidOffsetError{Path: path, Offset: offset}
	}

	return &readStreamReader{
		driver: d,
		oid:    oid,
		size:   uint64(stat.Size()),
		offset: uint64(offset),
	}, nil
}

func (r *readStreamReader) Read(b []byte) (n int, err error) {
	// Determine the part available to read
	bufferOffset := uint64(0)
	bufferSize := uint64(len(b))

	// End of the object, read less than the buffer size
	if bufferSize > r.size-r.offset {
		bufferSize = r.size - r.offset
	}

	// Fill `b`
	for bufferOffset < bufferSize {
		// Get the offset in the object chunk
		chunkedOid, chunkedOffset := r.driver.getChunkNameFromOffset(r.oid, r.offset)

		// Determine the best size to read
		bufferEndOffset := bufferSize
		if bufferEndOffset-bufferOffset > r.driver.chunksize-chunkedOffset {
			bufferEndOffset = bufferOffset + (r.driver.chunksize - chunkedOffset)
		}

		// Read the chunk
		n, err = r.driver.Ioctx.Read(chunkedOid, b[bufferOffset:bufferEndOffset], chunkedOffset)

		if err != nil {
			return int(bufferOffset), err
		}

		bufferOffset += uint64(n)
		r.offset += uint64(n)
	}

	// EOF if the offset is at the end of the object
	if r.offset == r.size {
		return int(bufferOffset), io.EOF
	}

	return int(bufferOffset), nil
}

func (d *radosDriver) WriteStream(path string, offset int64, reader io.Reader) (totalRead int64, err error) {
	buf := make([]byte, d.chunksize)
	totalRead = 0

	oid, err := d.getOid(path)
	if err != nil {
		return 0, err
	} else {
		// Check total object size only for existing ones
		totalSize, err := d.getXattrTotalSize(oid)
		if err != nil {
			return 0, err
		}

		// If offset if after the current object size, fill the gap with zeros
		for totalSize < uint64(offset) {
			sizeToWrite := d.chunksize
			if totalSize-uint64(offset) < sizeToWrite {
				sizeToWrite = totalSize - uint64(offset)
			}

			chunkName, chunkOffset := d.getChunkNameFromOffset(oid, uint64(totalSize))
			err = d.Ioctx.Write(chunkName, buf[:sizeToWrite], uint64(chunkOffset))
			if err != nil {
				return totalRead, err
			}

			totalSize += sizeToWrite
		}
	}

	// Writer
	for {
		// Align to chunk size
		sizeRead := uint64(0)
		sizeToRead := uint64(offset+totalRead) % d.chunksize
		if sizeToRead == 0 {
			sizeToRead = d.chunksize
		}

		// Read from `reader`
		for sizeRead < sizeToRead {
			nn, err := reader.Read(buf[sizeRead:sizeToRead])
			sizeRead += uint64(nn)

			if err != nil {
				if err != io.EOF {
					return totalRead, err
				}

				break
			}
		}

		// End of file and nothing was read
		if sizeRead == 0 {
			break
		}

		// Write chunk object
		chunkName, chunkOffset := d.getChunkNameFromOffset(oid, uint64(offset+totalRead))
		err = d.Ioctx.Write(chunkName, buf[:sizeRead], uint64(chunkOffset))

		if err != nil {
			return totalRead, err
		}

		// Update total object size as xattr in the first chunk of the object
		err = d.setXattrTotalSize(oid, uint64(offset+totalRead)+sizeRead)
		if err != nil {
			return totalRead, err
		}

		totalRead += int64(sizeRead)

		// End of file
		if sizeRead < sizeToRead {
			break
		}
	}

	return totalRead, nil

}

// Stat retrieves the FileInfo for the given path, including the current size
func (d *radosDriver) Stat(path string) (driver.FileInfo, error) {
	// get oid from filename
	oid, err := d.getOid(path)

	if err != nil {
		return nil, err
	}

	// the path is a virtual directory?
	if oid == "" {
		return driver.FileInfoInternal{
			FileInfoFields: driver.FileInfoFields{
				Path:  path,
				Size:  0,
				IsDir: true,
			},
		}, nil
	}

	// stat first chunk
	stat, err := d.Ioctx.Stat(oid + "-0")

	if err != nil {
		return nil, err
	}

	// get total size of chunked object
	totalSize, err := d.getXattrTotalSize(oid)

	if err != nil {
		return nil, err
	}

	return driver.FileInfoInternal{
		FileInfoFields: driver.FileInfoFields{
			Path:    path,
			Size:    int64(totalSize),
			ModTime: stat.ModTime,
		},
	}, nil
}

func (d *radosDriver) getOid(objectPath string) (string, error) {
	directory := path.Dir(objectPath)
	base := path.Base(objectPath)

	files, err := d.Ioctx.GetOmapValues(directory, "", base, 1)

	if (err != nil) || (files[base] == nil) {
		return "", driver.PathNotFoundError{Path: objectPath}
	}

	return string(files[base]), nil
}

// Takes an offset in an chunked object and return the chunk name and a new
// offset in this chunk object
func (d *radosDriver) getChunkNameFromOffset(oid string, offset uint64) (string, uint64) {
	chunkID := offset / d.chunksize
	chunkedOid := oid + "-" + strconv.FormatInt(int64(chunkID), 10)
	chunkedOffset := offset % d.chunksize
	return chunkedOid, chunkedOffset
}

// Set the total size of a chunked object `oid`
func (d *radosDriver) setXattrTotalSize(oid string, size uint64) error {
	// Convert uint64 `size` to []byte
	xattr := make([]byte, binary.MaxVarintLen64)
	binary.LittleEndian.PutUint64(xattr, size)

	// Save the total size as a xattr in the first chunk
	return d.Ioctx.SetXattr(oid+"-0", defaultXattrTotalSizeName, xattr)
}

func (d *radosDriver) getXattrTotalSize(oid string) (uint64, error) {
	// Fetch xattr as []byte
	xattr := make([]byte, binary.MaxVarintLen64)
	xattrLength, err := d.Ioctx.GetXattr(oid+"-0", defaultXattrTotalSizeName, xattr)

	if err != nil {
		return 0, err
	}

	if xattrLength != len(xattr) {
		fmt.Println("object %s xattr length mismatch: %d != %d", oid, xattrLength, len(xattr))
		return 0, driver.PathNotFoundError{Path: oid}
	}

	// Convert []byte as uint64
	totalSize := binary.LittleEndian.Uint64(xattr)

	return totalSize, nil
}
