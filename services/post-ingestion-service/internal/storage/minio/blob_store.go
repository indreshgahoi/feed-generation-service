// Package minio wraps presigned-upload URL generation. This is the one
// piece of storage code with no domain interface: it's a pure
// infrastructure utility (direct-to-blob upload, bypassing the app
// server per the design doc's write path) with no business rule attached
// to it, so there's nothing for a domain interface to abstract.
package minio

import (
	"context"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type BlobStore struct {
	client *minio.Client
	bucket string
}

func NewBlobStore(endpoint, accessKey, secretKey, bucket string, useSSL bool) (*BlobStore, error) {
	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, err
	}
	return &BlobStore{client: client, bucket: bucket}, nil
}

func (b *BlobStore) EnsureBucket(ctx context.Context) error {
	exists, err := b.client.BucketExists(ctx, b.bucket)
	if err != nil {
		return err
	}
	if !exists {
		return b.client.MakeBucket(ctx, b.bucket, minio.MakeBucketOptions{})
	}
	return nil
}

func (b *BlobStore) PresignedUploadURL(ctx context.Context, objectName string) (string, error) {
	url, err := b.client.PresignedPutObject(ctx, b.bucket, objectName, 15*time.Minute)
	if err != nil {
		return "", err
	}
	return url.String(), nil
}
