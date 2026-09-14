package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"

	"feed-aggregation-service/internal/domain"
)

// AES-256-GCM pagination cursor -- the design doc rejects OFFSET/LIMIT
// specifically to avoid pagination drift on scroll; see doc/flow.md.
type CursorCodec struct {
	key []byte
}

func NewCursorCodec(secret string) *CursorCodec {
	sum := sha256.Sum256([]byte(secret))
	return &CursorCodec{key: sum[:]}
}

func (c *CursorCodec) Encode(cursor domain.FeedCursor) (string, error) {
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	plaintext, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return base64.RawURLEncoding.EncodeToString(ciphertext), nil
}

func (c *CursorCodec) Decode(token string) (domain.FeedCursor, error) {
	var cursor domain.FeedCursor
	if token == "" {
		return cursor, nil
	}
	block, err := aes.NewCipher(c.key)
	if err != nil {
		return cursor, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return cursor, err
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return cursor, err
	}
	if len(raw) < gcm.NonceSize() {
		return cursor, errors.New("cursor too short")
	}
	nonce, ciphertext := raw[:gcm.NonceSize()], raw[gcm.NonceSize():]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return cursor, err
	}
	if err := json.Unmarshal(plaintext, &cursor); err != nil {
		return cursor, err
	}
	return cursor, nil
}
