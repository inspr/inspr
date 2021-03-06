package main

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"testing"

	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
	"inspr.dev/inspr/pkg/logs"
)

func Test_KeyGen(t *testing.T) {
	logger, _ = logs.Logger(zap.Fields(zap.String("section", "auth-provider")))

	privateKey, err := generatePrivateKey()
	if err != nil {
		logger.Fatal(err.Error())
	}

	privateKeyBytes, publicKeyBytes, err := encodeKeysToPEM(privateKey)
	if err != nil {
		logger.Fatal(err.Error())
	}

	if ok := verifyKeyPair(privateKeyBytes, publicKeyBytes); !ok {
		t.Errorf("alalala")
	}

}

func verifyKeyPair(private, public []byte) bool {
	block, _ := pem.Decode(private)
	key, _ := x509.ParsePKCS1PrivateKey(block.Bytes)
	pubBlock, _ := pem.Decode(public)
	parsed, _, _, _, err := ssh.ParseAuthorizedKey(pubBlock.Bytes)
	parsedCryptoKey := parsed.(ssh.CryptoPublicKey)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}

	// Then, we can call CryptoPublicKey() to get the actual crypto.PublicKey
	pubCrypto := parsedCryptoKey.CryptoPublicKey()

	// Finally, we can convert back to an *rsa.PublicKey
	pub := pubCrypto.(*rsa.PublicKey)
	return key.PublicKey.Equal(pub)
}
