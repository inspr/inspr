package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"os"

	"math/big"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/crypto/ssh"
	"inspr.dev/inspr/pkg/logs"
	corev1 "k8s.io/api/core/v1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var clientSet kubernetes.Interface
var logger *zap.Logger

func generatePassword() string {
	chars := []rune("ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"abcdefghijklmnopqrstuvwxyz" +
		"0123456789")
	length := 20
	var b strings.Builder
	for i := 0; i < length; i++ {
		index, _ := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		b.WriteRune(chars[index.Int64()])
	}
	str := b.String()
	return str
}

// initKube initializes a k8s operator with in cluster configuration
func initKube() error {

	config, err := rest.InClusterConfig()
	if err != nil {
		return err
	}

	clientSet, err = kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}

	return nil
}

func generatePrivateKey() (*rsa.PrivateKey, error) {
	// Private Key generation
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}

	// Validate Private Key
	err = privateKey.Validate()
	if err != nil {
		return nil, err
	}

	logger.Info("Private Key generated")
	return privateKey, nil
}

func encodeKeysToPEM(privateKey *rsa.PrivateKey) (pubKey []byte, privKey []byte, err error) {
	// Get ASN.1 DER format
	privDER := x509.MarshalPKCS1PrivateKey(privateKey)
	publicKeyBytes, err := generatePublicKey(&privateKey.PublicKey)
	if err != nil {
		logger.Fatal(err.Error())
		return nil, nil, err
	}
	// pem.Block
	privBlock := pem.Block{
		Type:    "RSA PRIVATE KEY",
		Headers: nil,
		Bytes:   privDER,
	}

	pubBlock := pem.Block{
		Type:    "RSA PUBLIC KEY",
		Headers: nil,
		Bytes:   publicKeyBytes,
	}

	// Private key in PEM format
	privatePEM := pem.EncodeToMemory(&privBlock)
	publicPEM := pem.EncodeToMemory(&pubBlock)
	return privatePEM, publicPEM, nil
}

// generatePublicKey take a rsa.PublicKey and return bytes suitable for writing to .pub file
// returns in the format "ssh-rsa ..."
func generatePublicKey(publicKey *rsa.PublicKey) ([]byte, error) {
	publicRsaKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		return nil, err
	}

	pubKeyBytes := ssh.MarshalAuthorizedKey(publicRsaKey)

	logger.Info("Public key generated")
	return pubKeyBytes, nil
}

func main() {
	logger, _ = logs.Logger()

	namespace := os.Getenv("K8S_NAMESPACE")

	initKube()

	_, errPriv := clientSet.CoreV1().Secrets(namespace).Get(context.Background(), "jwtprivatekey", v1.GetOptions{})
	_, errPub := clientSet.CoreV1().Secrets(namespace).Get(context.Background(), "jwtpublickey", v1.GetOptions{})

	if errPriv != nil || errPub != nil {
		if errPriv == nil {
			clientSet.CoreV1().Secrets(namespace).Delete(context.Background(), "jwtprivatekey", v1.DeleteOptions{})
		}

		if errPub == nil {
			clientSet.CoreV1().Secrets(namespace).Delete(context.Background(), "jwtpublickey", v1.DeleteOptions{})
		}

		privateKey, err := generatePrivateKey()
		if err != nil {
			logger.Fatal(err.Error())
		}

		privateKeyBytes, publicKeyBytes, err := encodeKeysToPEM(privateKey)
		if err != nil {
			logger.Fatal(err.Error())
		}

		privSec := corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			ObjectMeta: v1.ObjectMeta{
				Name: "jwtprivatekey",
			},
			Data: map[string][]byte{
				"key": privateKeyBytes,
			},
		}
		pubSec := corev1.Secret{
			Type: corev1.SecretTypeOpaque,
			ObjectMeta: v1.ObjectMeta{
				Name: "jwtpublickey",
			},
			Data: map[string][]byte{
				"key": publicKeyBytes,
			},
		}
		_, err = clientSet.CoreV1().Secrets(namespace).Create(context.Background(), &privSec, v1.CreateOptions{})
		if err != nil {
			logger.Fatal(err.Error())
		}
		_, err = clientSet.CoreV1().Secrets(namespace).Create(context.Background(), &pubSec, v1.CreateOptions{})
		if err != nil {
			logger.Fatal(err.Error())
		}
		logger.Info("New secrets generated.")
	}

	if secretName := os.Getenv("INSPRD_INIT_KEY_SECRET_NAME"); secretName != "" {
		logger.Info("generating init key")
		secret, err := clientSet.CoreV1().Secrets(namespace).Get(context.Background(), secretName, v1.GetOptions{})
		if err != nil {
			panic(err)
		}
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		secret.Data["key"] = []byte(generatePassword())
		_, err = clientSet.CoreV1().Secrets(namespace).Update(context.Background(), secret, v1.UpdateOptions{})
		if err != nil {
			panic(err)
		}
	}
}
