package nodes

import (
	"context"
	"os"

	"go.uber.org/zap"
	"inspr.dev/inspr/cmd/insprd/memory/brokers"
	"inspr.dev/inspr/cmd/insprd/memory/tree"
	"inspr.dev/inspr/pkg/auth"
	"inspr.dev/inspr/pkg/meta"
	"inspr.dev/inspr/pkg/meta/utils"
	"k8s.io/client-go/rest"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	v1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	cv1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

// ignores unused code for this file in the staticcheck
//lint:file-ignore U1000 Ignore all unused code

//NodeOperator defines a node operations interface.
type NodeOperator struct {
	clientSet kubernetes.Interface
	memory    tree.Manager
	brokers   brokers.Manager
	auth      auth.Auth
}

// Secrets returns the secret interface of the node operator
func (no *NodeOperator) Secrets() cv1.SecretInterface {
	appsNamespace := getK8SVariables().AppsNamespace
	return no.clientSet.CoreV1().Secrets(appsNamespace)
}

// Services returns the service interface for the node operator
func (no *NodeOperator) Services() cv1.ServiceInterface {
	appsNamespace := getK8SVariables().AppsNamespace
	return no.clientSet.CoreV1().Services(appsNamespace)
}

// Deployments returns the deployment interface for the k8s operator
func (no *NodeOperator) Deployments() v1.DeploymentInterface {
	appsNamespace := getK8SVariables().AppsNamespace
	return no.clientSet.AppsV1().Deployments(appsNamespace)
}

// CreateNode deploys a new node structure, if it's information is valid.
// Otherwise, returns an error
func (no *NodeOperator) CreateNode(ctx context.Context, app *meta.App) (*meta.Node, error) {
	logger.Info("deploying a Node structure in k8s",
		zap.Any("node", app), zap.String("operation", "create"))

	for _, applicable := range no.dappApplications(app, false) {
		if applicable == nil {
			continue
		}
		err := applicable.create(no)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// UpdateNode updates a node that already exists, if the new structure is valid.
// Otherwise, returns an error.
func (no *NodeOperator) UpdateNode(ctx context.Context, app *meta.App) (*meta.Node, error) {
	logger.Info("deploying a Node structure in k8s",
		zap.Any("node", app), zap.String("operation", "update"))

	for _, applicable := range no.dappApplications(app, false) {
		err := applicable.update(no)
		if err != nil {
			return nil, err
		}
	}

	return nil, nil
}

// DeleteNode deletes node with given name, if it exists. Otherwise, returns an error
func (no *NodeOperator) DeleteNode(ctx context.Context, nodeContext string, nodeName string) error {
	logger.Info("deleting a Node structure in k8s",
		zap.String("node", nodeName),
		zap.String("context", nodeContext))

	logger.Debug("getting name of the k8s deployment to be deleted")
	scope, _ := utils.JoinScopes(nodeContext, nodeName)
	app, err := no.memory.Perm().Apps().Get(scope)
	if err != nil {
		logger.Info("Error while getting app inside DeleteNode",
			zap.String("scope", scope),
		)
	}

	logger.Debug("deleting a Node structure in k8s",
		zap.Any("node", app))

	for _, applicable := range no.dappApplications(app, true) {
		err := applicable.del(no)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewNodeOperator initializes a k8s based node operator with in cluster configuration
func NewNodeOperator(memory tree.Manager, authenticator auth.Auth, broker brokers.Manager) (nop *NodeOperator, err error) {
	nop = &NodeOperator{
		memory:  memory,
		auth:    authenticator,
		brokers: broker,
	}
	if _, exists := os.LookupEnv("DEBUG"); exists {
		logger.Info("initializing node operator with debug configs")
		nop.clientSet = fake.NewSimpleClientset()
	} else {
		logger.Info("initializing node operator with production configs")
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, err
		}

		nop.clientSet, err = kubernetes.NewForConfig(config)
		if err != nil {
			return nil, err
		}
	}
	return nop, nil
}
