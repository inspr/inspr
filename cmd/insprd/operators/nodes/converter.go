package nodes

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"go.uber.org/zap"
	"inspr.dev/inspr/pkg/auth"
	"inspr.dev/inspr/pkg/environment"
	"inspr.dev/inspr/pkg/meta"
	metautils "inspr.dev/inspr/pkg/meta/utils"
	"inspr.dev/inspr/pkg/operator/k8s"
	"inspr.dev/inspr/pkg/utils"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
)

var lbsidecarPort int32

func (no *NodeOperator) dappToService(app *meta.App) *kubeService {
	logger.Info("creating kubernetes service")

	temp, _ := strconv.Atoi(os.Getenv("INSPR_LBSIDECAR_READ_PORT"))
	if temp > 0 && temp < math.MaxUint16 {
		lbsidecarPort = int32(temp)
	} else {
		// ref http://www.faqs.org/rfcs/rfc793.html
		panic("invalid port number. the highest valid TCP / UDP port number is 65535 and the lowest is 1")
	}

	appID := toAppID(app)
	appDeployName := toDeploymentName(app)
	appLabels := map[string]string{"inspr-app": appID}

	svc := &kubeService{
		ObjectMeta: metav1.ObjectMeta{
			Name: appDeployName,
		},
		Spec: corev1.ServiceSpec{
			Ports: func() (ports []corev1.ServicePort) {
				for i, port := range app.Spec.Node.Spec.Ports {
					ports = append(ports, corev1.ServicePort{
						Name:       fmt.Sprintf("port%v", i),
						Port:       int32(port.Port),
						TargetPort: intstr.FromInt(port.TargetPort),
					})
				}
				ports = append(ports, corev1.ServicePort{
					Name:       "lbsidecar-port",
					Port:       lbsidecarPort,
					TargetPort: intstr.FromInt(int(lbsidecarPort)),
				})
				return
			}(),
			Selector: appLabels,
		},
	}

	return svc
}

// dAppToDeployment translates the DApp to a k8s deployment
func (no *NodeOperator) dAppToDeployment(app *meta.App, usePermTree bool) *kubeDeployment {
	appDeployName := toDeploymentName(app)
	appID := toAppID(app)
	var depNames utils.StringArray
	depNames = strings.Split(app.Meta.Parent, ".")
	if depNames[0] == "" {
		depNames = utils.StringArray{}
	}
	appLabels := map[string]string{
		"inspr-app": appID,
	}
	logger.Info("constructing deployment", zap.Bool("useperm", usePermTree))

	nodeContainer := createNodeContainer(app, appDeployName)
	scContainers := no.withAllSidecarsContainers(app, appDeployName, usePermTree)

	appAnnotations := map[string]string{
		"inspr.com/app-id":             appID,
		"inspr.com/app-name":           app.Meta.Name,
		"inspr.com/app-reference":      app.Meta.Reference,
		"inspr.com/app-scope":          strings.Join(depNames, "-"),
		"app.kubernetes.io/name":       app.Meta.Name,
		"app.kubernetes.io/instance":   app.Meta.UUID,
		"app.kubernetes.io/managed-by": "inspr",
		"app.kubernetes.io/created-by": "inspr",
		"prometheus.io/scrape":         "true",
	}

	return (*kubeDeployment)(
		k8s.NewDeployment(
			appDeployName,
			k8s.WithLabels(appLabels),
			k8s.WithAnnotations(appAnnotations),
			k8s.WithContainer(
				append(scContainers, nodeContainer)...,
			),
			k8s.WithReplicas(app.Spec.Node.Spec.Replicas),
		))
}

func (no *NodeOperator) withAllSidecarsContainers(app *meta.App, appDeployName string, usePermTree bool) []corev1.Container {
	var containers []corev1.Container
	var sidecarAddrs []corev1.EnvVar

	factory, err := no.brokers.Factory().Get("kafka")
	if err != nil {
		panic(fmt.Sprintf("broker %v not allowed: %v", "kafka", err))
	}

	lbSidecar, _ := factory(app, nil,
		no.withLBSidecarName(),
		no.withLBSidecarImage(),
		no.withBoundary(app, usePermTree),
		no.withRoutes(app),
		overwritePortEnvs(app),
		withLBPort(),
		withLBSidecarConfiguration(),
		k8s.ContainerWithEnv(sidecarAddrs...),
		withNodeID(app),
		k8s.ContainerWithEnv(corev1.EnvVar{
			Name:  "LOG_LEVEL",
			Value: app.Spec.LogLevel,
		}),
		k8s.ContainerWithPullPolicy(corev1.PullAlways),
	)

	containers = append(containers, lbSidecar)

	return containers
}

func (no *NodeOperator) withRoutes(app *meta.App) k8s.ContainerOption {
	return func(c *corev1.Container) {
		env := make(utils.EnvironmentMap)
		for route, data := range app.Spec.Routes {
			raw := data.Address + ";"
			raw = raw + data.Endpoints.Join(";")
			env[route+"_ROUTE"] = raw
		}
		c.Env = append(c.Env, env.ParseToK8sArrEnv()...)
	}
}

func (no *NodeOperator) getAllSidecarBrokers(app *meta.App, usePermTree bool) utils.StringArray {
	input := app.Spec.Boundary.Channels.Input
	output := app.Spec.Boundary.Channels.Output
	channels := input.Union(output)

	logger.Debug("resolving Node Boundary in the cluster",
		zap.String("operation", "getAllSidecarBrokers"),
		zap.Bool("useperm", usePermTree),
		zap.String("app:", app.Meta.Name),
	)

	_, resolvedChannel, err := no.memory.Apps().ResolveBoundary(app, usePermTree)
	if err != nil {
		logger.Error("unable to resolve Node boundaries",
			zap.Any("boundaries", app.Spec.Boundary))
		panic(err)
	}

	set, _ := metautils.MakeStrSet(channels.Map(func(boundary string) string {
		resolved := resolvedChannel[boundary]
		parent, chName, _ := metautils.RemoveLastPartInScope(resolved)
		var ch *meta.Channel
		if usePermTree {
			ch, err = no.memory.Perm().Channels().Get(parent, chName)
		} else {
			ch, err = no.memory.Channels().Get(parent, chName)
		}
		if err != nil {
			logger.Error("unable get channel for boudary resolution",
				zap.String("channel", chName))
			panic(err)
		}
		return ch.Spec.SelectedBroker
	}))
	return set.ToArray()
}

// withBoundary adds the boundary configuration to the kubernetes' deployment environment variables
func (no *NodeOperator) withBoundary(app *meta.App, usePermTree bool) k8s.ContainerOption {
	scope, _ := metautils.JoinScopes(app.Meta.Parent, app.Meta.Name)
	if _, err := no.memory.Apps().Get(scope); err != nil {
		return nil
	}
	return func(c *corev1.Container) {
		input := app.Spec.Boundary.Channels.Input
		output := app.Spec.Boundary.Channels.Output
		channels := input.Union(output)

		logger.Debug("with boundary", zap.Bool("useperm", usePermTree))

		_, resolvedChannel, err := no.memory.Apps().ResolveBoundary(app, usePermTree)
		if err != nil {
			logger.Error("unable to resolve Node boundaries",
				zap.Any("boundaries", app.Spec.Boundary))
			panic(err)
		}

		inputEnv := input.Map(func(boundary string) string {
			return no.returnChannelBroker(boundary, resolvedChannel[boundary])
		})

		outputEnv := output.Map(func(boundary string) string {
			return no.returnChannelBroker(boundary, resolvedChannel[boundary])
		})

		env := utils.EnvironmentMap{
			"INSPR_INPUT_CHANNELS":  inputEnv.Join(";"),
			"INSPR_OUTPUT_CHANNELS": outputEnv.Join(";"),
		}

		logger.Debug("resolving with Node Boundary", zap.Bool("useperm", usePermTree))
		channels.Map(func(boundary string) string {
			resolved := resolvedChannel[boundary]
			parent, chName, _ := metautils.RemoveLastPartInScope(resolved)
			var ch *meta.Channel
			var ct *meta.Type
			var cterr, cherr error
			if usePermTree {
				ch, cherr = no.memory.Perm().Channels().Get(parent, chName)
				ct, cterr = no.memory.Perm().Types().Get(parent, ch.Spec.Type)
			} else {
				ch, cherr = no.memory.Channels().Get(parent, chName)
				ct, cterr = no.memory.Types().Get(parent, ch.Spec.Type)
			}
			if cherr != nil {
				logger.Error("Unable to get channel to resolve with boundary", zap.String("channel", chName))
				panic(err)
			}
			if cterr != nil {
				logger.Error("Unable to get channel type to resolve with boundary", zap.String("type", ch.Spec.Type))
				panic(err)
			}
			resolved = "INSPR_" + ch.Meta.UUID
			env[resolved+"_SCHEMA"] = ct.Schema
			env[boundary+"_RESOLVED"] = resolved
			return boundary
		})
		logger.Debug("resolved with Node Boundary", zap.Bool("useperm", usePermTree))

		c.Env = append(c.Env, env.ParseToK8sArrEnv()...)
	}
}

// withLBSidecarImage adds the sidecar image to the dApp
func (no *NodeOperator) withLBSidecarImage() k8s.ContainerOption {
	return func(c *corev1.Container) {
		c.Image = environment.GetSidecarImage()
	}
}

// withLBSidecarName adds the sidecar name to the container
func (no *NodeOperator) withLBSidecarName() k8s.ContainerOption {
	return func(c *corev1.Container) {
		c.Name = "lbsidecar"
	}
}

func (no *NodeOperator) returnChannelBroker(channel, pathToResolvedChannel string) string {
	scope, chName, err := metautils.RemoveLastPartInScope(pathToResolvedChannel)
	if err != nil {
		return ""
	}
	resolvedCh, err := no.memory.Channels().Get(scope, chName)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%s@%s", channel, resolvedCh.Spec.SelectedBroker)
}

func (no *NodeOperator) toSecret(app *meta.App) *kubeSecret {
	logger.Info("creating secret")
	scope, err := metautils.JoinScopes(app.Meta.Parent, app.Meta.Name)
	if err != nil {
		logger.Error("invalid scope", zap.Any("error", err))
		return nil
	}

	payload := auth.Payload{
		UID:        app.Meta.UUID,
		Refresh:    []byte(scope),
		RefreshURL: fmt.Sprintf("http://%v/refreshController", os.Getenv("INSPR_INSPRD_ADDRESS")),
	}
	payload.ImportPermissionList(app.Spec.Auth.Permissions, app.Spec.Auth.Scope)

	token, err := no.auth.Tokenize(payload)
	if err != nil {
		logger.Error("unable to tokenize", zap.Any("error", err))
		return nil
	}

	return &kubeSecret{
		ObjectMeta: metav1.ObjectMeta{
			Name: toDeploymentName(app),
		},
		Data: map[string][]byte{
			"INSPR_CONTROLLER_TOKEN": token,
			"INSPR_CONTROLLER_SCOPE": []byte(app.Spec.Auth.Scope),
		},
	}
}

// Auxiliar methods

func withSecretDefinition(app *meta.App) k8s.ContainerOption {
	env := corev1.EnvFromSource{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{
				Name: toDeploymentName(app),
			},
		},
	}
	return k8s.ContainerWithEnvFrom(env)
}

func createNodeContainer(app *meta.App, appDeployName string) corev1.Container {
	return k8s.NewContainer(
		appDeployName,
		app.Spec.Node.Spec.Image,
		overwritePortEnvs(app),
		withNodePort(),
		withSecretDefinition(app),
		k8s.ContainerWithEnv(corev1.EnvVar{
			Name:  "LOG_LEVEL",
			Value: app.Spec.LogLevel,
		}),
		withLBSidecarConfiguration(),
	)
}

func withNodePort() k8s.ContainerOption {
	return k8s.ContainerWithPorts(corev1.ContainerPort{
		ContainerPort: 16002, Name: "tcp-nd-metrics", Protocol: corev1.ProtocolTCP,
	})
}

func withLBPort() k8s.ContainerOption {
	return k8s.ContainerWithPorts(corev1.ContainerPort{
		ContainerPort: 16000, Name: "tcp-lbs-metrics", Protocol: corev1.ProtocolTCP,
	})
}

func withLBSidecarConfiguration() k8s.ContainerOption {
	return k8s.ContainerWithEnvFrom(
		corev1.EnvFromSource{
			ConfigMapRef: &corev1.ConfigMapEnvSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: os.Getenv("INSPR_LBSIDECAR_CONFIGMAP"),
				},
			},
		},
	)
}

// overwritePortEnvs adds the load balancer sidecar ports if they are defined in the dApp definitions.
// On kubernetes, this overrides the defined configuration on the configmap
func overwritePortEnvs(app *meta.App) k8s.ContainerOption {
	return func(c *corev1.Container) {
		lbWritePort := app.Spec.Node.Spec.SidecarPort.LBWrite
		lbReadPort := app.Spec.Node.Spec.SidecarPort.LBRead

		if lbWritePort > 0 {
			c.Env = append(c.Env, corev1.EnvVar{
				Name:  "INSPR_LBSIDECAR_WRITE_PORT",
				Value: strconv.Itoa(lbWritePort),
			})
		} else {
			app.Spec.Node.Spec.SidecarPort.LBWrite, _ = strconv.Atoi(os.Getenv("INSPR_LBSIDECAR_WRITE_PORT"))
		}

		if lbReadPort > 0 {
			c.Env = append(c.Env, corev1.EnvVar{
				Name:  "INSPR_LBSIDECAR_READ_PORT",
				Value: strconv.Itoa(lbReadPort),
			})
		} else {
			app.Spec.Node.Spec.SidecarPort.LBRead, _ = strconv.Atoi(os.Getenv("INSPR_LBSIDECAR_READ_PORT"))
		}

	}
}

func withNodeID(app *meta.App) k8s.ContainerOption {
	return k8s.ContainerWithEnv(corev1.EnvVar{
		Name:  "INSPR_APP_ID",
		Value: toAppID(app),
	})
}

// toDeployment - creates the kubernetes deployment name from the app
func toDeploymentName(app *meta.App) string {
	return "node-" + app.Meta.UUID
}

// toAppID - creates the kubernetes deployment name from the app
func toAppID(app *meta.App) string {
	var depNames utils.StringArray
	depNames = strings.Split(app.Meta.Parent, ".")
	if depNames[0] == "" {
		depNames = utils.StringArray{}
	}
	depNames = append(depNames, app.Meta.Name)
	return depNames.Join("-")
}

// intToint32 - converts an integer to a *int32
func intToint32(v int) *int32 {
	t := int32(v)
	return &t
}
