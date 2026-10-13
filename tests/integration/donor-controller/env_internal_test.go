package donorcontroller

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Default environment settings.
const (
	defaultDonorNamespace      = "timeslice-system"
	defaultDeploymentSelector  = "app.kubernetes.io/name=donor-controller"
	defaultDonorIdleTTL        = 20 * time.Second
	defaultDonorIsolationTaint = "timeslice.io/shared=true:NoSchedule"
	defaultResyncPeriod        = 30 * time.Second
	defaultStaleFenceGrace     = 20 * time.Second
)

// EnvConfig holds configuration loaded from environment variables or defaults.
type EnvConfig struct {
	Namespace          string
	DeploymentSelector string
	DeploymentName     string
	IdleTTL            time.Duration
	IsolationTaintRaw  string
	IsolationTaint     *corev1.Taint
	StaleFenceGrace    time.Duration
	RunID              string
}

// LoadEnv reads configuration from the environment, populating defaults where unset.
func LoadEnv() (*EnvConfig, error) {
	ns := os.Getenv("DONOR_NAMESPACE")
	if ns == "" {
		ns = defaultDonorNamespace
	}

	idleTTL := defaultDonorIdleTTL
	if rawTTL := os.Getenv("DONOR_IDLE_TTL"); rawTTL != "" {
		d, err := time.ParseDuration(rawTTL)
		if err != nil {
			return nil, fmt.Errorf("parsing DONOR_IDLE_TTL=%q: %w", rawTTL, err)
		}
		idleTTL = d
	}

	staleFenceGrace := defaultStaleFenceGrace
	if raw := os.Getenv("DONOR_STALE_FENCE_GRACE"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return nil, fmt.Errorf("parsing DONOR_STALE_FENCE_GRACE=%q: %w", raw, err)
		}
		staleFenceGrace = d
	}

	taintRaw := os.Getenv("DONOR_ISOLATION_TAINT")
	if taintRaw == "" {
		taintRaw = defaultDonorIsolationTaint
	}

	var parsedTaint *corev1.Taint
	if taintRaw != "" {
		t, err := ParseIsolationTaint(taintRaw)
		if err != nil {
			return nil, fmt.Errorf("parsing DONOR_ISOLATION_TAINT=%q: %w", taintRaw, err)
		}
		parsedTaint = t
	}

	runID := os.Getenv("DONOR_RUN_ID")
	if runID == "" {
		b := make([]byte, 3)
		if _, err := rand.Read(b); err != nil {
			return nil, fmt.Errorf("generating run id: %w", err)
		}
		runID = hex.EncodeToString(b)
	}

	return &EnvConfig{
		Namespace:          ns,
		DeploymentSelector: defaultDeploymentSelector,
		DeploymentName:     os.Getenv("DONOR_DEPLOYMENT"),
		IdleTTL:            idleTTL,
		IsolationTaintRaw:  taintRaw,
		IsolationTaint:     parsedTaint,
		StaleFenceGrace:    staleFenceGrace,
		RunID:              runID,
	}, nil
}

// ParseIsolationTaint parses strings like "timeslice.io/shared=true:NoSchedule" or "key:NoSchedule".
func ParseIsolationTaint(raw string) (*corev1.Taint, error) {
	if raw == "" {
		return nil, nil //nolint:nilnil // returning nil, nil is intentional for an empty string
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("expected format key[=value]:Effect, got %q", raw)
	}

	var effect corev1.TaintEffect
	switch parts[1] {
	case "NoSchedule":
		effect = corev1.TaintEffectNoSchedule
	case "PreferNoSchedule":
		effect = corev1.TaintEffectPreferNoSchedule
	case "NoExecute":
		effect = corev1.TaintEffectNoExecute
	default:
		return nil, fmt.Errorf("unknown taint effect %q", parts[1])
	}

	kv := strings.SplitN(parts[0], "=", 2)
	key := kv[0]
	value := ""
	if len(kv) == 2 {
		value = kv[1]
	}

	return &corev1.Taint{
		Key:    key,
		Value:  value,
		Effect: effect,
	}, nil
}

// NewKubeClient builds a kubernetes client using default clientcmd loading rules
// (honoring KUBECONFIG or ~/.kube/config) with automatic fallback to in-cluster config.
func NewKubeClient() (kubernetes.Interface, *rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	kubeconfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
	cfg, err := kubeconfig.ClientConfig()
	if err != nil {
		cfg, err = rest.InClusterConfig()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to build kubeconfig (neither out-of-cluster nor in-cluster available): %w", err)
		}
	}

	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("creating clientset: %w", err)
	}
	return client, cfg, nil
}

// ControllerHandle provides operations for managing the donor-controller under test.
type ControllerHandle struct {
	client         kubernetes.Interface
	namespace      string
	deploymentName string
	selector       string
}

// NewControllerHandle discovers the donor-controller Deployment and constructs a handle.
func NewControllerHandle(ctx context.Context, client kubernetes.Interface, cfg *EnvConfig) (*ControllerHandle, error) {
	depName := cfg.DeploymentName
	if depName == "" {
		deps, err := client.AppsV1().Deployments(cfg.Namespace).List(ctx, metav1.ListOptions{
			LabelSelector: cfg.DeploymentSelector,
		})
		if err != nil {
			return nil, fmt.Errorf(
				"listing deployments with selector %q in namespace %q: %w",
				cfg.DeploymentSelector,
				cfg.Namespace,
				err,
			)
		}
		if len(deps.Items) == 0 {
			return nil, fmt.Errorf(
				"no donor-controller deployment found in namespace %q with selector %q",
				cfg.Namespace,
				cfg.DeploymentSelector,
			)
		}
		depName = deps.Items[0].Name
	}

	return &ControllerHandle{
		client:         client,
		namespace:      cfg.Namespace,
		deploymentName: depName,
		selector:       cfg.DeploymentSelector,
	}, nil
}

// Namespace returns the namespace of the donor-controller deployment.
func (c *ControllerHandle) Namespace() string {
	return c.namespace
}

// Restart deletes the donor-controller pod(s) and waits until a new pod is Ready.
func (c *ControllerHandle) Restart(ctx context.Context) error {
	pods, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: c.selector,
	})
	if err != nil {
		return fmt.Errorf("listing controller pods: %w", err)
	}

	oldUIDs := make(map[string]bool)
	zero := int64(0)
	for i := range pods.Items {
		p := pods.Items[i]
		oldUIDs[string(p.UID)] = true
		if err := c.client.CoreV1().Pods(c.namespace).Delete(ctx, p.Name, metav1.DeleteOptions{
			GracePeriodSeconds: &zero,
		}); err != nil {
			_ = err
		}
	}

	// Wait for a ready controller pod with a new UID.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		currentPods, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
			LabelSelector: c.selector,
		})
		if err == nil {
			for i := range currentPods.Items {
				p := currentPods.Items[i]
				if oldUIDs[string(p.UID)] || p.DeletionTimestamp != nil {
					continue
				}
				for _, cond := range p.Status.Conditions {
					if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
						return nil
					}
				}
			}
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for new controller pod to become Ready after restart")
}

// SetDryRun updates the controller Deployment args to add/remove "--dry-run" and waits for rollout.
func (c *ControllerHandle) SetDryRun(ctx context.Context, dryRun bool) error {
	dep, err := c.client.AppsV1().Deployments(c.namespace).Get(ctx, c.deploymentName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("getting deployment %s: %w", c.deploymentName, err)
	}

	containerIdx := -1
	for i := range dep.Spec.Template.Spec.Containers {
		container := &dep.Spec.Template.Spec.Containers[i]
		if container.Name == "donor-controller" {
			containerIdx = i
			break
		}
	}
	if containerIdx == -1 {
		if len(dep.Spec.Template.Spec.Containers) == 1 {
			containerIdx = 0
		} else {
			return fmt.Errorf("cannot find container donor-controller in deployment %s", c.deploymentName)
		}
	}

	args := dep.Spec.Template.Spec.Containers[containerIdx].Args
	hasDryRun := false
	var filtered []string
	for _, a := range args {
		if a == "--dry-run" {
			hasDryRun = true
		} else {
			filtered = append(filtered, a)
		}
	}

	if dryRun == hasDryRun {
		return nil // already in desired state
	}

	if dryRun {
		filtered = append(filtered, "--dry-run")
	}
	dep.Spec.Template.Spec.Containers[containerIdx].Args = filtered

	if !dryRun {
		// We are toggling back to normal. Clean up our ManagedFields to prevent Helm SSA conflicts.
		var cleanMF []metav1.ManagedFieldsEntry
		for _, mf := range dep.ManagedFields {
			if mf.Manager == "Go-http-client" || mf.Manager == "donor-controller.test" || mf.Manager == "test-runner" {
				continue
			}
			cleanMF = append(cleanMF, mf)
		}
		dep.ManagedFields = cleanMF
	}

	_, err = c.client.AppsV1().Deployments(c.namespace).Update(ctx, dep, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("updating deployment %s dry-run to %v: %w", c.deploymentName, dryRun, err)
	}

	// Wait for rollout
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		currentDep, err := c.client.AppsV1().Deployments(c.namespace).Get(ctx, c.deploymentName, metav1.GetOptions{})
		if c.isRolloutComplete(ctx, currentDep, dryRun, err) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("timeout waiting for rollout of deployment %s (dry-run=%v)", c.deploymentName, dryRun)
}

// Logs returns the logs of the donor-controller pod(s).
func (c *ControllerHandle) Logs(ctx context.Context, previous bool) (string, error) {
	pods, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: c.selector,
	})
	if err != nil {
		return "", fmt.Errorf("listing controller pods for logs: %w", err)
	}

	var allLogs bytes.Buffer
	for i := range pods.Items {
		p := pods.Items[i]
		req := c.client.CoreV1().Pods(c.namespace).GetLogs(p.Name, &corev1.PodLogOptions{
			Container: "donor-controller",
			Previous:  previous,
		})
		stream, err := req.Stream(ctx)
		if err != nil {
			if previous && apierrors.IsBadRequest(err) {
				// No previous container, safe to ignore
				continue
			}
			return "", fmt.Errorf("streaming logs for pod %s: %w", p.Name, err)
		}
		_, err = io.Copy(&allLogs, stream)
		_ = stream.Close()
		if err != nil {
			return "", fmt.Errorf("reading logs for pod %s: %w", p.Name, err)
		}
		allLogs.WriteString("\n")
	}

	return allLogs.String(), nil
}

func (c *ControllerHandle) isRolloutComplete(ctx context.Context, currentDep *appsv1.Deployment, dryRun bool, getErr error) bool {
	if getErr != nil || currentDep.Status.ObservedGeneration < currentDep.Generation {
		return false
	}
	if currentDep.Status.UpdatedReplicas != 1 || currentDep.Status.ReadyReplicas != 1 || currentDep.Status.AvailableReplicas != 1 {
		return false
	}
	pods, err := c.client.CoreV1().Pods(c.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: c.selector,
	})
	if err != nil || len(pods.Items) == 0 {
		return false
	}
	for i := range pods.Items {
		p := pods.Items[i]
		if p.DeletionTimestamp != nil {
			continue
		}
		for _, cond := range p.Status.Conditions {
			if cond.Type == corev1.PodReady && cond.Status == corev1.ConditionTrue {
				podHasDry := false
				for i := range p.Spec.Containers {
					container := &p.Spec.Containers[i]
					for _, arg := range container.Args {
						if arg == "--dry-run" {
							podHasDry = true
						}
					}
				}
				if podHasDry == dryRun {
					return true
				}
			}
		}
	}
	return false
}
