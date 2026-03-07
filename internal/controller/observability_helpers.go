package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8smeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	monitoringReadyConditionType = "MonitoringReady"
	observabilityRequeueInterval = time.Minute
)

var serviceMonitorGVK = schema.GroupVersionKind{Group: "monitoring.coreos.com", Version: "v1", Kind: "ServiceMonitor"}

type workloadObservabilityConfig struct {
	Owner                     ctrlclient.Object
	Scheme                    *runtime.Scheme
	Labels                    map[string]string
	Selector                  map[string]string
	MetricsEnabled            bool
	MetricsServiceName        string
	MetricsServicePort        int32
	MetricsServiceAnnotations map[string]string
	MetricsPath               string
	ServiceMonitorEnabled     bool
	ServiceMonitorInterval    time.Duration
	ServiceMonitorLabels      map[string]string
}

type workloadObservabilityStatus struct {
	MetricsServiceName string
	ConditionStatus    metav1.ConditionStatus
	Reason             string
	Message            string
}

func reconcileWorkloadObservability(ctx context.Context, c ctrlclient.Client, cfg workloadObservabilityConfig) (workloadObservabilityStatus, error) {
	if !cfg.MetricsEnabled {
		if err := deleteWorkloadMetricsService(ctx, c, cfg.Owner.GetNamespace(), cfg.MetricsServiceName); err != nil {
			return workloadObservabilityStatus{}, err
		}
		if err := deleteWorkloadServiceMonitor(ctx, c, cfg.Owner.GetNamespace(), cfg.MetricsServiceName); err != nil {
			return workloadObservabilityStatus{}, err
		}
		return workloadObservabilityStatus{
			ConditionStatus: metav1.ConditionTrue,
			Reason:          "ObservabilityNotConfigured",
			Message:         "Workload metrics are not configured.",
		}, nil
	}

	if err := reconcileWorkloadMetricsService(ctx, c, cfg); err != nil {
		return workloadObservabilityStatus{}, err
	}

	if !cfg.ServiceMonitorEnabled {
		if err := deleteWorkloadServiceMonitor(ctx, c, cfg.Owner.GetNamespace(), cfg.MetricsServiceName); err != nil {
			return workloadObservabilityStatus{}, err
		}
		return workloadObservabilityStatus{
			MetricsServiceName: cfg.MetricsServiceName,
			ConditionStatus:    metav1.ConditionTrue,
			Reason:             "MetricsServiceReady",
			Message:            fmt.Sprintf("Metrics Service %q is reconciled.", cfg.MetricsServiceName),
		}, nil
	}

	if err := reconcileWorkloadServiceMonitor(ctx, c, cfg); err != nil {
		if k8smeta.IsNoMatchError(err) {
			return workloadObservabilityStatus{
				MetricsServiceName: cfg.MetricsServiceName,
				ConditionStatus:    metav1.ConditionFalse,
				Reason:             "ServiceMonitorUnavailable",
				Message:            "ServiceMonitor CRD is not available in the cluster.",
			}, nil
		}
		return workloadObservabilityStatus{}, err
	}

	return workloadObservabilityStatus{
		MetricsServiceName: cfg.MetricsServiceName,
		ConditionStatus:    metav1.ConditionTrue,
		Reason:             "ServiceMonitorReady",
		Message:            fmt.Sprintf("Metrics Service %q and ServiceMonitor are reconciled.", cfg.MetricsServiceName),
	}, nil
}

func reconcileWorkloadMetricsService(ctx context.Context, c ctrlclient.Client, cfg workloadObservabilityConfig) error {
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: cfg.MetricsServiceName, Namespace: cfg.Owner.GetNamespace()}}
	_, err := controllerutil.CreateOrUpdate(ctx, c, svc, func() error {
		svc.Labels = mergeStringMaps(cfg.Labels, map[string]string{"fuseki.apache.org/service-role": "metrics"})
		svc.Annotations = mergeStringMaps(nil, cfg.MetricsServiceAnnotations)
		svc.Spec.Type = corev1.ServiceTypeClusterIP
		svc.Spec.Selector = mergeStringMaps(nil, cfg.Selector)
		svc.Spec.Ports = []corev1.ServicePort{{
			Name:       "http",
			Port:       cfg.MetricsServicePort,
			Protocol:   corev1.ProtocolTCP,
			TargetPort: intstr.FromInt32(cfg.MetricsServicePort),
		}}
		return controllerutil.SetControllerReference(cfg.Owner, svc, cfg.Scheme)
	})
	return err
}

func deleteWorkloadMetricsService(ctx context.Context, c ctrlclient.Client, namespace, name string) error {
	if name == "" {
		return nil
	}

	return ctrlclient.IgnoreNotFound(c.Delete(ctx, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}))
}

func reconcileWorkloadServiceMonitor(ctx context.Context, c ctrlclient.Client, cfg workloadObservabilityConfig) error {
	serviceMonitor := &unstructured.Unstructured{}
	serviceMonitor.SetGroupVersionKind(serviceMonitorGVK)
	serviceMonitor.SetNamespace(cfg.Owner.GetNamespace())
	serviceMonitor.SetName(cfg.MetricsServiceName)

	_, err := controllerutil.CreateOrUpdate(ctx, c, serviceMonitor, func() error {
		serviceMonitor.SetLabels(mergeStringMaps(cfg.Labels, cfg.ServiceMonitorLabels))
		spec := map[string]any{
			"namespaceSelector": map[string]any{"matchNames": []any{cfg.Owner.GetNamespace()}},
			"selector":          map[string]any{"matchLabels": stringMapToAnyMap(mergeStringMaps(cfg.Labels, map[string]string{"fuseki.apache.org/service-role": "metrics"}))},
			"endpoints": []any{map[string]any{
				"port":     "http",
				"path":     cfg.MetricsPath,
				"interval": cfg.ServiceMonitorInterval.String(),
			}},
		}
		if err := unstructured.SetNestedMap(serviceMonitor.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(cfg.Owner, serviceMonitor, cfg.Scheme)
	})
	return err
}

func deleteWorkloadServiceMonitor(ctx context.Context, c ctrlclient.Client, namespace, name string) error {
	if name == "" {
		return nil
	}

	serviceMonitor := &unstructured.Unstructured{}
	serviceMonitor.SetGroupVersionKind(serviceMonitorGVK)
	serviceMonitor.SetNamespace(namespace)
	serviceMonitor.SetName(name)
	err := c.Delete(ctx, serviceMonitor)
	if k8smeta.IsNoMatchError(err) {
		return nil
	}
	return ctrlclient.IgnoreNotFound(err)
}

func stringMapToAnyMap(values map[string]string) map[string]any {
	if len(values) == 0 {
		return nil
	}

	converted := make(map[string]any, len(values))
	for key, value := range values {
		converted[key] = value
	}
	return converted
}
