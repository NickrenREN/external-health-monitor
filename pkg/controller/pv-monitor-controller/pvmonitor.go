package monitor

import (
	"fmt"

	"google.golang.org/grpc"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

type PVMonitor struct {
	monitorName string
	csiConn         *grpc.ClientConn
	k8sClient kubernetes.Interface

	eventRecorder record.EventRecorder
}

func NewPVMonitor(name string, conn *grpc.ClientConn, kClient kubernetes.Interface) *PVMonitor {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: kClient.CoreV1().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: fmt.Sprintf("csi-monitor %s", name)})

	return &PVMonitor{
		monitorName: name,
		csiConn: conn,
		k8sClient: kClient,
		eventRecorder:eventRecorder,
	}
}

func (monitor *PVMonitor) CheckVolumeStatus() error {}