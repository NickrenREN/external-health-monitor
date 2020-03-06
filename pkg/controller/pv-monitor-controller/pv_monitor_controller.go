package controller

import (
	"fmt"
	"time"
	"sync"

	"k8s.io/klog"

	"google.golang.org/grpc"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"

	"k8s.io/client-go/util/workqueue"

	"github.com/kubernetes-csi/external-health-monitor/pkg/monitor"
	"k8s.io/apimachinery/pkg/labels"
)

type PodSet map[string]*v1.Pod

type PVMonitorController struct {
	client        kubernetes.Interface
	monitorName string
	pvMonitor *monitor.PVMonitor

	csiConn         *grpc.ClientConn

	pvLister corelisters.PersistentVolumeLister
	pvListerSynced cache.InformerSynced

	pvcLister corelisters.PersistentVolumeClaimLister
	pvcListerSynced cache.InformerSynced

	podLister corelisters.PodLister
	podListerSynced cache.InformerSynced

	sync.Mutex
	// caches pvc/pods mapping info, key is pvc uid
	pvcToPodsMap map[string]PodSet
	pvEnqueued map[string]bool

	pvQueue workqueue.RateLimitingInterface

	shouldReconcilePVs bool
}

const (
	pvReconcileSyncInterval = 10 * time.Minute
)

func NewPVMonitorController(client kubernetes.Interface, monitorName string, conn *grpc.ClientConn, pvMonitor *monitor.PVMonitor, pvInformer coreinformers.PersistentVolumeInformer,
	pvcInformer coreinformers.PersistentVolumeClaimInformer, podInformer coreinformers.PodInformer,
	shouldReconcilePVs bool) *PVMonitorController {

	ctrl := &PVMonitorController{
		csiConn:conn,
		pvMonitor:pvMonitor,
		client:client,
		monitorName:monitorName,
		shouldReconcilePVs:shouldReconcilePVs,
		pvQueue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-monitor-pv-queue"),

		pvcToPodsMap: make(map[string]PodSet),
		pvEnqueued: make(map[string]bool),
	}

	// PV informer
	pvInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.pvAdded,
		UpdateFunc: ctrl.pvUpdated,
		//DeleteFunc: ctrl.pvDeleted, TODO: do we need this?
	})
	ctrl.pvLister = pvInformer.Lister()
	ctrl.pvListerSynced = pvInformer.Informer().HasSynced

	// PVC informer
	ctrl.pvcLister = pvcInformer.Lister()
	ctrl.pvListerSynced = pvcInformer.Informer().HasSynced

	// Pod informer
	podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:   ctrl.podAdded,
		// UpdateFunc: ctrl.podUpdated,  TODO: do we need this ?
		DeleteFunc: ctrl.podDeleted,
	})
	ctrl.podLister = podInformer.Lister()
	ctrl.podListerSynced = podInformer.Informer().HasSynced


	return ctrl
}


func (ctrl *PVMonitorController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.pvQueue.ShutDown()


	klog.Infof("Starting CSI External PV Health Monitor Controller")
	defer klog.Infof("Shutting CSI External PV Health Monitor Controller")

	if !cache.WaitForCacheSync(stopCh, ctrl.pvcListerSynced, ctrl.pvListerSynced, ctrl.podListerSynced) {
		klog.Errorf("Cannot sync caches")
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.syncPV, 0, stopCh)
	}

	if ctrl.shouldReconcilePVs {
		go wait.Until(func() {
			err := ctrl.ReconcilePVs()
			if err != nil {
				klog.Errorf("Failed to reconcile volume attachments: %v", err)
			}
		}, pvReconcileSyncInterval, stopCh)
	}

	<-stopCh
}

func (ctrl *PVMonitorController) ReconcilePVs() error {
	// TODO: add PV filters when listing
	pvs, err := ctrl.pvLister.List(labels.Everything())
	if err != nil {
		// klog.Errorf("list PVs error: %+v", err)
		return err
	}

	for _, pv := range pvs {
		if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != ctrl.monitorName {
			continue
		}
		ctrl.Lock()
		if !ctrl.pvEnqueued[pv.Name] {
			ctrl.pvEnqueued[pv.Name] = true
			ctrl.pvQueue.Add(pv)
		}
		ctrl.Unlock()
	}

	return nil
}

