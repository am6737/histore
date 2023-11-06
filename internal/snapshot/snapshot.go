package snapshot

import (
	"context"
	"fmt"
	hitoseacomv1 "github.com/am6737/histore/api/v1"
	vsv1 "github.com/kubernetes-csi/external-snapshotter/client/v6/apis/volumesnapshot/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	defaultVolumeSnapshotClassAnnotation = "snapshot.storage.kubernetes.io/is-default-class"
	snapshotSourceNameLabel              = "snapshot.hitosea.com/source-vm-name"
	snapshotSourceNamespaceLabel         = "snapshot.hitosea.com/source-vm-namespace"
)

type VMSnapshotController interface {
	CreateVolumeSnapshot(content *hitoseacomv1.VirtualMachineSnapshotContent, volumeBackup *hitoseacomv1.VolumeBackup) (*vsv1.VolumeSnapshot, error)
	DeleteVolumeSnapshot()
}

type vmSnapshotController struct {
	client.Client
}

func (c *vmSnapshotController) DeleteVolumeSnapshot() {

}

func (c *vmSnapshotController) CreateVolumeSnapshot(
	content *hitoseacomv1.VirtualMachineSnapshotContent,
	volumeBackup *hitoseacomv1.VolumeBackup,
) (*vsv1.VolumeSnapshot, error) {

	sc := volumeBackup.PersistentVolumeClaim.Spec.StorageClassName
	if sc == nil {
		return nil, fmt.Errorf("%s/%s VolumeSnapshot requested but no storage class",
			content.Namespace, volumeBackup.PersistentVolumeClaim.Name)
	}

	volumeSnapshotClass, err := c.getVolumeSnapshotClass(*sc)
	if err != nil {
		//log.Log.Warningf("Couldn't find VolumeSnapshotClass for %s", *sc)
		return nil, err
	}

	t := true
	vs := &vsv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: *volumeBackup.VolumeSnapshotName,
			Labels: map[string]string{
				snapshotSourceNameLabel:      content.Spec.Source.VirtualMachine.Name,
				snapshotSourceNamespaceLabel: content.Spec.Source.VirtualMachine.Namespace,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         hitoseacomv1.GroupVersion.String(),
					Kind:               "VirtualMachineSnapshotContent",
					Name:               content.Name,
					UID:                content.UID,
					Controller:         &t,
					BlockOwnerDeletion: &t,
				},
			},
		},
		Spec: vsv1.VolumeSnapshotSpec{
			Source: vsv1.VolumeSnapshotSource{
				PersistentVolumeClaimName: &volumeBackup.PersistentVolumeClaim.Name,
			},
			VolumeSnapshotClassName: &volumeSnapshotClass,
		},
	}

	err = c.Client.Create(context.Background(), vs, &client.CreateOptions{})
	if err != nil {
		return nil, err
	}

	//ctrl.Recorder.Eventf(
	//	content,
	//	corev1.EventTypeNormal,
	//	volumeSnapshotCreateEvent,
	//	"Successfully created VolumeSnapshot %s",
	//	snapshot.Name,
	//)

	return vs, nil

	//return volumeSnapshot, nil
}

func (c *vmSnapshotController) getVolumeSnapshotClass(storageClassName string) (string, error) {
	//obj, exists, err := ctrl.StorageClassInformer.GetStore().GetByKey(storageClassName)
	//if !exists || err != nil {
	//	return "", err
	//}
	var obj interface{}

	storageClass := obj.(*storagev1.StorageClass).DeepCopy()

	var matches []vsv1.VolumeSnapshotClass
	volumeSnapshotClasses := c.getVolumeSnapshotClasses()
	for _, volumeSnapshotClass := range volumeSnapshotClasses {
		if volumeSnapshotClass.Driver == storageClass.Provisioner {
			matches = append(matches, volumeSnapshotClass)
		}
	}

	if len(matches) == 0 {
		//log.Log.Warningf("No VolumeSnapshotClass for %s", storageClassName)
		return "", nil
	}

	if len(matches) == 1 {
		return matches[0].Name, nil
	}

	for _, volumeSnapshotClass := range matches {
		for annotation := range volumeSnapshotClass.Annotations {
			if annotation == defaultVolumeSnapshotClassAnnotation {
				return volumeSnapshotClass.Name, nil
			}
		}
	}

	return "", fmt.Errorf("%d matching VolumeSnapshotClasses for %s", len(matches), storageClassName)
}

func (c *vmSnapshotController) getVolumeSnapshotClasses() []vsv1.VolumeSnapshotClass {
	//di := ctrl.dynamicInformerMap[volumeSnapshotClassCRD]
	//di.mutex.Lock()
	//defer di.mutex.Unlock()
	//
	//if di.informer == nil {
	//	return nil
	//}

	var vscs []vsv1.VolumeSnapshotClass
	//objs := di.informer.GetStore().List()
	//for _, obj := range objs {
	//	vsc := obj.(*vsv1.VolumeSnapshotClass).DeepCopy()
	//	vscs = append(vscs, *vsc)
	//}

	return vscs
}
