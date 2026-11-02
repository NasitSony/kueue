/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package jobframework

import (
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/featuregate"
	testingclock "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	configapi "sigs.k8s.io/kueue/apis/config/v1beta2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/constants"
	controllerconsts "sigs.k8s.io/kueue/pkg/controller/constants"
	"sigs.k8s.io/kueue/pkg/features"
	"sigs.k8s.io/kueue/pkg/podset"
	utiltesting "sigs.k8s.io/kueue/pkg/util/testing"
	utiltestingapi "sigs.k8s.io/kueue/pkg/util/testing/v1beta2"
	testingjob "sigs.k8s.io/kueue/pkg/util/testingjobs/job"
	"sigs.k8s.io/kueue/pkg/workloadslicing"
)

func TestDeferAdmissionCheckNodeSelectorsToPods(t *testing.T) {
	const (
		podSetName = kueue.PodSetReference("workers")
		flavorName = kueue.ResourceFlavorReference("flavor")
	)
	flavor := &kueue.ResourceFlavor{
		Spec: kueue.ResourceFlavorSpec{
			NodeLabels: map[string]string{
				"stable.example.com/node-pool": "workers",
				"shared.example.com/key":       "stable",
			},
		},
	}
	flavor.Name = string(flavorName)
	cl := utiltesting.NewClientBuilder(kueue.AddToScheme).WithObjects(flavor).Build()
	wl := &kueue.Workload{
		Spec: kueue.WorkloadSpec{
			PodSets: []kueue.PodSet{{
				Name:  podSetName,
				Count: 1,
			}},
		},
		Status: kueue.WorkloadStatus{
			Admission: &kueue.Admission{
				PodSetAssignments: []kueue.PodSetAssignment{{
					Name: podSetName,
					Flavors: map[corev1.ResourceName]kueue.ResourceFlavorReference{
						corev1.ResourceCPU: flavorName,
					},
				}},
			},
			AdmissionChecks: []kueue.AdmissionCheckState{{
				Name: "provisioning",
				PodSetUpdates: []kueue.PodSetUpdate{{
					Name: podSetName,
					NodeSelector: map[string]string{
						"request.example.com/id": "request-2",
						"shared.example.com/key": "stable",
					},
				}},
			}},
		},
	}
	info := []podset.PodSetInfo{{
		Name: podSetName,
		NodeSelector: map[string]string{
			"stable.example.com/node-pool": "workers",
			"shared.example.com/key":       "stable",
			"request.example.com/id":       "request-2",
		},
	}}

	if err := deferAdmissionCheckNodeSelectorsToPods(t.Context(), cl, wl, info); err != nil {
		t.Fatalf("deferAdmissionCheckNodeSelectorsToPods() error: %v", err)
	}

	want := map[string]string{
		"stable.example.com/node-pool": "workers",
		"shared.example.com/key":       "stable",
	}
	if diff := cmp.Diff(want, info[0].NodeSelector); diff != "" {
		t.Errorf("NodeSelector mismatch (-want,+got):\n%s", diff)
	}
}

func TestExpectedRunningPodSetsKeepsImplicitTASRequestInSync(t *testing.T) {
	features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{
		features.TopologyAwareScheduling: true,
	})

	const podIndexLabel = "batch.kubernetes.io/job-completion-index"
	assignment := utiltestingapi.MakePodSetAssignment(kueue.DefaultPodSetName).
		Count(2).
		TopologyAssignment(utiltestingapi.MakeTopologyAssignment([]string{corev1.LabelHostname}).
			Domain(utiltestingapi.MakeTopologyDomainAssignment([]string{"node-a"}, 2).Obj()).
			Obj()).
		Obj()
	wl := utiltestingapi.MakeWorkload("workload", "default").
		PodSets(*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 2).
			PodIndexLabel(new(podIndexLabel)).
			Obj()).
		ReserveQuotaAt(utiltestingapi.MakeAdmission("cluster-queue").PodSets(assignment).Obj(), time.Now()).
		Obj()

	got := expectedRunningPodSets(t.Context(), utiltesting.NewClientBuilder().Build(), wl)
	want := []kueue.PodSet{
		*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 2).
			Labels(map[string]string{
				constants.ClusterQueueLabel: "cluster-queue",
				constants.LocalQueueLabel:   "",
				constants.PodSetLabel:       string(kueue.DefaultPodSetName),
			}).
			Annotations(map[string]string{
				kueue.PodSetUnconstrainedTopologyAnnotation: "true",
				kueue.WorkloadAnnotation:                    "workload",
			}).
			NodeSelector(map[string]string{}).
			SchedulingGates(corev1.PodSchedulingGate{Name: kueue.TopologySchedulingGate}).
			UnconstrainedTopologyRequest().
			PodIndexLabel(new(podIndexLabel)).
			Obj(),
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("running PodSets (-want,+got):\n%s", diff)
	}
}

func TestClearUnusableMinCounts(t *testing.T) {
	podSetsWithMinCount := func() []kueue.PodSet {
		return []kueue.PodSet{
			*utiltestingapi.MakePodSet(kueue.DefaultPodSetName, 10).SetMinimumCount(5).Obj(),
		}
	}
	elasticWorkload := func() *utiltestingapi.WorkloadWrapper {
		return utiltestingapi.MakeWorkload("wl", "default").
			Annotation(workloadslicing.EnabledAnnotationKey, workloadslicing.EnabledAnnotationValue)
	}

	nonElastic := utiltestingapi.MakeWorkload("wl", "default").Obj()
	// The scale-up strategy annotation is set on the Job and is not propagated to the Workload, so
	// it cannot take part in this decision; opting in is enforced where MinCount is produced.
	elastic := elasticWorkload().Obj()

	cases := map[string]struct {
		partialAdmission     bool
		partialReplicaScale  bool
		wl                   *kueue.Workload
		wantMinCountsCleared bool
	}{
		"PartialAdmission enabled: minCount is kept regardless of elastic status": {
			partialAdmission:     true,
			wl:                   nonElastic,
			wantMinCountsCleared: false,
		},
		"both features disabled: minCount is cleared": {
			wl:                   nonElastic,
			wantMinCountsCleared: true,
		},
		"partial scale-up enabled, non-elastic workload: minCount is cleared": {
			partialReplicaScale:  true,
			wl:                   nonElastic,
			wantMinCountsCleared: true,
		},
		"partial scale-up enabled, elastic workload: minCount is kept": {
			partialReplicaScale:  true,
			wl:                   elastic,
			wantMinCountsCleared: false,
		},
		"elastic workload but partial scale-up disabled: minCount is cleared": {
			wl:                   elastic,
			wantMinCountsCleared: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{
				features.ElasticJobsViaWorkloadSlices:                          true,
				features.PartialAdmission:                                      tc.partialAdmission,
				features.ElasticJobsViaWorkloadSlicesWithPartialReplicaScaleUp: tc.partialReplicaScale,
			})

			got := clearUnusableMinCounts(podSetsWithMinCount(), tc.wl)

			for _, ps := range got {
				if cleared := ps.MinCount == nil; cleared != tc.wantMinCountsCleared {
					t.Errorf("podSet %q: minCount cleared = %v, want %v", ps.Name, cleared, tc.wantMinCountsCleared)
				}
			}
		})
	}
}
func TestRecordFinishedAt(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	earlier := now.Add(-time.Hour)

	cases := map[string]struct {
		jobAnnotations map[string]string
		finishedReason string
		wantAnnotation string
	}{
		"stamps when the workload finished with Succeeded": {
			finishedReason: kueue.WorkloadFinishedReasonSucceeded,
			wantAnnotation: now.Format(time.RFC3339),
		},
		"stamps when the workload finished with Failed": {
			finishedReason: kueue.WorkloadFinishedReasonFailed,
			wantAnnotation: now.Format(time.RFC3339),
		},
		"does not stamp when the workload was a replaced slice": {
			finishedReason: kueue.WorkloadSliceReplaced,
			wantAnnotation: "",
		},
		"does not overwrite an existing stamp": {
			jobAnnotations: map[string]string{
				controllerconsts.WorkloadFinishedAtAnnotation: earlier.Format(time.RFC3339),
			},
			finishedReason: kueue.WorkloadFinishedReasonSucceeded,
			wantAnnotation: earlier.Format(time.RFC3339),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, _ := utiltesting.ContextWithLog(t)

			job := testingjob.MakeJob("test-job", "default").UID("test-job").Obj()
			job.Annotations = tc.jobAnnotations

			wl := utiltestingapi.MakeWorkload("wl", "default").Obj()
			apimeta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{
				Type:               kueue.WorkloadFinished,
				Status:             metav1.ConditionTrue,
				Reason:             tc.finishedReason,
				Message:            "test",
				LastTransitionTime: metav1.NewTime(now),
			})

			cl := utiltesting.NewClientBuilder(batchv1.AddToScheme, kueue.AddToScheme).
				WithObjects(utiltesting.MakeNamespace("default")).
				WithObjects(job, wl).
				Build()

			reconciler := NewReconciler(cl, &utiltesting.EventRecorder{})

			if err := reconciler.recordFinishedAt(ctx, job, wl); err != nil {
				t.Fatalf("recordFinishedAt: %v", err)
			}

			got := &batchv1.Job{}
			if err := cl.Get(ctx, client.ObjectKeyFromObject(job), got); err != nil {
				t.Fatalf("getting job: %v", err)
			}
			if gotAnnotation := got.Annotations[controllerconsts.WorkloadFinishedAtAnnotation]; gotAnnotation != tc.wantAnnotation {
				t.Errorf("annotation = %q, want %q", gotAnnotation, tc.wantAnnotation)
			}
		})
	}
}
func TestHandleOwnerAfterFinishedPolicy(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	finishedAt := now.Add(-2 * time.Hour)
	oneHour := &configapi.ObjectRetentionPolicies{
		Jobs: &configapi.JobRetentionPolicy{
			AfterFinished: &metav1.Duration{Duration: time.Hour},
		},
	}
	stamp := map[string]string{
		controllerconsts.WorkloadFinishedAtAnnotation: finishedAt.Format(time.RFC3339),
	}

	cases := map[string]struct {
		jobAnnotations   map[string]string
		retention        *configapi.ObjectRetentionPolicies
		wantDeleted      bool
		wantRequeue      time.Duration
		gateEnabled      bool
		workloadFinished *bool
	}{
		"deletes the owner once the retention period has elapsed": {
			jobAnnotations: stamp,
			retention:      oneHour,
			wantDeleted:    true,
			gateEnabled:    true,
		},
		"requeues while the retention period has not elapsed": {
			jobAnnotations: map[string]string{
				controllerconsts.WorkloadFinishedAtAnnotation: now.Add(-30 * time.Minute).Format(time.RFC3339),
			},
			retention:   oneHour,
			wantRequeue: 30 * time.Minute,
			gateEnabled: true,
		},
		"does nothing when the owner carries no stamp": {
			retention:   oneHour,
			gateEnabled: true,
		},
		"does nothing when retention is unset": {
			jobAnnotations: stamp,
			gateEnabled:    true,
		},
		"does nothing when the feature gate is disabled": {
			jobAnnotations: stamp,
			retention:      oneHour,
			gateEnabled:    false,
		},
		"ignores a stamp while the workload is still unfinished": {
			jobAnnotations:   stamp,
			retention:        oneHour,
			gateEnabled:      true,
			workloadFinished: new(false),
		},
		"ignores a finish timestamp in the future": {
			jobAnnotations: map[string]string{
				controllerconsts.WorkloadFinishedAtAnnotation: now.Add(time.Hour).Format(time.RFC3339),
			},
			retention:   oneHour,
			gateEnabled: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			features.SetFeatureGatesDuringTest(t, map[featuregate.Feature]bool{
				features.JobOwnerRetentionPolicy: tc.gateEnabled,
			})

			ctx, _ := utiltesting.ContextWithLog(t)

			job := testingjob.MakeJob("test-job", "default").UID("test-job").Obj()
			job.Annotations = tc.jobAnnotations

			cl := utiltesting.NewClientBuilder(batchv1.AddToScheme, kueue.AddToScheme).
				WithObjects(utiltesting.MakeNamespace("default")).
				WithObjects(job).
				Build()

			reconciler := NewReconciler(cl, &utiltesting.EventRecorder{},
				WithObjectRetentionPolicies(tc.retention),
				WithClock(testingclock.NewFakeClock(now)),
			)

			var wl *kueue.Workload
			if tc.workloadFinished != nil {
				wl = utiltestingapi.MakeWorkload("wl", "default").Obj()
				if *tc.workloadFinished {
					apimeta.SetStatusCondition(&wl.Status.Conditions, metav1.Condition{
						Type:   kueue.WorkloadFinished,
						Status: metav1.ConditionTrue,
						Reason: kueue.WorkloadFinishedReasonSucceeded,
					})
				}
			}

			gotRequeue, err := reconciler.handleOwnerAfterFinishedPolicy(ctx, job, wl)
			if err != nil {
				t.Fatalf("handleOwnerAfterFinishedPolicy: %v", err)
			}

			got := &batchv1.Job{}
			getErr := cl.Get(ctx, client.ObjectKeyFromObject(job), got)
			if gotDeleted := apierrors.IsNotFound(getErr); gotDeleted != tc.wantDeleted {
				t.Errorf("deleted = %v, want %v", gotDeleted, tc.wantDeleted)
			}
			if gotRequeue != tc.wantRequeue {
				t.Errorf("requeueAfter = %v, want %v", gotRequeue, tc.wantRequeue)
			}
		})
	}
}
