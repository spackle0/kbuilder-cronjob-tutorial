/*
Copyright 2026.

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

package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/robfig/cron"
	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	batchv1 "tutorial.kubebuilder.io/project/api/v1"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

type realClock struct{}

func (_ realClock) Now() time.Time { return time.Now() } //nolint:staticcheck

// Clock knows how to get the current time.
// It can be used to fake out timing for testing.
type Clock interface {
	Now() time.Time
}

// Definitions to manage status conditions
const (
	// typeAvailableCronJob represents the status of the CronJob reconciliation
	typeAvailableCronJob = "Available"
	// typeProgressingCronJob represents the status used when the CronJob is being reconciled
	typeProgressingCronJob = "Progressing"
	// typeDegradedCronJob represents the status used when the CronJob has encountered an error
	typeDegradedCronJob = "Degraded"
)

// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/finalizers,verbs=update
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

var (
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
)

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		if apierrors.IsNotFound(err) {
			// If the custom resource is not found, then it usually means that it was deleted or not created.
			// In this way, we will stop the reconciliation
			log.Info("CronJob resource not found. Ignoring since object must be deleted.")
			return ctrl.Result{}, nil
		}
		// Error reading the object - requeue the request.
		log.Error(err, "Failed to get CronJob")
		return ctrl.Result{}, err
	}

	// Initialize status conditions if not yet present
	if len(cronJob.Status.Conditions) == 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionUnknown,
			Reason:  "Reconciling",
			Message: "Starting reconciliation",
		})
		if err := r.Status().Update(ctx, &cronJob); err != nil {
			log.Error(err, "Failed to update CronJob status")
			return ctrl.Result{}, err
		}
		if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
			log.Error(err, "Failed to re-fetch CronJob")
			return ctrl.Result{}, err
		}
	}

	var childJobs kbatch.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace),
		client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to list child Jobs")

		if fetchErr := r.Get(ctx, req.NamespacedName, &cronJob); fetchErr != nil {
			log.Error(fetchErr, "Failed to re-fetch CronJob")
			return ctrl.Result{}, fetchErr
		}
		// Update status condition to reflect the error
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "ReconciliationError",
			Message: fmt.Sprintf("Failed to list child jobs: %v", err),
		})
		if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
			log.Error(statusErr, "Failed to update CronJob status")
		}
		return ctrl.Result{}, err
	}
	// find the active list of jobs
	var activeJobs []*kbatch.Job
	var successfulJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	var mostRecentTime *time.Time // find the last run so we can update the status

	isJobFinished := func(job *kbatch.Job) (bool, kbatch.JobConditionType) {
		for _, c := range job.Status.Conditions {
			if (c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed) && c.Status == corev1.ConditionTrue {
				return true, c.Type
			}
		}
		return false, ""
	}

	getScheduledTimeForJob := func(job *kbatch.Job) (*time.Time, error) {
		timeRaw := job.Annotations[scheduledTimeAnnotation]
		if len(timeRaw) == 0 {
			return nil, nil
		}

		timeParsed, err := time.Parse(time.RFC3339, timeRaw)
		if err != nil {
			return nil, err
		}
		return &timeParsed, nil
	}

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": // ongoing
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
		}

		// We'll store the launch time in an annotation, so we'll reconstitute that from
		// the active jobs themselves
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job", "job", &job)
			continue
		}
		if scheduledTimeForJob != nil {
			if mostRecentTime == nil || mostRecentTime.Before(*scheduledTimeForJob) {
				mostRecentTime = scheduledTimeForJob
			}
		}
	}

	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}
	cronJob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := reference.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", activeJob)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	log.V(1).Info("job count",
		"active jobs", len(activeJobs),
		"successful jobs", len(successfulJobs),
		"failed jobs", len(failedJobs))

	// Check of CronJob is suspended
	isSuspended := cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend

	if isSuspended {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "Suspended",
			Message: "CronJob is suspended",
		})
	} else if len(failedJobs) > 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
	} else if len(activeJobs) > 0 {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("%d job(s) are currently active", len(activeJobs)),
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "JobsActive",
			Message: fmt.Sprintf("CronJob is progressing with %d active job(s)", len(activeJobs)),
		})
	} else {
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeAvailableCronJob,
			Status:  metav1.ConditionTrue,
			Reason:  "AllJobsCompleted",
			Message: "All jobs have completed successfully",
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeProgressingCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "NoJobsActive",
			Message: "No jobs are currently active",
		})
	}

	if err := r.Status().Update(ctx, &cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}
	// NB: deleting these are "best effort" -- if we fail on a particular one,
	// we won't requeue just to finish the deleting.
	if cronJob.Spec.FailedJobsHistoryLimit != nil {
		slices.SortStableFunc(failedJobs, func(a, b *kbatch.Job) int {
			aStartTime := a.Status.StartTime
			bStartTime := b.Status.StartTime
			if aStartTime == nil && bStartTime != nil {
				return 1
			}

			if aStartTime.Before(bStartTime) {
				return -1
			} else if bStartTime.Before(aStartTime) {
				return 1
			}
			return 0
		})
		for i, job := range failedJobs {
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete old failed job", "job", job)
			} else {
				log.V(1).Info("deleted old failed job", "job", job)
			}
		}
	}

	if cronJob.Spec.SuccessfulJobsHistoryLimit != nil {
		slices.SortStableFunc(successfulJobs, func(a, b *kbatch.Job) int {
			aStartTime := a.Status.StartTime
			bStartTime := b.Status.StartTime
			if aStartTime == nil && bStartTime == nil {
				return 1
			}
			if aStartTime.Before(bStartTime) {
				return -1
			} else if bStartTime.Before(aStartTime) {
				return 1
			}
			return 0
		})
		for i, job := range successfulJobs {
			if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SuccessfulJobsHistoryLimit {
				break
			}
			if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
				log.Error(err, "unable to delete old successful job", "job", job)
			} else {
				log.V(1).Info("deleted old successful job", "job", job)
			}
		}
	}

	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	getNextSchedule := func(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
		sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("unparseable shedule %q: %w", cronJob.Spec.Schedule, err)
		}

		// for optimization purposes, cheat a bit and start from our last observed run time
		// we could reconstitute this here, but there's not much point, since we've
		// just updated it.
		var earliestTime time.Time
		if cronJob.Status.LastScheduleTime != nil {
			earliestTime = cronJob.Status.LastScheduleTime.Time
		} else {
			earliestTime = cronJob.CreationTimestamp.Time
		}
		if cronJob.Spec.StartingDeadlineSeconds != nil {
			// controller is not going to schedule anything below this point
			schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))

			if schedulingDeadline.After(earliestTime) {
				earliestTime = schedulingDeadline
			}
		}
		if earliestTime.After(now) {
			return time.Time{}, sched.Next(now), nil
		}

		starts := 0
		for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
			lastMissed = t
			// An object might miss several starts. For example, if
			// controller gets wedged on Friday at 5:01pm when everyone has
			// gone home, and someone comes in on Tuesday AM and discovers
			// the problem and restarts the controller, then all the hourly
			// jobs, more than 80 of them for one hourly scheduledJob, should
			// all start running with no further intervention (if the scheduledJob
			// allows concurrency and late starts).
			//
			// However, if there is a bug somewhere, or incorrect clock
			// on controller's server or apiservers (for setting creationTimestamp)
			// then there could be so many missed start times (it could be off
			// by decades or more), that it would eat up all the CPU and memory
			// of this controller. In that case, we want to not try to list
			// all the missed start times.
			starts++
			if starts > 100 {
				// We can't get the most recent times so just return an empty slice
				return time.Time{}, time.Time{}, fmt.Errorf("Too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew.") //nolint:staticcheck
			}
		}
		return lastMissed, sched.Next(now), nil
	}
	// figure out the next times that we need to create
	// jobs at (or anything we missed).
	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out CroJob schedule")
		if fetchErr := r.Get(ctx, req.NamespacedName, &cronJob); fetchErr != nil {
			log.Error(fetchErr, "Failed to re-fetch CronJob")
			return ctrl.Result{}, fetchErr
		}
		// Update the status condition to reflect the schedule error
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type:    typeDegradedCronJob,
			Status:  metav1.ConditionFalse,
			Reason:  "InvalidSchedule",
			Message: fmt.Sprintf("Failed to parse schedule: %v", err),
		})
		if statusErr := r.Status().Update(ctx, &cronJob); statusErr != nil {
			log.Error(statusErr, "Failed to update CronJob status")
		}
		// we don't really care about requeuing until we fet an update that
		// fixes the schedule, so don't return an error
		return ctrl.Result{}, nil
	}

	// We'll prep our eventual request to requeue until the next job, and then figure out if we actually need to run
	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())} // save this so we can re-use it elsewhere
	log = log.WithValues("now", r.Now(), "next run", nextRun)

}

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Named("cronjob").
		Complete(r)
}
