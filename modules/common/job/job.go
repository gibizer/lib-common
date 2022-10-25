/*
Copyright 2021 Red Hat

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

package job

import (
	"context"
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/openstack-k8s-operators/lib-common/modules/common/helper"
	"github.com/openstack-k8s-operators/lib-common/modules/common/util"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// NewJob returns an initialized Job.
func NewJob(
	job *batchv1.Job,
	jobType string,
	preserve bool,
	timeout int,
	beforeHash string,
) *Job {

	return &Job{
		job:        job,
		jobType:    jobType,
		preserve:   preserve,
		timeout:    time.Duration(timeout) * time.Second, // timeout to set in s to reconcile
		beforeHash: beforeHash,
		changed:    false,
		exists:     false,
	}
}

// createJob - creates job
func (j *Job) createJob(
	ctx context.Context,
	h *helper.Helper,
) (JobStatus, error) {
	op, err := controllerutil.CreateOrPatch(ctx, h.GetClient(), j.job, func() error {
		err := controllerutil.SetControllerReference(h.GetBeforeObject(), j.job, h.GetScheme())
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		if k8s_errors.IsNotFound(err) {
			h.GetLogger().Info(fmt.Sprintf("Job %s not found", j.job.Name))
			return Running, nil
		}
		return Failed, err
	}
	if op != controllerutil.OperationResultNone {
		h.GetLogger().Info(fmt.Sprintf("Job %s %s - %s", j.jobType, j.job.Name, op))
		return Running, nil
	}

	return Running, nil
}

func (j *Job) defaultTTL() {
	// preserve has higher priority than having any kind of TTL
	if j.preserve {
		// so we reset TTL to avoid automatic deletion
		j.job.Spec.TTLSecondsAfterFinished = nil
		return
	}
	// if the client set a specific TTL then we honore it.
	if j.job.Spec.TTLSecondsAfterFinished != nil {
		return
	}
	// we are here as preserve is false and no TTL is set. We apply a default
	// TTL:
	// i) to make sure that the Job is eventually cleaned up
	// ii) to trigger the Job deletion with a delay to avoid racing between
	// Job deletion and callers reading old CR data from caches and re-creating
	// the Job. See more in https://github.com/openstack-k8s-operators/nova-operator/issues/110
	ttl := defaultTTL
	j.job.Spec.TTLSecondsAfterFinished = &ttl
}

//
// DoJob - run a job if the hashBefore and hash is different. If there is an existing job, wait for the job
// to finish. Right now we do not expect the job to change while running.
// If TTLSecondsAfterFinished is unset on the Job and preserve is false, the Job
// will be deleted after 10 minutes. Set preserve to true if you want to keep
// the job, or set a specific value to job.Spec.TTLSecondsAfterFinished to
// define when the Job should be deleted.
//
func (j *Job) DoJob(
	ctx context.Context,
	h *helper.Helper,
) (JobStatus, error) {
	var err error

	j.hash, err = util.ObjectHash(j.job)
	if err != nil {
		return Failed, fmt.Errorf("error calculating %s hash: %v", j.jobType, err)
	}

	// if the hash changed the job should run
	if j.beforeHash != j.hash {
		j.changed = true
	}

	// NOTE(gibi): This should be in NewJob but then the defaulting would affect
	// the hash of the Job calculated in DoJob above. As preserve can change
	// after the job finished and preserve is implemented by changing TTL the
	// change of preserve would change the hash of the Job after such hash is
	// persisted by the caller.
	// Moving hash calculation and defaultTTL to NewJob would be logically
	// possible but as hash calculation might fail NewJob inteface would need
	// to be changed to report the possible error.
	j.defaultTTL()

	//
	// Check if this job already exists
	//
	job, err := GetJobWithName(ctx, h, j.job.Name, j.job.Namespace)
	if err != nil && !k8s_errors.IsNotFound(err) {
		return Failed, err
	}

	// NOTE(gibi): we don't support changing a job while it is running.
	if k8s_errors.IsNotFound(err) && j.changed {
		// if job's hash is changed and we haven't started a job yet then we
		// need to create a new job
		status, err := j.createJob(ctx, h)
		if err != nil {
			return status, err
		}
		j.exists = true
	}

	if err == nil { // so there is a Job either in progress or finished
		j.exists = true
		j.job = job

		// allow updating TTLSecondsAfterFinished even after the job is finished
		_, err = controllerutil.CreateOrPatch(ctx, h.GetClient(), job, func() error {
			job.Spec.TTLSecondsAfterFinished = j.job.Spec.TTLSecondsAfterFinished
			return nil
		})
		if err != nil {
			return Failed, err
		}
	}

	// Calculate job status
	if !j.exists {
		if !j.HasChanged() {
			// Job succeeded in the past and already got deleted
			util.LogForObject(h, "Job succeeded and deleted", j.job)
			return Succeeded, nil
		} else {
			// Created a new Job as hash changed but cannot read it back due to
			// caching.
			util.LogForObject(h, "Job is running (cache)", j.job)
			return Running, nil
		}
	} else { // the Job exists
		if j.job.Status.Failed > 0 {
			util.LogForObject(h, "Job failed", j.job)
			return Failed, nil
		}
		if j.job.Status.Succeeded > 0 {
			util.LogForObject(h, "Job succeeded", j.job)
			return Succeeded, nil
		}
		util.LogForObject(h, "Job is running", j.job)
		return Running, nil
	}
}

// HasChanged func
func (j *Job) HasChanged() bool {
	return j.changed
}

// GetHash func
func (j *Job) GetHash() string {
	return j.hash
}

// SetTimeout defines the duration used for requeueing while waiting for the job
// to finish.
func (j *Job) SetTimeout(timeout time.Duration) {
	j.timeout = timeout
}

// DeleteJob func
// kclient required to properly cleanup the job depending pods with DeleteOptions
func DeleteJob(
	ctx context.Context,
	h *helper.Helper,
	name string,
	namespace string,
) error {
	foundJob, err := h.GetKClient().BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		h.GetLogger().Info("Deleting Job", "Job.Namespace", namespace, "Job.Name", name)
		background := metav1.DeletePropagationBackground
		err = h.GetKClient().BatchV1().Jobs(foundJob.Namespace).Delete(
			ctx, foundJob.Name, metav1.DeleteOptions{PropagationPolicy: &background})
		if err != nil {
			return err
		}
		return err
	}
	return nil
}

// GetJobWithName func
func GetJobWithName(
	ctx context.Context,
	h *helper.Helper,
	name string,
	namespace string,
) (*batchv1.Job, error) {

	// Check if this Job already exists
	job := &batchv1.Job{}
	err := h.GetClient().Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, job)
	if err != nil {
		if k8s_errors.IsNotFound(err) {
			return job, err
		}
		h.GetLogger().Info("GetJobWithName err")
		return job, err
	}

	return job, nil
}
