/*
Copyright 2022 Red Hat
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

package helpers

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	ginkgo "github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	"github.com/openstack-k8s-operators/lib-common/modules/common/condition"
	k8s_errors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

type conditionsGetter interface {
	GetConditions(name types.NamespacedName) condition.Conditions
}

// ConditionGetterFunc - recieves custom condition getters for operators specific needs
type ConditionGetterFunc func(name types.NamespacedName) condition.Conditions

// GetConditions - implements conditions getter for operators specific needs
func (f ConditionGetterFunc) GetConditions(name types.NamespacedName) condition.Conditions {
	return f(name)
}

// TestHelper -
type TestHelper struct {
	k8sClient client.Client
	ctx       context.Context
	timeout   time.Duration
	interval  time.Duration
	logger    logr.Logger
}

// NewTestHelper returns a TestHelper
func NewTestHelper(
	ctx context.Context,
	k8sClient client.Client,
	timeout time.Duration,
	interval time.Duration,
	logger logr.Logger,
) *TestHelper {
	return &TestHelper{
		ctx:       ctx,
		k8sClient: k8sClient,
		timeout:   getTestTimeout(timeout),
		interval:  interval,
		logger:    logger,
	}
}

// getTestTimeout returns test timeout from TEST_TIMEOUT_SEC environment
// variable, in seconds; or picks defaultTimeout, in milliseconds
func getTestTimeout(defaultTimeout time.Duration) time.Duration {
	t := os.Getenv("TEST_TIMEOUT_SEC")
	timeout, err := strconv.Atoi(t)
	if err != nil {
		return defaultTimeout
	}
	return time.Duration(timeout) * time.Second
}

// SkipInExistingCluster -
func (tc *TestHelper) SkipInExistingCluster(message string) {
	s := os.Getenv("USE_EXISTING_CLUSTER")
	v, err := strconv.ParseBool(s)

	if err == nil && v {
		ginkgo.Skip("Skipped running against existing cluster. " + message)
	}

}

// GetName -
func (tc *TestHelper) GetName(obj client.Object) types.NamespacedName {
	return types.NamespacedName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
}

// DeleteInstance -
func (tc *TestHelper) DeleteInstance(instance client.Object, opts ...client.DeleteOption) {
	// We have to wait for the controller to fully delete the instance
	tc.logger.Info(
		"Deleting", "Name", instance.GetName(),
		"Namespace", instance.GetNamespace(),
		"Kind", instance.GetObjectKind(),
	)

	gomega.Eventually(func(g gomega.Gomega) {
		name := types.NamespacedName{Name: instance.GetName(), Namespace: instance.GetNamespace()}
		err := tc.k8sClient.Get(tc.ctx, name, instance)
		// if it is already gone that is OK
		if k8s_errors.IsNotFound(err) {
			return
		}
		g.Expect(err).ShouldNot(gomega.HaveOccurred())

		g.Expect(tc.k8sClient.Delete(tc.ctx, instance, opts...)).Should(gomega.Succeed())

		err = tc.k8sClient.Get(tc.ctx, name, instance)
		g.Expect(k8s_errors.IsNotFound(err)).To(gomega.BeTrue())
	}, tc.timeout, tc.interval).Should(gomega.Succeed())

	tc.logger.Info(
		"Deleted", "Name", instance.GetName(),
		"Namespace", instance.GetNamespace(),
		"Kind", instance.GetObjectKind().GroupVersionKind().Kind,
	)
}
