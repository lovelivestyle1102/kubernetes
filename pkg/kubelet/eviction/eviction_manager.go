/*
Copyright 2016 The Kubernetes Authors.

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

package eviction

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"k8s.io/klog"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/clock"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	apiv1resource "k8s.io/kubernetes/pkg/api/v1/resource"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	statsapi "k8s.io/kubernetes/pkg/kubelet/apis/stats/v1alpha1"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/pkg/kubelet/server/stats"
	kubelettypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	schedulerapi "k8s.io/kubernetes/pkg/scheduler/api"
)

const (
	podCleanupTimeout  = 30 * time.Second
	podCleanupPollFreq = time.Second
)

// managerImpl implements Manager
type managerImpl struct {
	//  used to track time
	clock clock.Clock

	// config is how the manager is configured
	config Config

	// the function to invoke to kill a pod
	// kill pod的方法
	killPodFunc KillPodFunc

	// the function to get the mirror pod by a given statid pod
	// 获取静态pod的mirror pod 方法
	mirrorPodFunc MirrorPodFunc

	// the interface that knows how to do image gc
	//  当node出现diskPressure condition时，imageGC进行unused images删除操作以回收disk space。
	imageGC ImageGC

	// the interface that knows how to do container gc
	containerGC ContainerGC

	// protects access to internal state
	sync.RWMutex

	// node conditions are the set of conditions present
	nodeConditions []v1.NodeConditionType

	// captures when a node condition was last observed based on a threshold being met
	nodeConditionsLastObservedAt nodeConditionsObservedAt

	// nodeRef is a reference to the node
	nodeRef *v1.ObjectReference

	// used to record events about the node
	recorder record.EventRecorder

	// used to measure usage stats on system
	// 提供node和node上所有pods的最新status数据汇总，既NodeStats and []PodStats。
	summaryProvider stats.SummaryProvider

	// records when a threshold was first observed
	thresholdsFirstObservedAt thresholdsObservedAt

	// records the set of thresholds that have been met (including graceperiod) but not yet resolved
	// 保存已经触发但还没解决的Thresholds，包括那些处于grace period等待阶段的Thresholds。
	thresholdsMet []evictionapi.Threshold

	// signalToRankFunc maps a resource to ranking function for that resource.
	// 定义各Resource进行evict挑选时的排名方法。
	signalToRankFunc map[evictionapi.Signal]rankFunc

	// signalToNodeReclaimFuncs maps a resource to an ordered list of functions that know how to reclaim that resource.
	// 定义各Resource进行回收时调用的方法
	signalToNodeReclaimFuncs map[evictionapi.Signal]nodeReclaimFuncs

	// last observations from synchronize
	lastObservations signalObservations

	// dedicatedImageFs indicates if imagefs is on a separate device from the rootfs
	dedicatedImageFs *bool

	// thresholdNotifiers is a list of memory threshold notifiers which each notify for a memory eviction threshold
	// 内存阈值通知器集合
	thresholdNotifiers []ThresholdNotifier

	// thresholdsLastUpdated is the last time the thresholdNotifiers were updated.
	// 上次thresholdNotifiers发通知的时间
	thresholdsLastUpdated time.Time

	// etcHostsPath is a function that will get the etc-hosts file's path for a pod given its UID
	etcHostsPath func(podUID types.UID) string
}

// ensure it implements the required interface
// 校验是否完整实现接口
var _ Manager = &managerImpl{}

// NewManager returns a configured Manager and an associated admission handler to enforce eviction configuration.
func NewManager(
	summaryProvider stats.SummaryProvider,
	config Config,
	killPodFunc KillPodFunc,
	mirrorPodFunc MirrorPodFunc,
	imageGC ImageGC,
	containerGC ContainerGC,
	recorder record.EventRecorder,
	nodeRef *v1.ObjectReference,
	clock clock.Clock,
	etcHostsPath func(types.UID) string,
) (Manager, lifecycle.PodAdmitHandler) {
	manager := &managerImpl{
		clock:                        clock,
		killPodFunc:                  killPodFunc,
		mirrorPodFunc:                mirrorPodFunc,
		imageGC:                      imageGC,
		containerGC:                  containerGC,
		config:                       config,
		recorder:                     recorder,
		summaryProvider:              summaryProvider,
		nodeRef:                      nodeRef,
		nodeConditionsLastObservedAt: nodeConditionsObservedAt{},
		thresholdsFirstObservedAt:    thresholdsObservedAt{},
		dedicatedImageFs:             nil,
		thresholdNotifiers:           []ThresholdNotifier{},
		etcHostsPath:                 etcHostsPath,
	}
	return manager, manager
}

// Admit rejects a pod if its not safe to admit for node stability.
func (m *managerImpl) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	m.RLock()
	defer m.RUnlock()

	if len(m.nodeConditions) == 0 {
		return lifecycle.PodAdmitResult{Admit: true}
	}

	// Admit Critical pods even under resource pressure since they are required for system stability.
	// https://github.com/kubernetes/kubernetes/issues/40573 has more details.
	if kubelettypes.IsCriticalPod(attrs.Pod) {
		return lifecycle.PodAdmitResult{Admit: true}
	}

	// the node has memory pressure, admit if not best-effort
	if hasNodeCondition(m.nodeConditions, v1.NodeMemoryPressure) {
		notBestEffort := v1.PodQOSBestEffort != v1qos.GetPodQOS(attrs.Pod)

		if notBestEffort {
			return lifecycle.PodAdmitResult{Admit: true}
		}

		// When node has memory pressure and TaintNodesByCondition is enabled, check BestEffort Pod's toleration:
		// admit it if tolerates memory pressure taint, fail for other tolerations, e.g. DiskPressure.
		if utilfeature.DefaultFeatureGate.Enabled(features.TaintNodesByCondition) &&
			v1helper.TolerationsTolerateTaint(attrs.Pod.Spec.Tolerations, &v1.Taint{
				Key:    schedulerapi.TaintNodeMemoryPressure,
				Effect: v1.TaintEffectNoSchedule,
			}) {
			return lifecycle.PodAdmitResult{Admit: true}
		}
	}

	// reject pods when under memory pressure (if pod is best effort), or if under disk pressure.
	klog.Warningf("Failed to admit pod %s - node has conditions: %v", format.Pod(attrs.Pod), m.nodeConditions)

	return lifecycle.PodAdmitResult{
		Admit:   false,
		Reason:  Reason,
		Message: fmt.Sprintf(nodeConditionMessageFmt, m.nodeConditions),
	}
}

// Start starts the control loop to observe and response to low compute resources.
func (m *managerImpl) Start(diskInfoProvider DiskInfoProvider, podFunc ActivePodsFunc, podCleanedUpFunc PodCleanedUpFunc, monitoringInterval time.Duration) {
	thresholdHandler := func(message string) {
		klog.Infof(message)

		// 核心方法
		// 从本地磁盘上读取 Pod 的元数据（如 Pod 的状态、容器的状态等），并与 Kubernetes 的 API Server 进行比较和同步。
		// 如果在本地磁盘上存在未知的 Pod 或者存在与 Kubernetes API Server 上不一致的 Pod 信息，
		// 那么该方法会将本地的 Pod 信息同步到 API Server 上，以确保节点上运行的 Pod 的状态与集群状态保持一致
		// 1.diskInfoProvider 参数用于提供本地磁盘上存储 Pod 信息的路径
		// 2. podFunc 则是一个回调函数，用于处理 Pod 的信息，通常是将读取到的 Pod 信息进行解析和处理，并将其与 API Server 上的信息进行比较和同步。
		m.synchronize(diskInfoProvider, podFunc)
	}

	// 检查是否启用了 KernelMemcgNotification 配置项
	// KernelMemcgNotification 是一个 Kubernetes 集群的配置选项，它指定了 kubelet 是否将内存控制组 (memory cgroup) 的通知 (notification) 发送给内核。
	// 当这个配置项被启用时，kubelet 会将内存控制组的事件通知发送到内核，以便内核能够及时地响应这些事件。
	// 具体来说，当 Kubernetes 中的容器使用了太多的内存时，kubelet 就会向内核发送一个通知，告诉它容器已经超出了内存限制。
	// 内核可以通过这个通知来阻止容器进一步使用内存，以防止系统内存不足而导致系统崩溃。
	// 因此，如果 m.config.KernelMemcgNotification 配置项被启用，那么就意味着 kubelet 会将内存控制组的通知发送到内核。
	// 而如果这个配置项被禁用，那么 kubelet 就不会发送内存控制组的通知，而是只会记录相关的事件日志。
	if m.config.KernelMemcgNotification {
		for _, threshold := range m.config.Thresholds {

			// 判断内存阈值是否已经达到或者超过了预设的阈值，并且根据判断结果执行相应的操作。
			// 如果 threshold.Signal 的值等于 evictionapi.SignalMemoryAvailable 或者 evictionapi.SignalAllocatableMemoryAvailable
			// 那么就意味着系统中的可用内存或可用的可分配内存已经达到或者低于预设的阈值，需要执行相应的内存回收操作
			// 如果阈值已经超过，那么 Kubernetes 中的 kubelet 会通过 evictions 模块向 API 服务器发出请求
			// 请求将一些已经分配的 Pod 从 Node 节点上删除或迁移，以释放更多的内存资源。
			// 而如果阈值还没有达到，那么 kubelet 只会记录相应的事件日志，等待下一次的检查。
			if threshold.Signal == evictionapi.SignalMemoryAvailable || threshold.Signal == evictionapi.SignalAllocatableMemoryAvailable {
				notifier, err := NewMemoryThresholdNotifier(threshold, m.config.PodCgroupRoot, &CgroupNotifierFactory{}, thresholdHandler)
				if err != nil {
					klog.Warningf("eviction manager: failed to create memory threshold notifier: %v", err)
				} else {
					// 启动事件内存检测
					go notifier.Start()

					m.thresholdNotifiers = append(m.thresholdNotifiers, notifier)
				}
			}
		}
	}
	// start the eviction manager monitoring
	go func() {
		for {
			if evictedPods := m.synchronize(diskInfoProvider, podFunc); evictedPods != nil {
				klog.Infof("eviction manager: pods %s evicted, waiting for pod to be cleaned up", format.Pods(evictedPods))

				// 阻塞等待驱逐的 pods 都被清理.
				m.waitForPodsCleanup(podCleanedUpFunc, evictedPods)
			} else {

				// 每 10 秒进行一次 synchronize 同步.
				time.Sleep(monitoringInterval)
			}
		}
	}()
}

// IsUnderMemoryPressure returns true if the node is under memory pressure.
func (m *managerImpl) IsUnderMemoryPressure() bool {
	m.RLock()
	defer m.RUnlock()
	return hasNodeCondition(m.nodeConditions, v1.NodeMemoryPressure)
}

// IsUnderDiskPressure returns true if the node is under disk pressure.
func (m *managerImpl) IsUnderDiskPressure() bool {
	m.RLock()
	defer m.RUnlock()
	return hasNodeCondition(m.nodeConditions, v1.NodeDiskPressure)
}

// IsUnderPIDPressure returns true if the node is under PID pressure.
func (m *managerImpl) IsUnderPIDPressure() bool {
	m.RLock()
	defer m.RUnlock()
	return hasNodeCondition(m.nodeConditions, v1.NodePIDPressure)
}

// synchronize is the main control loop that enforces eviction thresholds.
// Returns the pod that was killed, or nil if no pod was killed.
func (m *managerImpl) synchronize(diskInfoProvider DiskInfoProvider, podFunc ActivePodsFunc) []*v1.Pod {
	// if we have nothing to do, just return
	thresholds := m.config.Thresholds
	if len(thresholds) == 0 && !utilfeature.DefaultFeatureGate.Enabled(features.LocalStorageCapacityIsolation) {
		return nil
	}

	klog.V(3).Infof("eviction manager: synchronize housekeeping")

	// build the ranking functions (if not yet known)
	// TODO: have a function in cadvisor that lets us know if global housekeeping has completed
	// 检查是否存在一个特定的文件系统对象来管理容器镜像。
	// 如果指针指向nil，则表示没有指定特定的文件系统对象用于管理容器镜像，这意味着Kubernetes将使用默认的文件系统对象来管理容器镜像。
	// 否则，Kubernetes将使用m.dedicatedImageFs指向的文件系统对象来管理容器镜像。
	if m.dedicatedImageFs == nil {
		hasImageFs, ok := diskInfoProvider.HasDedicatedImageFs()
		if ok != nil {
			return nil
		}

		m.dedicatedImageFs = &hasImageFs

		m.signalToRankFunc = buildSignalToRankFunc(hasImageFs)

		// m.imageGC：一个用于回收未使用的镜像的 GC（Garbage Collector）对象。
		// m.containerGC：一个用于回收未使用的容器的 GC 对象。
		// hasImageFs：一个布尔值，用于指示当前节点是否具有 Image 文件系统（即 容器 镜像所在的文件系统）。
		// 根据上面三个参数，buildSignalToNodeReclaimFuncs() 函数会返回一个函数列表，这个函数列表中的每个函数都用于回收节点资源。
		// 具体来说，这些函数会向节点发送一个信号，通知其回收未使用的镜像和容器。
		// 如果节点具有 Image 文件系统，则这些函数还会向节点发送一个信号，通知其回收未使用的 Image 文件系统空间。
		m.signalToNodeReclaimFuncs = buildSignalToNodeReclaimFuncs(m.imageGC, m.containerGC, hasImageFs)
	}

	activePods := podFunc()

	updateStats := true

	summary, err := m.summaryProvider.Get(updateStats)
	if err != nil {
		klog.Errorf("eviction manager: failed to get summary stats: %v", err)
		return nil
	}

	// 比较当前时间与上次更新告警阈值的时间之间的时间差是否超过了一个固定的时间间隔 notifierRefreshInterval。
	// 如果时间差超过了这个时间间隔，那么就需要更新告警阈值。
	if m.clock.Since(m.thresholdsLastUpdated) > notifierRefreshInterval {
		m.thresholdsLastUpdated = m.clock.Now()
		for _, notifier := range m.thresholdNotifiers {
			if err := notifier.UpdateThreshold(summary); err != nil {
				klog.Warningf("eviction manager: failed to update %s: %v", notifier.Description(), err)
			}
		}
	}

	// make observations and get a function to derive pod usage stats relative to those observations.
	// 根据summary信息创建相应的统计信息到observations对象中，如SignalMemoryAvailable、SignalNodeFsAvailable等。
	observations, statsFunc := makeSignalObservations(summary)

	debugLogObservations("observations", observations)

	// determine the set of thresholds met independent of grace period
	// 用于计算当前节点的告警阈值是否已经满足
	// thresholds 表示当前节点的告警阈值，而 observations 则表示关于信号发送情况的观察结果。
	// thresholdsMet() 函数会根据这些参数，计算当前节点的告警阈值是否已经满足。
	// 如果已经满足，那么函数会返回一个更新后的告警阈值，否则返回原始的告警阈值。
	// thresholds 被赋值为 thresholdsMet(thresholds, observations, false) 的返回值，表示更新后的告警阈值。
	// 这个操作的目的是确保当前节点的告警阈值是最新的，并且已经满足了当前的资源使用情况。
	// 第三个参数 false 表示该操作并不会触发告警。如果设置为 true，那么函数会在更新告警阈值时触发告警。这个参数的设置通常是由调用方根据具体情况来确定的。
	thresholds = thresholdsMet(thresholds, observations, false)

	debugLogThresholdsWithObservation("thresholds - ignoring grace period", thresholds, observations)

	// determine the set of thresholds previously met that have not yet satisfied the associated min-reclaim
	if len(m.thresholdsMet) > 0 {
		thresholdsNotYetResolved := thresholdsMet(m.thresholdsMet, observations, true)
		thresholds = mergeThresholds(thresholds, thresholdsNotYetResolved)
	}

	debugLogThresholdsWithObservation("thresholds - reclaim not satisfied", thresholds, observations)

	// track when a threshold was first observed
	now := m.clock.Now()

	// 用来记录 eviction signal 第一次的时间，没有则设置 now 时间
	thresholdsFirstObservedAt := thresholdsFirstObservedAt(thresholds, m.thresholdsFirstObservedAt, now)

	// the set of node conditions that are triggered by currently observed thresholds
	// Kubelet会将对应的Eviction Signals映射到对应的Node Conditions
	nodeConditions := nodeConditions(thresholds)
	if len(nodeConditions) > 0 {
		klog.V(3).Infof("eviction manager: node conditions - observed: %v", nodeConditions)
	}

	// track when a node condition was last observed
	// 本轮 node condition 与上次的observed合并，以最新的为准
	nodeConditionsLastObservedAt := nodeConditionsLastObservedAt(nodeConditions, m.nodeConditionsLastObservedAt, now)

	// node conditions report true if it has been observed within the transition period window
	nodeConditions = nodeConditionsObservedSince(nodeConditionsLastObservedAt, m.config.PressureTransitionPeriod, now)
	if len(nodeConditions) > 0 {
		klog.V(3).Infof("eviction manager: node conditions - transition period not met: %v", nodeConditions)
	}

	// determine the set of thresholds we need to drive eviction behavior (i.e. all grace periods are met)
	thresholds = thresholdsMetGracePeriod(thresholdsFirstObservedAt, now)
	debugLogThresholdsWithObservation("thresholds - grace periods satisified", thresholds, observations)

	// update internal state
	m.Lock()
	m.nodeConditions = nodeConditions
	m.thresholdsFirstObservedAt = thresholdsFirstObservedAt
	m.nodeConditionsLastObservedAt = nodeConditionsLastObservedAt
	m.thresholdsMet = thresholds

	// determine the set of thresholds whose stats have been updated since the last sync
	thresholds = thresholdsUpdatedStats(thresholds, observations, m.lastObservations)
	debugLogThresholdsWithObservation("thresholds - updated stats", thresholds, observations)

	m.lastObservations = observations
	m.Unlock()

	// evict pods if there is a resource usage violation from local volume temporary storage
	// If eviction happens in localStorageEviction function, skip the rest of eviction action
	if utilfeature.DefaultFeatureGate.Enabled(features.LocalStorageCapacityIsolation) {
		if evictedPods := m.localStorageEviction(summary, activePods); len(evictedPods) > 0 {
			return evictedPods
		}
	}

	if len(thresholds) == 0 {
		klog.V(3).Infof("eviction manager: no resources are starved")
		return nil
	}

	// rank the thresholds by eviction priority
	sort.Sort(byEvictionPriority(thresholds))

	thresholdToReclaim, resourceToReclaim, foundAny := getReclaimableThreshold(thresholds)
	if !foundAny {
		return nil
	}

	klog.Warningf("eviction manager: attempting to reclaim %v", resourceToReclaim)

	// record an event about the resources we are now attempting to reclaim via eviction
	m.recorder.Eventf(m.nodeRef, v1.EventTypeWarning, "EvictionThresholdMet", "Attempting to reclaim %s", resourceToReclaim)

	// check if there are node-level resources we can reclaim to reduce pressure before evicting end-user pods.
	if m.reclaimNodeLevelResources(thresholdToReclaim.Signal, resourceToReclaim) {
		klog.Infof("eviction manager: able to reduce %v pressure without evicting pods.", resourceToReclaim)
		return nil
	}

	klog.Infof("eviction manager: must evict pod(s) to reclaim %v", resourceToReclaim)

	// rank the pods for eviction
	rank, ok := m.signalToRankFunc[thresholdToReclaim.Signal]
	if !ok {
		klog.Errorf("eviction manager: no ranking function for signal %s", thresholdToReclaim.Signal)
		return nil
	}

	// the only candidates viable for eviction are those pods that had anything running.
	if len(activePods) == 0 {
		klog.Errorf("eviction manager: eviction thresholds have been met, but no pods are active to evict")
		return nil
	}

	// rank the running pods for eviction for the specified resource
	rank(activePods, statsFunc)

	klog.Infof("eviction manager: pods ranked for eviction: %s", format.Pods(activePods))

	//record age of metrics for met thresholds that we are using for evictions.
	for _, t := range thresholds {
		timeObserved := observations[t.Signal].time
		if !timeObserved.IsZero() {
			metrics.EvictionStatsAge.WithLabelValues(string(t.Signal)).Observe(metrics.SinceInSeconds(timeObserved.Time))
			metrics.DeprecatedEvictionStatsAge.WithLabelValues(string(t.Signal)).Observe(metrics.SinceInMicroseconds(timeObserved.Time))
		}
	}

	// we kill at most a single pod during each eviction interval
	for i := range activePods {
		pod := activePods[i]

		gracePeriodOverride := int64(0)

		if !isHardEvictionThreshold(thresholdToReclaim) {
			gracePeriodOverride = m.config.MaxPodGracePeriodSeconds
		}

		message, annotations := evictionMessage(resourceToReclaim, pod, statsFunc)

		if m.evictPod(pod, gracePeriodOverride, message, annotations) {
			metrics.Evictions.WithLabelValues(string(thresholdToReclaim.Signal)).Inc()

			return []*v1.Pod{pod}
		}
	}

	klog.Infof("eviction manager: unable to evict any pods from the node")

	return nil
}

func (m *managerImpl) waitForPodsCleanup(podCleanedUpFunc PodCleanedUpFunc, pods []*v1.Pod) {
	timeout := m.clock.NewTimer(podCleanupTimeout)
	defer timeout.Stop()
	ticker := m.clock.NewTicker(podCleanupPollFreq)
	defer ticker.Stop()
	for {
		select {
		case <-timeout.C():
			klog.Warningf("eviction manager: timed out waiting for pods %s to be cleaned up", format.Pods(pods))
			return
		case <-ticker.C():
			for i, pod := range pods {
				if !podCleanedUpFunc(pod) {
					break
				}
				if i == len(pods)-1 {
					klog.Infof("eviction manager: pods %s successfully cleaned up", format.Pods(pods))
					return
				}
			}
		}
	}
}

// reclaimNodeLevelResources attempts to reclaim node level resources.  returns true if thresholds were satisfied and no pod eviction is required.
func (m *managerImpl) reclaimNodeLevelResources(signalToReclaim evictionapi.Signal, resourceToReclaim v1.ResourceName) bool {
	nodeReclaimFuncs := m.signalToNodeReclaimFuncs[signalToReclaim]
	for _, nodeReclaimFunc := range nodeReclaimFuncs {
		// attempt to reclaim the pressured resource.
		if err := nodeReclaimFunc(); err != nil {
			klog.Warningf("eviction manager: unexpected error when attempting to reduce %v pressure: %v", resourceToReclaim, err)
		}

	}
	if len(nodeReclaimFuncs) > 0 {
		summary, err := m.summaryProvider.Get(true)
		if err != nil {
			klog.Errorf("eviction manager: failed to get summary stats after resource reclaim: %v", err)
			return false
		}

		// make observations and get a function to derive pod usage stats relative to those observations.
		observations, _ := makeSignalObservations(summary)
		debugLogObservations("observations after resource reclaim", observations)

		// determine the set of thresholds met independent of grace period
		thresholds := thresholdsMet(m.config.Thresholds, observations, false)
		debugLogThresholdsWithObservation("thresholds after resource reclaim - ignoring grace period", thresholds, observations)

		if len(thresholds) == 0 {
			return true
		}
	}
	return false
}

// localStorageEviction checks the EmptyDir volume usage for each pod and determine whether it exceeds the specified limit and needs
// to be evicted. It also checks every container in the pod, if the container overlay usage exceeds the limit, the pod will be evicted too.
func (m *managerImpl) localStorageEviction(summary *statsapi.Summary, pods []*v1.Pod) []*v1.Pod {
	statsFunc := cachedStatsFunc(summary.Pods)
	evicted := []*v1.Pod{}
	for _, pod := range pods {
		podStats, ok := statsFunc(pod)
		if !ok {
			continue
		}

		if m.emptyDirLimitEviction(podStats, pod) {
			evicted = append(evicted, pod)
			continue
		}

		if m.podEphemeralStorageLimitEviction(podStats, pod) {
			evicted = append(evicted, pod)
			continue
		}

		if m.containerEphemeralStorageLimitEviction(podStats, pod) {
			evicted = append(evicted, pod)
		}
	}

	return evicted
}

func (m *managerImpl) emptyDirLimitEviction(podStats statsapi.PodStats, pod *v1.Pod) bool {
	podVolumeUsed := make(map[string]*resource.Quantity)
	for _, volume := range podStats.VolumeStats {
		podVolumeUsed[volume.Name] = resource.NewQuantity(int64(*volume.UsedBytes), resource.BinarySI)
	}
	for i := range pod.Spec.Volumes {
		source := &pod.Spec.Volumes[i].VolumeSource
		if source.EmptyDir != nil {
			size := source.EmptyDir.SizeLimit
			used := podVolumeUsed[pod.Spec.Volumes[i].Name]
			if used != nil && size != nil && size.Sign() == 1 && used.Cmp(*size) > 0 {
				// the emptyDir usage exceeds the size limit, evict the pod
				return m.evictPod(pod, 0, fmt.Sprintf(emptyDirMessageFmt, pod.Spec.Volumes[i].Name, size.String()), nil)
			}
		}
	}

	return false
}

func (m *managerImpl) podEphemeralStorageLimitEviction(podStats statsapi.PodStats, pod *v1.Pod) bool {
	_, podLimits := apiv1resource.PodRequestsAndLimits(pod)
	_, found := podLimits[v1.ResourceEphemeralStorage]
	if !found {
		return false
	}

	podEphemeralStorageTotalUsage := &resource.Quantity{}
	var fsStatsSet []fsStatsType
	if *m.dedicatedImageFs {
		fsStatsSet = []fsStatsType{fsStatsLogs, fsStatsLocalVolumeSource}
	} else {
		fsStatsSet = []fsStatsType{fsStatsRoot, fsStatsLogs, fsStatsLocalVolumeSource}
	}
	podEphemeralUsage, err := podLocalEphemeralStorageUsage(podStats, pod, fsStatsSet, m.etcHostsPath(pod.UID))
	if err != nil {
		klog.Errorf("eviction manager: error getting pod disk usage %v", err)
		return false
	}

	podEphemeralStorageTotalUsage.Add(podEphemeralUsage[v1.ResourceEphemeralStorage])
	podEphemeralStorageLimit := podLimits[v1.ResourceEphemeralStorage]
	if podEphemeralStorageTotalUsage.Cmp(podEphemeralStorageLimit) > 0 {
		// the total usage of pod exceeds the total size limit of containers, evict the pod
		return m.evictPod(pod, 0, fmt.Sprintf(podEphemeralStorageMessageFmt, podEphemeralStorageLimit.String()), nil)
	}
	return false
}

func (m *managerImpl) containerEphemeralStorageLimitEviction(podStats statsapi.PodStats, pod *v1.Pod) bool {
	thresholdsMap := make(map[string]*resource.Quantity)
	for _, container := range pod.Spec.Containers {
		ephemeralLimit := container.Resources.Limits.StorageEphemeral()
		if ephemeralLimit != nil && ephemeralLimit.Value() != 0 {
			thresholdsMap[container.Name] = ephemeralLimit
		}
	}

	for _, containerStat := range podStats.Containers {
		containerUsed := diskUsage(containerStat.Logs)
		if !*m.dedicatedImageFs {
			containerUsed.Add(*diskUsage(containerStat.Rootfs))
		}

		if ephemeralStorageThreshold, ok := thresholdsMap[containerStat.Name]; ok {
			if ephemeralStorageThreshold.Cmp(*containerUsed) < 0 {
				return m.evictPod(pod, 0, fmt.Sprintf(containerEphemeralStorageMessageFmt, containerStat.Name, ephemeralStorageThreshold.String()), nil)

			}
		}
	}
	return false
}

func (m *managerImpl) evictPod(pod *v1.Pod, gracePeriodOverride int64, evictMsg string, annotations map[string]string) bool {
	// If the pod is marked as critical and static, and support for critical pod annotations is enabled,
	// do not evict such pods. Static pods are not re-admitted after evictions.
	// https://github.com/kubernetes/kubernetes/issues/40573 has more details.
	if kubelettypes.IsStaticPod(pod) {
		// need mirrorPod to check its "priority" value; static pod doesn't carry it
		if mirrorPod, ok := m.mirrorPodFunc(pod); ok && mirrorPod != nil {
			// skip only when it's a static and critical pod
			if kubelettypes.IsCriticalPod(mirrorPod) {
				klog.Errorf("eviction manager: cannot evict a critical static pod %s", format.Pod(pod))
				return false
			}
		} else {
			// we should never hit this
			klog.Errorf("eviction manager: cannot get mirror pod from static pod %s, so cannot evict it", format.Pod(pod))
			return false
		}
	}

	status := v1.PodStatus{
		Phase:   v1.PodFailed,
		Message: evictMsg,
		Reason:  Reason,
	}

	// record that we are evicting the pod
	m.recorder.AnnotatedEventf(pod, annotations, v1.EventTypeWarning, Reason, evictMsg)

	// this is a blocking call and should only return when the pod and its containers are killed.
	err := m.killPodFunc(pod, status, &gracePeriodOverride)
	if err != nil {
		klog.Errorf("eviction manager: pod %s failed to evict %v", format.Pod(pod), err)
	} else {
		klog.Infof("eviction manager: pod %s is evicted successfully", format.Pod(pod))
	}

	return true
}
