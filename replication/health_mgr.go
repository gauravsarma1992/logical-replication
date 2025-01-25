package replication

import (
	"context"
	"errors"
	"time"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
)

var (
	HighCPUUsageError    = errors.New("error: high CPU usage")
	HighMemoryUsageError = errors.New("error: high memory usage")
	HighDiskUsageError   = errors.New("error: high disk usage")
)

type (
	HealthManager struct {
		ctx          context.Context
		replMgr      *ReplicationManager
		health       *NodeHealth
		healthLimits NodeHealthLimits

		lastUpdatedState NodeStateT
	}

	NodeHealth struct {
		CPUUsage     float64
		MemoryUsage  float64
		StorageUsage float64
	}
	NodeHealthLimits struct {
		CPUUsage     float64
		MemoryUsage  float64
		StorageUsage float64
	}
)

func DefaultNodeHealthLimits() (limits NodeHealthLimits) {
	limits = NodeHealthLimits{
		CPUUsage:     80.0,
		MemoryUsage:  80.0,
		StorageUsage: 80.0,
	}
	return
}

func NewHealthManager(ctx context.Context) (healthMgr *HealthManager, err error) {
	healthMgr = &HealthManager{
		ctx:              ctx,
		health:           &NodeHealth{},
		healthLimits:     DefaultNodeHealthLimits(),
		lastUpdatedState: NodeStateUnknown,
	}
	healthMgr.replMgr = ctx.Value(ReplicationManagerInContext).(*ReplicationManager)
	return
}

func (healthMgr *HealthManager) updateHealth() (health *NodeHealth, err error) {
	health = &NodeHealth{}
	// Check CPU usage for the last 5 seconds
	cpuUsages, err := cpu.Percent(5*time.Second, false)
	if err != nil {
		return
	}
	if len(cpuUsages) > 0 {
		health.CPUUsage = cpuUsages[0]
	}

	// Check memory usage
	vmStat, err := mem.VirtualMemory()
	if err != nil {
		return
	}
	health.MemoryUsage = vmStat.UsedPercent

	// Check storage usage
	diskStat, err := disk.Usage("/")
	if err != nil {
		return
	}
	health.StorageUsage = diskStat.UsedPercent
	return
}

func (healthMgr *HealthManager) compareWithLimits() (err error) {
	if healthMgr.health.CPUUsage > healthMgr.healthLimits.CPUUsage {
		err = HighCPUUsageError
		return
	}
	if healthMgr.health.MemoryUsage > healthMgr.healthLimits.MemoryUsage {
		err = HighMemoryUsageError
		return
	}
	if healthMgr.health.StorageUsage > healthMgr.healthLimits.StorageUsage {
		err = HighDiskUsageError
		return
	}
	return
}

func (healthMgr *HealthManager) CheckHealth() (err error) {
	var (
		checkedState NodeStateT
	)
	if healthMgr.health, err = healthMgr.updateHealth(); err != nil {
		return
	}
	if err = healthMgr.compareWithLimits(); err != nil {
		healthMgr.replMgr.log.Println("error while checking health", err)
		checkedState = NodeStateUnhealthy
	} else {
		checkedState = NodeStateHealthy
	}
	if checkedState != healthMgr.lastUpdatedState {
		healthMgr.replMgr.log.Println("Node state changed to", checkedState)
		healthMgr.replMgr.localNode.UpdateState(checkedState)
		healthMgr.lastUpdatedState = checkedState
	}

	return
}

func (healthMgr *HealthManager) Start() (err error) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-healthMgr.ctx.Done():
			return
		case <-ticker.C:
			if err = healthMgr.CheckHealth(); err != nil {
				return
			}
		}
	}
	return
}
