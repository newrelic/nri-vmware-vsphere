// Copyright 2020 New Relic Corporation. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package load

import (
	"github.com/vmware/govmomi/vim25/mo"
	"github.com/vmware/govmomi/vim25/types"
)

type mor = types.ManagedObjectReference

// Datacenter struct
type Datacenter struct {
	Datacenter      *mo.Datacenter
	Hosts           map[mor]*mo.HostSystem
	Clusters        map[mor]*mo.ClusterComputeResource
	ResourcePools   map[mor]*mo.ResourcePool
	Datastores      map[mor]*mo.Datastore
	Networks        map[mor]*mo.Network
	VirtualMachines map[mor]*mo.VirtualMachine
}

// NewDatacenter Initialize datacenter struct
func NewDatacenter(datacenter *mo.Datacenter) Datacenter {
	return Datacenter{
		Datacenter:      datacenter,
		Hosts:           make(map[mor]*mo.HostSystem),
		Clusters:        make(map[mor]*mo.ClusterComputeResource),
		ResourcePools:   make(map[mor]*mo.ResourcePool),
		Datastores:      make(map[mor]*mo.Datastore),
		Networks:        make(map[mor]*mo.Network),
		VirtualMachines: make(map[mor]*mo.VirtualMachine),
	}
}

// FindResourcePool finds the ResourcePool associated to a Cluster except for the default resource pool
func (dc *Datacenter) FindResourcePool(clusterReference mor) (rp []*mo.ResourcePool) {
	for _, resourcePool := range dc.ResourcePools {
		// Default ResourcePool is the root, the rest should be listed as child
		if (resourcePool.Owner == clusterReference) && (len(resourcePool.ResourcePool) > 0) {
			for _, rpChild := range resourcePool.ResourcePool {
				rp = append(rp, dc.ResourcePools[rpChild])
			}
		}
	}
	return
}

// FindHost returns the child Host for a computeResource
func (dc *Datacenter) FindHost(computeResourceReference mor) *mo.HostSystem {
	for _, host := range dc.Hosts {
		if host.Parent.Reference() == computeResourceReference {
			return host
		}
	}
	return nil
}

// GetResourcePoolName returns the name of the Resource Pool if is not the default
func (dc *Datacenter) GetResourcePoolName(resourcePoolReference mor) string {
	if !dc.IsDefaultResourcePool(resourcePoolReference) {
		return dc.ResourcePools[resourcePoolReference].Name
	}
	return ""
}

// IsDefaultResourcePool returns true if the resource pool is the default
func (dc *Datacenter) IsDefaultResourcePool(resourcePoolReference mor) bool {
	if rp, ok := dc.ResourcePools[resourcePoolReference]; ok {
		if rp.Parent.Type != "ResourcePool" {
			return true
		}
	}
	return false
}
