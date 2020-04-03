package collect

import (
	"context"

	"github.com/newrelic/nri-vmware-vsphere/internal/load"
	"github.com/vmware/govmomi/vim25/mo"
)

// VirtualMachines vms
func VirtualMachines(config *load.Config) {
	ctx := context.Background()
	m := config.ViewManager

	for i, dc := range config.Datacenters {
		cv, err := m.CreateContainerView(ctx, dc.Datacenter.Reference(), []string{"VirtualMachine"}, true)
		if err != nil {
			config.Logrus.WithError(err).Fatal("failed to create VirtualMachine container view")
		}
		defer cv.Destroy(ctx)

		var vms []mo.VirtualMachine
		// Reference: http://pubs.vmware.com/vsphere-60/topic/com.vmware.wssdk.apiref.doc/vim.VirtualMachine.html
		err = cv.Retrieve(ctx, []string{"VirtualMachine"}, []string{"summary", "network", "config", "guest", "runtime", "resourcePool"}, &vms)
		if err != nil {
			config.Logrus.WithError(err).Fatal("failed to retrieve VM Summaries")
		}
		for j := 0; j < len(vms); j++ {
			config.Datacenters[i].VirtualMachines[vms[j].Self] = &vms[j]
		}
	}
}
