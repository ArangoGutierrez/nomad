package catalog

import (
	"github.com/hashicorp/nomad/drivers/rawexec"
	"github.com/hashicorp/nomad/drivers/singularity"
)

// This file is where all builtin plugins should be registered in the catalog.
// Plugins with build restrictions should be placed in the appropriate
// register_XXX.go file.
func init() {
	RegisterDeferredConfig(rawexec.PluginID, rawexec.PluginConfig, rawexec.PluginLoader)
	RegisterDeferredConfig(singularity.PluginID, singularity.PluginConfig, singularity.PluginLoader)
}
