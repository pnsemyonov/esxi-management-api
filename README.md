### API for deployment and maintenance ESXi infrastructure

Used tools
- _Python_
- _VMware vSphere Web Services SDK_

Given API has been developed with the aim of providing convenient way of deployment of _ESXi_ infrastructure on all-flash 
array storage system and performing common operations on it. This API allows to perform following operations:
- _VMFS_ datastore creation/modifying
- Virtual machines creation/cloning
- VM disks creation/modifying
- VM operations etc.

Using this API, one can easy simulate bulk VM operations such as:
- Mass deployment of VMs
- Performing I/O on virtual disks
- Boot storm simulation
- Mass power off etc.

This gives ability to simulate real-life activity of _ESXi_ infrastructure with emphasize on load/performance testing.