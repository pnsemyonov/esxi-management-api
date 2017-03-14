#!/usr/bin/env python

# Lib import


PYVMOMI_INSTALL_MSG = '''
pyVmomi libraries are missing. Please follow below instructions to install:
    sudo apt-get install python-pip
    pip install pyvmomi
'''

try:
    # pyVmomi: vSphere API
    from pyVim import connect
    from pyVmomi import vim
except ImportError:
    LOG.error(PYVMOMI_INSTALL_MSG)
    sys.exit(0)


class CreateFSException(Exception):
    pass


class RescanDeviceException(Exception):
    pass


class ESXHost(LinuxHost):
    """
    Module to manage ESX hosts
    Abbreviations:
        ds(s): Datastore(s)
        vm(s): Virtual Machine(s)
        vSC: vSphere Client GUI
    """
    def __init__(self, hostname, username='root', password='netapp1!'):
        super(ESXHost, self).__init__(hostname=hostname, username=username, password=password)
        self.host = hostname
        self.hosttype = 'esx'
        self.user = username
        self.password = password
        self._connect()

    def _connect(self):
        """
            Establish connection to ESXi host
        """
        requests.packages.urllib3.disable_warnings()
        try:
            # pyVmomi: vSphere API
            from pyVim import connect
        except ImportError:
            LOG.error(PYVMOMI_INSTALL_MSG)
            sys.exit(0)

        self.serviceInstance = connect.Connect(host=self.host, user=self.user, pwd=self.password)
        atexit.register(connect.Disconnect, self.serviceInstance)
        self.serviceContent = self.serviceInstance.RetrieveServiceContent()

        self.datacenter = self.serviceContent.rootFolder.childEntity[0]
        self.hostSystem = self.datacenter.hostFolder.childEntity[0].host[0]
        self.configManager = self.hostSystem.configManager
        self.datastoreSystem = self.configManager.datastoreSystem
        self.storageSystem = self.configManager.storageSystem

    def _satisfyFilters(self, attributes, filterAttrs):
        """
            @param attributeCondition: Determines how attributes are compared - AND/OR. Default is OR.
            @param valuesCondition: Determines how list values are compared - AND/OR. Default is OR.
            @param matchPattern: If Boolean True, does regex comparison. Default is False.
                Applicable only for attributes and not for values list.
        """
        filterAttrs = copy.deepcopy(filterAttrs)
        LOG.l5('Input Attributes: ', attributes)
        LOG.l5('Input Filters: ', filterAttrs)
        if not len(filterAttrs.keys()):
            return True
        attributeCondition = filterAttrs.get('attributeCondition', 'OR').upper()
        if 'attributeCondition' in filterAttrs:
            del filterAttrs['attributeCondition']
        valuesCondition = filterAttrs.get('valuesCondition', 'OR').upper()
        if 'valuesCondition' in filterAttrs:
            del filterAttrs['valuesCondition']
        matchPattern = filterAttrs.get('matchPattern', False)
        if 'matchPattern' in filterAttrs:
            del filterAttrs['matchPattern']
        attrCount = 0
        for fAttr in filterAttrs.keys():
            if fAttr in attributes:
                if isinstance(filterAttrs[fAttr], int) or isinstance(filterAttrs[fAttr], str):
                    if matchPattern:
                        if len(re.findall(str(filterAttrs[fAttr]), str(attributes[fAttr]))):
                            if attributeCondition == 'OR':
                                return True
                            else:
                                attrCount += 1
                    else:
                        if str(attributes[fAttr]) == str(filterAttrs[fAttr]):
                            if attributeCondition == 'OR':
                                return True
                            else:
                                attrCount += 1
                # If filterAttrs key is list
                elif isinstance(filterAttrs[fAttr], list):
                    valCount = 0
                    valFound = False
                    for value in filterAttrs[fAttr]:
                        if value in attributes[fAttr]:
                            if valuesCondition == 'OR':
                                valFound = True
                                break
                            else:
                                valCount += 1
                    if attributeCondition == 'OR':
                        if valuesCondition == 'OR' and valFound:
                            return True
                        if valuesCondition == 'AND' and valCount == len(filterAttrs[fAttr]):
                            attrCount += 1
                    if attributeCondition == 'AND':
                        if valuesCondition == 'OR' and valFound:
                            attrCount += 1
                        elif valuesCondition == 'AND' and valCount == len(filterAttrs[fAttr]):
                            attrCount += 1
        if attributeCondition == 'AND' and attrCount == len(filterAttrs):
            return True
        if attributeCondition == 'OR' and attrCount > 0:
            return True
        return False

    def getVersion(self):
        result = self.cmd('vmware -v')
        return result.strip()

    def getPID(self, process):
        return self.cmd("""ps | grep "%s" | grep -v "grep" | awk '{print $1}'""" % process, timeout=20).strip()

    def killProcess(self, processname, signal="SIGKILL"):
        res = self.cmd('pkill %s %s 2>&1' % (signal, processname))
        assert len(res) == 0, "process kill failed: %s" % (res)
        return True

    def killPID(self, pid, signal='9'):
        res = self.cmd('kill -%s %s 2>&1' % (str(signal), str(pid)), timeout=20)
        assert len(res) == 0, "process kill failed: %s" % (res)
        return True

    def getIPs(self):
        return [{'device': pnic.device, 'mac': pnic.mac, 'ipAddress': pnic.spec.ip.ip.Address} for pnic in
        self.configManager.networkSystem.networkInfo.pnic]

    def waitForLogin(self, timeout=600):
        """
            Wait up to timeout seconds for the system to come up
            Raises LinuxhostException if timeout is exceeded
        """
        t = Timeout(timeout)
        while True:
            t.exceeded()
            try:
                self.cmd('uptime', timeout=2)
                return
            except:
                pass
            time.sleep(2)

    def reboot(self, force=False, wait=False, timeout=1200):
        """
            Reboot ESX host
            @param force: If true, the host is rebooted even being in maintenance mode or running VMs
            @param wait: If True, wait for reboot completion
            @param timeout: Time to wait for resolution until throwing error
            @return: True if wait == True and reboot has been completed, otherwise None
        """
        if self.hostSystem.capability.rebootSupported:
            self.hostSystem.RebootHost_Task(force=force)
        else:
            raise ConfigException('Reboot not supported by the host.')

        LOG.info('ESX host rebooting...')
        try:
            while self.serviceInstance.CurrentTime():
                time.sleep(3)
        except BadStatusLine:
            LOG.info('ESX host shut down.')

        # After it goes down, then if wait == True, waitForLogin for given timeout
        if wait:
            time.sleep(5)
            LOG.info('Waiting for reboot to complete...')
            self.waitForLogin()
            LOG.info('ESX host reboot completed.')

            return True

    def rescan(self):
        """
            Rescan for adapter changes
        """
        LOG.info('Running rescan of HBAs')
        self.storageSystem.RescanAllHba()

    def _getTimestampID(self):
        """
            Returns timestamp in hex format as unique ID in mass operations.
        """
        return hex(int(time.time() * 1000000)).upper()[9:]

    def getFCInitiators(self, matchWWPN=[]):
        """
        Return FC initiators
            @param matchWWPN: List of requested WWPN numbers (optional)
                If provided, then only those FC Initiators that match WWPN are returned
            @return: List of dictionaries for FC initiators info
        """
        try:
            # pyVmomi: vSphere API
            from pyVmomi import vim
        except ImportError:
            LOG.error(PYVMOMI_INSTALL_MSG)
            sys.exit(0)

        hostBusAdapters = []
        busAdapters = self.storageSystem.storageDeviceInfo.hostBusAdapter
        for busAdapter in busAdapters:
            if not isinstance(busAdapter, vim.HostFibreChannelHba):
                continue

            # WWPN is stored as long (ex. 2377900762154377710L), so convert it into hex string
            wwpnHex = format(busAdapter.portWorldWideName, 'x')
            wwpn = ':'.join(re.findall(r'..', wwpnHex))

            if not matchWWPN or wwpn in matchWWPN:
                adapterInfo = {
                    'WWPN': wwpn,

                    # Ex. 'vmhba1'. vSC: Configuration/Hardware/Storage Adapters
                    'adapter_name': busAdapter.device,

                    # Ex. 2305843168116449774L -> '20000024ff57a1ee' -> '20:00:00:24:ff:57:a1:ee'
                    'WWNN': ':'.join(re.findall(r'..', format(busAdapter.nodeWorldWideName, 'x'))),

                    # Ex. 'qla2xxx'
                    'driver_name': busAdapter.driver,

                    # Ex. 'ISP2532-based 8Gb Fibre Channel to PCI Express HBA'
                    'model': busAdapter.model,
                    'port_type': busAdapter.portType,

                    # Ex. 4
                    'bus': busAdapter.bus,

                    # Ex. string '04:00.1'
                    'pci': busAdapter.pci,
                    'speed': busAdapter.speed,

                    # Ex. 'online'
                    'status': busAdapter.status,
                }
                hostBusAdapters.append(adapterInfo)

        LOG.l4('Obtained list of FC initiators.')

        return hostBusAdapters

    def getDevices(self, node=None, uniqLuns=True, luns=[], groupBySize=False, available=False, **filterAttrs):
        """
            Return LUN devices mounted on host
            @param node: Mars node object.  Default: None
              If node object is not provided, then 'lun-pathname' attribute is set to canonical name
            @param uniqLuns (boolean): Returns unique LUNs when multipath is configured
              Default: True
            @param luns (list of LUN path names): Return devices only for specified LUN path names ('lun-pathname')
              Default: All LUNs
            @param groupBySize: If true, returns dictionary of luns grouped by size
                Default: False
            @param available: Return only those disks that can be used to contain VMFS datastore extents.
            @param filterAttrs: Additional filtering by elements of resulting structure.
        """
        self.rescan()

        # List of pairs {serial: lun-pathname} (ex. [{'8006GF1K3-XI': 'LUN_000', '8006GF1K3-XM': 'LUN_004'}])
        marsLuns = None
        if node is not None:
            marsLuns = {info['Serial Number']: lunName for lunName, info in node.lun.show().items()}

        # Pairs of LUNs and associated adapters (ex. 'key-vim.host.ScsiDisk-0100000...': 'key-vim.host.BlockHba-vmhba0')
        lunAdapters = {}
        for adapter in self.storageSystem.storageDeviceInfo.scsiTopology.adapter:
            adapterID = adapter.adapter
            for target in adapter.target:
                for lun in target.lun:
                    lunAdapters[lun.scsiLun] = adapterID

        # Mapping 'Adapter_Key: Adapter_Object'
        adapters = {adapter.key: adapter for adapter in self.storageSystem.storageDeviceInfo.hostBusAdapter}

        result = []
        for lun in self.storageSystem.storageDeviceInfo.scsiLun:
            # Pick HBA associated with given LUN
            # Ex. 'vmhba2'. vSC: Configuration/Storage Adapters
            adapter = adapters[lunAdapters[lun.key]]
            # Filter out non-FC adapters
            if not isinstance(adapter, vim.HostFibreChannelHba):
                continue

            serialNumber = array.array('B', lun.alternateName[1].data).tostring()

            lunInfo = {
                # Serial number stored as list of bytes (ex. converted: '8006GF1K3-XI'). Not seen on vSC
                'serial': serialNumber,

                # vSC: Configuration/Storage/Devices. Ex. 'disk'
                'device-type': lun.deviceType,

                # vSC: Devices (ex. ''NETAPP Fibre Channel Disk (naa.600a0980...')
                'display-name': lun.displayName,

                # vSC: Devices (ex. '/vmfs/devices/disks/naa.600a0980383030364746314b332d584d')
                'device-path': lun.devicePath,

                # Ex. 'disk'
                'lun-type': lun.lunType,

                # Ex. 'LUN FlashRay'. Not seen on vSC
                'model': lun.model.strip(),

                # Ex. '1000'
                'revision': lun.revision,

                # Ex. 4
                'scsiLevel': lun.scsiLevel,

                # Ex. 'naa.600a0980383030364746314b3654707a'. vSC: Storage/Devices
                'device': lun.canonicalName,

                'filer': None,

                # Size in MB. vSC: Storage/Devices
                'lun-size': lun.capacity.block * lun.capacity.blockSize / 1024 / 1024,

                # 'ok' if mounted. vSC: Storage/Devices
                'state': lun.operationalState[0],

                # Ex. 'NETAPP'
                'vendor': lun.vendor.strip(),

                'host-adapter': adapter.device,

                # vSC: Storage/Devices/Transport
                'protocol': self._getProtocolType(adapter),

                # TODO: Identify correct values to these attributes
                'multipath-device': None,
                'policy': None,
                'lun-pathname': marsLuns.get(serialNumber, lun.canonicalName) if marsLuns is not None else
                lun.canonicalName,
            }

            if self._satisfyFilters(lunInfo, filterAttrs):
                result.append(lunInfo)

        if uniqLuns:
            uniqDevs = []
            for dev in result:
                if dev['lun-pathname'] not in [x['lun-pathname'] for x in uniqDevs]:
                    uniqDevs.append(dev)
            result = uniqDevs[:]

        if available:
            availableDisks = [disk.canonicalName for disk in self.datastoreSystem.QueryAvailableDisksForVmfs()]
            result = [disk for disk in result if disk['device'] in availableDisks]

        # Include all LUNs or list of specified ones
        if len(luns):
            assert node is not None, 'To filter devices by LUNs provide node object to esxhost.getDevices()'
            filteredDevices = []
            for dev in result:
                if dev['lun-pathname'] in luns:
                    filteredDevices.append(dev)
            result = filteredDevices[:]

        LOG.l4('Obtained list of LUNs.')

        if groupBySize:
            resultDict = {}
            for dev in result:
                arr = resultDict.get(dev['lun-size'], [])
                arr.append(dev)
                resultDict[dev['lun-size']] = arr[:]
            return resultDict

        # List of {'device': ''/vmfs/dev...', ..., 'lun-size': 1024L etc.}
        return result

    def _getProtocolType(self, adapter):
        """
            Local method used to determine getProtocolType for getDevices()
        """
        try:
            # pyVmomi: vSphere API
            from pyVmomi import vim
        except ImportError:
            LOG.error(PYVMOMI_INSTALL_MSG)
            sys.exit(0)

        protocol = None
        if isinstance(adapter, vim.HostFibreChannelHba):
            protocol = 'FCP'
        elif isinstance(adapter, vim.HostBlockHba):
            protocol = 'BD'
        elif isinstance(adapter, vim.HostInternetScsiHba):
            protocol = 'iSCSI'
        elif isinstance(adapter, vim.HostParallelScsiHba):
            protocol = 'SPI'
        return protocol

    def _createDiskPartition(self, devicePath=None, size=None):
        """
            Create partition on LUN for subsequent formatting it with VMFS.
              pyVmomi API has a bug which set MBR format even when GPT is enforced, making it impossible to use
              CreateVmfsDatastore etc. for datastore larger than 2 TB. CLI calls are just workaround.
            @param devicePath:
        """
        # Format GPT supports datastores > 2 TB. Since ESXi 5.0
        partitionFormat = 'gpt'

        # One partition per device
        partitionNumber = 1

        # VMFS-5 starts from 2048th sector
        startSector = 2048

        diskPartitionInfo = self.storageSystem.RetrieveDiskPartitionInfo(devicePath=[devicePath])[0]
        chs = diskPartitionInfo.spec.chs

        # Alignment required
        endSector = chs.cylinder * chs.head * chs.sector - 1

        # Otherwise partition allocates all space on device
        if size is not None:
            size = int(utils.convertBytes(size=size)['B'])
            if size / 512 + 2048 - 1 > endSector:
                raise FailedConfigException('Specified datastore size %s exceeds LUN size.' % size)
            else:
                endSector = size / 512 + 2048 - 1

        # Partition type GUID. 'AA31E02A400F11DB9590000C2911D1B8' represents VMFS datastore
        partitionTypeGUID = 'AA31E02A400F11DB9590000C2911D1B8'

        # Partition attribute. Most partitions have 0
        partitionAttribute = '0'

        cmdResult = self.cmd(('/sbin/partedUtil setptbl {devicePath} {partitionFormat} "{partitionNumber} ' +
        '{startSector} {endSector} {partitionTypeGUID} {partitionAttribute}"').format(devicePath=devicePath,
        partitionFormat=partitionFormat, partitionNumber=partitionNumber, startSector=startSector, endSector=endSector,
        partitionTypeGUID=partitionTypeGUID, partitionAttribute=partitionAttribute)).splitlines()
        # If success, result appears as:
        #   gpt
        #   0 0 0 0
        #   <668373> 255 63 <10737418240>
        if not (len(cmdResult) == 3 and cmdResult[0] == 'gpt' and cmdResult[1] == '0 0 0 0'):
            raise FailedConfigException('Disk partitioning failed:\n' + '\n'.join(cmdResult))

    def _getVimDatastore(self, id=''):
        """
            Return object of class Vim.Datastore located by name or URL. In some cases name may become changed (by ghost
              processes of vpxuser) making it impossible to refer to datastore by name. In such cases using URL is more
              robust.
            @param id: Name or URL ((ex. '/vmfs/volumes/551e7501-84a2af30-bc67-0cc47a1a412c')) of datastore to be
              located
            @return: Reference to Vim.Datastore instance
        """
        try:
            datastore = [ds for ds in self.datacenter.datastore if (ds.name == id) or (ds.summary.url == id)][0]
        except IndexError:
            raise KeyError("Datastore with ID '%s' not found." % id)

        return datastore

    def getDatastores(self, names=[]):
        """
            Return [] with info about datastores
            @param names: Return only those datastores whose names or URLs are in given list (optional)
            @return: List of key-valued info about datastores
        """

        LOG.l4('Obtaining list of datastores...')

        datastoreInfo = []
        for datastore in self.datacenter.datastore:
            if not names or (datastore.name in names) or (datastore.summary.url in names):
                datastoreInfo.append({
                    'name': datastore.name,
                    'url': datastore.summary.url,
                    'capacity': datastore.summary.capacity,
                    'freeSpace': datastore.summary.freeSpace,
                    'type': datastore.summary.type,
                    '_vimObject': datastore
                })

        LOG.l4('Obtained list of datastores.')

        return datastoreInfo

    def createFS(self, name, device='', size=None, vmfsMajorVersion=5, remoteHost='', remotePath='', method='SAN', ):
        """
            if method == 'SAN'
            Create VMFS datastore
            @param name: Name of created VMFS
            @param device: Device NAA addressing ID. Ex. 'naa.600a0980383030364746314b3654707a'. vSC: Storage/Devices
            @param size: If not specified, use all available space on device
            @param vmfsMajorVersion: Version of VMFS (most likely 3 or 5, integer)
            @return: Datastore info as key-valued pairs (see getDatastores())

            if method == 'NAS'
            Create network-attached storage datastore
            @param name: Local name of new datastore (ex. 'nfs_datastore_1')
            @param remoteHost: Name of host that runs NFS server (ex. 'mls.marslab.netapp.com')
            @param remotePath: Local path to the folder on NFS server (ex. '/lab')
              Example: To mount NFS share as datastore use:
              createFS(method='NAS', name='Datastore_1', remoteHost='mls.marslab.netapp.com', remotePath='lab/template')
            @return: Datastore info as key-valued pairs (see getDatastores())
        """
        if method == 'SAN':
            try:
                vimDevice = [vimDevice for vimDevice in self.datastoreSystem.QueryAvailableDisksForVmfs() if
                vimDevice.canonicalName == device][0]
            except IndexError:
                raise FailedConfigException('Device "%s" not available.' % device)

            if size is not None:
                size = int(utils.convertBytes(size=size)['B'])

            self._createDiskPartition(devicePath=vimDevice.devicePath, size=size)

            createSpec = vim.HostVmfsSpec()
            createSpec.extent = vim.HostScsiDiskPartition(diskName=vimDevice.canonicalName, partition=1)
            createSpec.majorVersion = vmfsMajorVersion
            createSpec.volumeName = name
            LOG.info("Creating VMFS datastore '%s' on device '%s'..." % (name, device))
            self.storageSystem.FormatVmfs(createSpec=createSpec)
            LOG.info("VMFS datastore '%s' created on device '%s'." % (name, device))

            return self.getDatastores(names=[name])[0]

        elif method == 'NAS':
            nasVolumeSpec = vim.HostNasVolumeSpec(accessMode=vim.HostMountMode.readWrite, localPath=name,
                remoteHost=remoteHost, remotePath=remotePath)

            LOG.info("Creating NAS datastore '%s'..." % name)

            self.datastoreSystem.CreateNasDatastore(spec=nasVolumeSpec)
            LOG.info("NAS datastore '%s' created." % name)

            return self.getDatastores(names=[name])[0]

    def createDatastore(self, **kwargs):
        """
            Alias for createFS
        """
        return self.createFS(**kwargs)

    def expandDatastore(self, device, name=None, size=None):
        """
            Expand (grow in size) datastore on the same extent
            @param device: Name of device which datastore will use to grow (ex. 'naa.600a0980383030364746314b3654707a')
            @param name: Name or URL (ex. '/vmfs/volumes/551e7501-84a2af30-bc67-0cc47a1a412c') of datastore to expand.
              Use getDatastores() to get info about existing datastores
            @param size: New size of datastore; limited by size of partition on storage device
        """

        LOG.l4("Expanding datastore '%s'..." % name)

        datastore = self._getVimDatastore(id=name)

        try:
            diskName = [disk.diskName for disk in datastore.info.vmfs.extent if disk.diskName == device][0]
        except IndexError:
            raise FailedConfigException("Device '%s' does not belong to datastore '%s'." % (device, datastore.name))

        try:
            datastoreSpec = self.datastoreSystem.QueryVmfsDatastoreExpandOptions(datastore=datastore)[0].spec
        except IndexError:
            raise FailedConfigException("Unable to expand datastore '%s' on device '%s'." % (datastore.name,
            name))

        if size is not None:
            size = int(utils.convertBytes(size=size)['B'])
            datastoreSpec.partition.partition[0].endSector = size / 512 - 1

        self.datastoreSystem.ExpandVmfsDatastore(datastore=datastore, spec=datastoreSpec)
        LOG.info("Datastore '%s' expanded to %s bytes." % (datastore.name, datastore.summary.capacity))

    def extendDatastore(self, device, name=None, size=None):
        """
            Extend datastore by adding new extent
            @param device: Device NAA ID on which new extent will reside. Ex. 'naa.600a0980383030364746314b3654707a'
            @param name: Name or URL (ex. '/vmfs/volumes/551e7501-84a2af30-bc67-0cc47a1a412c') of datastore to extend.
              Use getDatastores() to get info about existing datastores
            @param size: If not specified, all available space on new extent will be used
        """

        LOG.l4("Extending datastore '%s'..." % name)

        datastore = self._getVimDatastore(id=name)

        try:
            vimDevice = [vimDevice for vimDevice in self.datastoreSystem.QueryAvailableDisksForVmfs() if
            vimDevice.canonicalName == device][0]
        except IndexError:
            raise FailedConfigException('Device "%s" not found.' % device)

        if size is not None:
            size = int(utils.convertBytes(size=size)['B'])
        # Create partition on another LUN
        self._createDiskPartition(devicePath=vimDevice.devicePath, size=size)

        extent = vim.HostScsiDiskPartition(diskName=vimDevice.canonicalName, partition=1)
        # Extend datastore by attaching partition on another LUN
        self.storageSystem.AttachVmfsExtent(vmfsPath=datastore.info.url, extent=extent)
        datastore.RefreshDatastoreStorageInfo()

        LOG.info("Datastore '%s' extended by additional %s bytes on device '%s'." % (datastore.name, size, device))

    def removeDatastore(self, name=None, force=True):
        """
            Delete datastore from datacenter
            @param name: Name or URL (ex. '/vmfs/volumes/551e7501-84a2af30-bc67-0cc47a1a412c') of datastore to be
              deleted. Use getDatastores() to get info about existing datastores
            @param force: If True, power off and unregister all VMs residing on datastore, then remove datastore
        """

        LOG.l4("Removing datastore '%s'..." % name)

        datastore = self._getVimDatastore(id=name)

        # List of VMs residing on given datastore
        datastoreVMs = [vm.name for vm in datastore.vm]

        if force:
            for vm in VirtualMachines(host=self, names=datastoreVMs):
                vm.powerOff()
                vm.vm.UnregisterVM()

        self.datastoreSystem.RemoveDatastore(datastore=datastore)
        LOG.info("Datastore '%s' removed." % name)

    def getVMs(self, **filterAttrs):
        """
            Return VirtualMachines() object representing collection of VirtualMachine() objects
            @param filterAttrs: Will select only VMs satisfying filter condition (ex. name='linux_48EA35')
            @return: VirtualMachines() object representing collection of selected VMs
        """

        LOG.l4('Obtaining list of virtual machines...')

        filteredVMs = []
        # Sort through all VMs in datacenter
        for vm in VirtualMachines(host=self):
            if self._satisfyFilters(vars(vm), filterAttrs):
                filteredVMs.append(vm.name)

        LOG.l4('Obtained list of virtual machines.')

        return VirtualMachines(host=self, names=filteredVMs)

    def createVM(self, guestID, datastore=None, vmPrefix=None, cpu=1, memory=None, random=False,
    waitForFinish=True):
        """
            Create virtual machine
            @param datastore: Name or URL (ex. '/vmfs/volumes/551e7501-84a2af30-bc67-0cc47...') of datastore where VM
              will reside on
            @param vmPrefix: Name prefix of new VM. If 'random' is True, add unique ID to name prefix
            @param cpu: Number of CPU cores in guest
            @param memory: Memory space reserved for guest
            @param guestID: ID denoting type and version of guest OS (google for VirtualMachineGuestOsIdentifier)
                For e.g.:
                    Marsdev: debian5Guest,
                    Windows 7 64-bit: windows7_64Guest
            @param random: If True, add to the name prefix unique ID (ex. 'NewVM_9cdcd941')
            @param waitForFinish: If True, wait until VM creation completed; if False - leave it running and proceed
        """
        datastore = self._getVimDatastore(id=datastore)

        LOG.l4("Creating virtual machine '%s' on datastore '%s'..." % (vmPrefix, datastore.name))

        if random:
            vmName = vmPrefix + '_' + str(uuid.uuid4())[:8]
        else:
            vmName = vmPrefix

        vmConfigSpec = vim.VirtualMachineConfigSpec()
        vmConfigSpec.name = vmName
        vmConfigSpec.numCPUs = cpu
        memory = int(utils.convertBytes(size=memory)['M'])
        vmConfigSpec.memoryMB = memory
        vmConfigSpec.guestId = guestID
        vmConfigSpec.files = vim.VirtualMachineFileInfo(vmPathName='[' + datastore.name + '] ')

        taskThread = ESXTask(tasks=[{
            'task': self.datacenter.vmFolder.CreateVM_Task,
            'kwargs': {
                'config': vmConfigSpec,
                'pool': self.datacenter.hostFolder.childEntity[0].resourcePool
            }
        }])
        taskThread.start()

        if waitForFinish:
            taskThread.join()
            # Wait until new VM appeared in registered VMs
            while vmName not in [vimVM.name for vimVM in self.datacenter.vmFolder.childEntity]:
                pass

            LOG.info("Virtual machine '%s' created." % vmName)

            return VirtualMachine(host=self, name=vmName)

    def cloneVM(self, sourceDatastore=None, sourceVM=None, targetDatastore=None, targetVMPrefix=None, random=False,
    count=1, parallel=3, force=True):
        """
            Clone VM: Copy VM files to specified location + register VM.
              New VM name is formed as: <new_vm_name> = <source_vm_name> + _<unique_timestamp>,
              ex. 'small-linux' -> 'small-linux_D45EA2'; 'small-linux_D45EA2' -> 'small-linux_D45EA2_15F2A4' etc.
            @param sourceDatastore: Name or URL (ex. '/vmfs/volumes/551e7501-84a2af30-bc67-0cc47a1a412c') of source
              datastore. See ESXHost.createFS() on how to obtain NFS-backed datastore
            @param sourceVM: Folder name of VM
            @param targetDatastore: Name or URL of target datastore
            @param targetVMPrefix: Name prefix of new VM. Can be extended by unique ID (ex. 'ClonedVM_2c2714c7')
            @param random: If True or if 'count' > 1, name prefix will be extended by unique ID
            @param count: Number of new VMs to be cloned
            @param parallel: If integer, limit number of concurrent threads; otherwise let the system to ran them as
              many as it can
            @param force: If True, delete target VM folder in advance
            @return: If waitForFinish == True, return list of created VirtualMachine() objects; otherwise return None
        """
        sourceDatastore = self._getVimDatastore(id=sourceDatastore)
        targetDatastore = self._getVimDatastore(id=targetDatastore)

        LOG.info("Cloning virtual machine '%s' %s time(s)..." % (sourceVM, count))

        # Ex. '[template_vm] ms-dos-6.22'
        sourceVMPath = '[' + sourceDatastore.name + '] ' + sourceVM

        # Obtain list of files in source VM's folder
        searchFileTask = self.hostSystem.datastoreBrowser.SearchDatastore_Task(datastorePath=sourceVMPath)
        while searchFileTask.info.state not in [vim.TaskInfoState.success, vim.TaskInfoState.error]:
            pass

        if searchFileTask.info.state != vim.TaskInfoState.success:
            raise LookupError("Source virtual machine '%s' not found." % sourceVM)

        # Get list of VM's files, filtering out flat disks and logs
        vmFiles = []
        for file in searchFileTask.info.result.file:
            if '-flat.vmdk' not in file.path and file.path[-4:] != '.log':
                vmFiles.append(file.path)
            if file.path[-4:] == '.vmx':
                # Get base name of VM (VMX file name without extension) (ex. 'marsdev', 'linux-ubuntu')
                vmBaseName = file.path[:-4]

        vmFiles = [file.path for file in searchFileTask.info.result.file if '-flat.vmdk' not in file.path and
        file.path[-4:] != '.log']

        vmCount = count
        startTime = time.time()

        # Highest number of parallel deployments equals 'parallel'
        activeThreads = {}
        clonedVMNames = []
        while True:
            # If not exceeded number of parallel and total deployments
            if len(activeThreads) < parallel and vmCount > 0:
                if targetVMPrefix is None:
                    vmUniqueName = sourceVM + '_' + str(uuid.uuid4())[:8]

                else:
                    vmUniqueName = targetVMPrefix
                    if count > 1 or random:
                        vmUniqueName = targetVMPrefix + '_' + str(uuid.uuid4())[:8]

                cloneVMThread = threading.Thread(target=self._copyVMFiles, args=(sourceDatastore, sourceVM, vmBaseName,
                targetDatastore, vmUniqueName, vmFiles, force))
                activeThreads[vmUniqueName] = cloneVMThread
                cloneVMThread.start()
                vmCount -= 1
                clonedVMNames.append(vmUniqueName)

            # Inspect all running threads
            for key in activeThreads.keys():
                # If thread finished,
                if not activeThreads[key].is_alive():
                    # remove it from active threads
                    del activeThreads[key]

            if not activeThreads and vmCount == 0:
                break

        LOG.info("Cloning virtual machine '%s' into %s virtual machine(s) has finished in %s sec." % (sourceVM, count,
        int(time.time() - startTime)))

        return VirtualMachines(host=self, names=clonedVMNames)

    def _copyVMFiles(self, sourceDatastore, sourceVM, vmBaseName, targetDatastore, targetVM, vmFiles, force):
        # Ex. '[datastore_2] ms-dos-6.22'
        targetVMPath = '[' + targetDatastore.name + '] ' + targetVM

        # Enforce target folder removal if it exists
        if force:
            searchFolderTask = self.hostSystem.datastoreBrowser.SearchDatastore_Task(datastorePath=targetVMPath)
            while searchFolderTask.info.state not in [vim.TaskInfoState.success, vim.TaskInfoState.error]:
                pass

            if searchFolderTask.info.state == vim.TaskInfoState.success:
                # Delete target folder in advance
                deleteFolderTask = self.serviceContent.fileManager.DeleteDatastoreFile_Task(name=targetVMPath)
                while deleteFolderTask.info.state not in [vim.TaskInfoState.success, vim.TaskInfoState.error]:
                    pass

        self.serviceContent.fileManager.MakeDirectory(name=targetVMPath)

        sourceVMPath = '[' + sourceDatastore.name + '] ' + sourceVM
        LOG.info("Cloning virtual machine '%s' to '%s'..." % (sourceVMPath, targetVMPath))

        tasks = []
        # Copy all listed files
        for fileName in vmFiles:
            if '.vmdk' in fileName:
                # CopyDatastoreFile_Task() does make a copy of fully provisioned file, which may be huge.
                #   vmkfstools retain thin provisioning copying over only thin part of VMDK, what is way faster
                result = self.cmd(('vmkfstools --clonevirtualdisk "{sourceFilePathName}" ' +
                '"{targetFilePathName}" --diskformat=thin').format(sourceFilePathName=
                sourceDatastore.summary.url + '/' + sourceVM + '/' + fileName, targetFilePathName=
                targetDatastore.summary.url + '/' + targetVM + '/' + fileName))

                if 'Clone: 100% done.' not in result:
                    raise IOError("Cloning '%s.vmdk' failed." % vmBaseName)
            else:
                sourceName = sourceVMPath + '/' + fileName
                destinationName = '[' + targetDatastore.name + '] ' + targetVM + '/' + fileName
                copyFileTask = self.serviceContent.fileManager.CopyDatastoreFile_Task(sourceName=sourceName,
                destinationName=destinationName)
                while copyFileTask.info.state not in [vim.TaskInfoState.success, vim.TaskInfoState.error]:
                    pass
                if copyFileTask.info.state != vim.TaskInfoState.success:
                    raise FailedConfigException('File copy failed:\n' + copyFileTask.info.error)

        path = '[' + targetDatastore.name + '] ' + targetVM + '/' + vmBaseName + '.vmx'
        registerVMTask = self.datacenter.vmFolder.RegisterVM_Task(path=path, name=targetVM, asTemplate=False,
        pool=self.datacenter.hostFolder.childEntity[0].resourcePool)
        while registerVMTask.info.state not in [vim.TaskInfoState.success, vim.TaskInfoState.error]:
            pass
        if registerVMTask.info.state != vim.TaskInfoState.success:
            raise FailedConfigException('Registering virtual machine failed:\n' + registerVMTask.info.error)

    def deployOVA(self, ovaPath='/x/mars/qa/tools/vm_images/', ovaName=None, datastore=None, vmPrefix=None, random=False,
    count=1, parallel=3):
        """
            Deploy OVA package as new virtual machine.
            @param ovaPath: Local path to folder with OVA package file
            @param ovaName: Name of OVA package file
            @param datastore: Name or URL (ex. '/vmfs/volumes/551e7501-84a2af30-bc67-0cc47a1a412c') of datastore where
              new virtual machine is deployed
            @param vmPrefix: Name prefix of new virtual machine. Can be extended by unique ID
            @param random: if True, add unique ID to name prefix
            @param count: Number of virtual machines to be deployed
            @param parallel: Number of parallel deployments
        """
        datastore = self._getVimDatastore(id=datastore)

        if count == 1:
            LOG.info("Deploying OVA package '%s' into virtual machine '%s'..." % (ovaName, vm))
        elif count > 1:
            LOG.info("Deploying OVA package '%s' into %s virtual machine(s)..." % (ovaName, count))
        else:
            raise FailedConfigException('Invalid count number %s.' % count)

        ovftoolHost = LinuxHost(marsconfig.IPMANAGER)
        vmCount = count
        startTime = time.time()
        # Highest number of parallel deployments equals 'parallel'
        activeThreads = {}
        while True:
            # If not exceeded number of parallel and total deployments
            if len(activeThreads) < parallel and vmCount > 0:
                if vmPrefix is None:
                    vmUniqueName = ovaName.split('.')[0] + '_' + str(uuid.uuid4())[:8]

                else:
                    vmUniqueName = vmPrefix
                    if count > 1 or random:
                        vmUniqueName = vmUniqueName + '_' + str(uuid.uuid4())[:8]

                deployOVAThread = _OVADeploy(ovftoolHost=ovftoolHost, datastore=datastore, vmName=vmUniqueName,
                ovaPath=ovaPath, ovaName=ovaName, esxHostName=self.host, username=self.user, password=self.password)
                activeThreads[vmUniqueName] = deployOVAThread
                deployOVAThread.start()
                vmCount -= 1

            # Inspect all running threads
            for key in activeThreads.keys():
                # If thread finished,
                if not activeThreads[key].is_alive():
                    if activeThreads[key].failed:
                        raise activeThreads[key].error
                    # remove it from active threads
                    del activeThreads[key]

            if not activeThreads and vmCount == 0:
                break

        LOG.info("Deploying OVA package '%s' into %s virtual machines has finished in %s sec." % (ovaName, count,
        int(time.time() - startTime)))


class _OVADeploy(threading.Thread):
    """
        Deploy OVA package to single instance of virtual machine.
    """
    def __init__(self, ovftoolHost, datastore, vmName, ovaPath, ovaName, esxHostName, username, password):
        threading.Thread.__init__(self)
        self.ovftoolHost = ovftoolHost
        self.datastore = datastore
        self.vmName = vmName
        self.ovaPath = ovaPath
        self.ovaName = ovaName
        self.esxHostName = esxHostName
        self.username = username
        self.password = password
        self.failed = False
        self.error = None

    def run(self):
        esxHostDomainName = getFQDN(self.esxHostName)

        cmdLine = ("/usr/lib/vmware-ovftool/ovftool --noSSLVerify --sourceType=OVA --targetType=VI " +
        "--datastore={dsName} --name={vmName} {ovaPath}{ovaName} vi://{username}:'{password}'@{esxHostDomainName}:443").format(dsName=self.datastore.name, vmName=self.vmName, ovaPath=self.ovaPath, ovaName=self.ovaName,
        username=self.username, password=self.password, esxHostDomainName=esxHostDomainName)

        LOG.info(cmdLine)
        try:
            cmdResult = self.ovftoolHost.cmd(cmdLine)
            LOG.info("Deployed OVA package '%s' into virtual machine '%s':\n%s" % (self.ovaName, self.vmName,
            cmdResult))

        except Exception as e:
            self.failed = True
            self.error = e
            LOG.info("Deploying OVA package '%s' failed:\n%s" % (self.ovaName, cmdResult))


class VirtualMachines(object):
    """
        Represent collection of VirtualMachine() objects
    """
    def __init__(self, host, names=[]):
        """
            @param host: Instance of ESXHost class
            @param names: select only VMs with names in provided list; if empty, select all VMs
        """
        self.host = host
        self.vms = []

        for vm in self.host.datacenter.vmFolder.childEntity:
            # If not filtered by names or satisfies filter, and VM is available
            if (not names or vm.name in names) and vm.configStatus == vim.ManagedEntityStatus.green:
                self.vms.append(VirtualMachine(host=host, name=vm.name))

    def __getitem__(self, vmItem):
        # Lookup by index
        if type(vmItem) is int:
            return self.vms[vmItem]
        # Lookup by VM name
        elif type(vmItem) is str:
            try:
                return [vm for vm in self.vms if vm.name == vmItem][0]
            except IndexError:
                raise KeyError("Virtual machine '%s' not found." % vmItem)

    def __setitem__(self, *args):
        # Workaround to avoid 'AttributeError: __setitem__' on 'vms[0] *= 3'
        pass

    def __iter__(self):
        for vm in self.vms:
            yield vm

    def __len__(self):
        return len(self.vms)


class VirtualMachine(object):
    """
        Represent underlying type vim.VirtualMachine
    """
    def __init__(self, host=None, name=None):
        self.host = host
        try:
            self.vm = [vm for vm in self.host.datacenter.vmFolder.childEntity if vm.name == name][0]
        except IndexError:
            raise KeyError("Virtual machine '%s' not found." % name)

        dsURL = self.vm.datastore[0].summary.url
        self.datastore = self.host.getDatastores(names=[dsURL])[0]

        self.refreshConfig()

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<express.host.VirtualMachine {name}>'.format(name=self.name)

    def __imul__(self, count):
        """
            Clone this VM to multiple new VMs on the same datastore. Ex. thisVM *= 10
            @param count: Number of new VMs to clone
        """
        self.host.cloneVM(sourceDatastore=self.vm.datastore[0].summary.url, sourceVM=self.vm.name,
        targetDatastore=self.vm.datastore[0].summary.url, count=count, parallel=3, force=True)

    def refreshConfig(self):
        """
            Re-request config of underlying vim.VirtualMachine object
        """
        self.name = self.vm.name
        self.guestOS = self.vm.config.guestFullName
        self.guestID = self.vm.config.guestId
        self.cpu = self.vm.config.hardware.numCPU
        self.memory = self.vm.config.hardware.memoryMB * 1024 * 1024
        self.powerState = self.getPowerState()
        self.uuid = self.vm.config.uuid
        self.version = self.vm.config.version
        self.scsiControllers = [{'busNumber': vmDevice.busNumber, 'controllerKey': vmDevice.controllerKey,
        'key': vmDevice.key, 'unitNumber': vmDevice.unitNumber, 'label': vmDevice.deviceInfo.label,
        'summary': vmDevice.deviceInfo.summary} for vmDevice in self.vm.config.hardware.device if isinstance(vmDevice,
        vim.VirtualSCSIController)]

        LOG.l4("Refreshed configuration of virtual machine '%s'." % self.name)

    def addSCSIController(self):
        """
        Add SCSI Controller to VM
        """

        LOG.l4("Adding SCSI controller to virtual machine '%s'..." % self.name)

        vdConfigSpec = vim.VirtualDeviceConfigSpec()
        vdConfigSpec.operation = vim.VirtualDeviceConfigSpecOperation.add
        vdConfigSpec.device = vim.VirtualLsiLogicController()
        vdConfigSpec.device.sharedBus = vim.VirtualSCSISharing.noSharing
        vmConfigSpec = vim.VirtualMachineConfigSpec()
        vmConfigSpec.deviceChange = [vdConfigSpec]
        taskThread = ESXTask(tasks=[{
            'task': self.vm.ReconfigVM_Task,
            'kwargs': {
                'spec': vmConfigSpec
            }
        }])
        taskThread.start()
        taskThread.join()
        self.refreshConfig()

        LOG.l4("Added SCSI controller to virtual machine '%s'." % self.name)

    def getPowerState(self):
        """
            Return VM's power state
            @return: Current power state of VM (poweredOn, poweredOff, suspended)
        """
        self.powerState = self.vm.runtime.powerState
        return self.powerState

    def powerOn(self, force=False, waitForFinish=True):
        """
            Power on VM
            @param force: Enforce powering off regardless VM's state, otherwise handle VM gracefully
            @param waitForFinish: If True, wait until VM powered on; otherwise proceed
        """
        if self.vm.runtime.powerState != vim.VirtualMachinePowerState.poweredOn or force:
            taskThread = ESXTask(tasks=[{
                'task': self.vm.PowerOnVM_Task
            }])
            taskThread.start()

            if waitForFinish:
                taskThread.join()
                LOG.info("Virtual machine '%s' powered on." % self.name)
            self.powerState = self.vm.runtime.powerState

    def powerOff(self, force=False, waitForFinish=True):
        """
            Power off VM
            @param force: Enforce powering on regardless VM's state, otherwise handle VM gracefully
            @param waitForFinish: If True, wait until VM powered off; otherwise proceed
        """
        if self.vm.runtime.powerState != vim.VirtualMachinePowerState.poweredOff or force:
            taskThread = ESXTask(tasks=[{
                'task': self.vm.PowerOffVM_Task
            }])
            taskThread.start()

            if waitForFinish:
                taskThread.join()
                LOG.info("Virtual machine '%s' powered off." % self.name)
            self.powerState = self.vm.runtime.powerState

    def suspend(self, force=False, waitForFinish=True):
        """
            Suspend VM
            @param force: Enforce suspending regardless VM's state, otherwise handle VM gracefully
            @param waitForFinish: If True, wait until VM suspended; otherwise proceed
        """
        if self.vm.runtime.powerState != vim.VirtualMachinePowerState.suspended or force:
            tasks = []
            # If not forced, first make (sure) VM powered on, then suspend
            if self.vm.runtime.powerState == vim.VirtualMachinePowerState.poweredOff and not force:
                # VM not guaranteed to have state poweredOn right after 'success' state detection
                vmPowerOnTask = self.vm.PowerOnVM_Task()
                while vmPowerOnTask.info.state not in [vim.TaskInfoState.success, vim.TaskInfoState.error]:
                    pass
                while self.vm.runtime.powerState != vim.VirtualMachinePowerState.poweredOn:
                    pass

            tasks.append({
                'task': self.vm.SuspendVM_Task
            })

            taskThread = ESXTask(tasks=tasks)
            taskThread.start()

            if waitForFinish:
                taskThread.join()
                LOG.info("Virtual machine '%s' suspended." % self.name)
            self.powerState = self.vm.runtime.powerState

    def reconfigure(self, cpu=None, memory=None, force=True):
        """
            Modify CPU and memory settings of VM
            @param cpu: New number of CPUs
            @param memory: New memory size
            @param force: If True, power of VM in advance then return to original state
            @return: None
        """

        LOG.info("Reconfiguring virtual machine '%s'..." % self.name)

        precedingPowerState = self.vm.runtime.powerState
        self.powerOff()

        vmConfigSpec = vim.VirtualMachineConfigSpec()
        if cpu is not None:
            vmConfigSpec.numCPUs = cpu

        if memory is not None:
            memory = int(utils.convertBytes(size=memory)['M'])
            vmConfigSpec.memoryMB = memory

        vmReconfigureTask = self.vm.ReconfigVM_Task(spec=vmConfigSpec)
        while vmReconfigureTask.info.state not in [vim.TaskInfoState.success, vim.TaskInfoState.error]:
            pass
        if vmReconfigureTask.info.state == vim.TaskInfoState.error:
            raise FailedConfigException('Failed to reconfigure virtual machine:\n' + vmPowerOffTask.info.error)

        self.refreshConfig()
        if force:
            self._switchToPowerState(precedingPowerState)
        LOG.info("Virtual machine '{vm}' reconfigured: CPU({cpu}) Memory({memory})".format(vm=self.name, cpu=self.cpu,
        memory=self.memory))

    def _switchToPowerState(self, state):
        powerstates = {
            vim.VirtualMachinePowerState.poweredOn: self.powerOn,
            vim.VirtualMachinePowerState.poweredOff: self.powerOff,
            vim.VirtualMachinePowerState.suspended: self.suspend}

        powerstates.get(state, vim.VirtualMachinePowerState.poweredOn)()

    def getDisks(self, diskIDs=[]):
        """
            Return disk info
            @param diskIDs: Return only those disks whose keys (ex. 2002) are in given list (optional)
            @return: List of key-valued info about all virtual disks mounted in given VM
        """
        virtualDisks = []
        for device in self.vm.config.hardware.device:
            if isinstance(device, vim.VirtualDisk) and (not diskIDs or device.key in diskIDs):
                virtualDisks.append({
                    'label': device.deviceInfo.label,
                    'summary': device.deviceInfo.summary,
                    'capacity': device.capacityInKB * 1024,
                    'controllerKey': device.controllerKey,
                    'diskID': device.key,
                    'unitNumber': device.unitNumber,
                    'uuid': device.backing.uuid,
                    'diskMode': device.backing.diskMode,
                    'fileName': device.backing.fileName
                })

        LOG.l4("Obtained list of virtual disks of virtual machine '%s'." % self.name)

        return virtualDisks

    def addDisk(self, device=None, datastore=None, size=None, filename=None, waitForFinish=True, force=True):
        """
            Add virtual disk to VM
            @param device: If set (device NAA ID, ex. 'naa.600a0980383030364746314b3654707a'), use LUN as disk backing
                (argument 'datastore' will be ignored). If omitted, disk will be created either on specified datastore
                or in VM's folder (if 'datastore' omitted)
            @param datastore: Name or URL (ex. '/vmfs/volumes/551e7501-84a2af30-bc67-0cc47a1a412c') of datastore. Takes
              action only when 'device' omitted
            @param dsURL: URL of datastore
            @param size: If disk is LUN-backed, all device's space used
            @param filename: Filename for host file used in backing as [<datastore_name>] <vm_folder_name>/<vm_name>.vmdk'
            @param waitForFinish: If True, wait until operation completed; otherwise proceed
            @param force: If True, handle VM gracefully: power it off, add new disk, bring back to original state
        """

        LOG.l4("Adding virtual disk on virtual machine '%s'..." % self.name)

        if force:
            precedingPowerState = self.vm.runtime.powerState
            self.powerOff()
            if not self.scsiControllers:
                self.addSCSIController()
                self.refreshConfig()

        elif self.vm.runtime.powerState != vim.VirtualMachinePowerState.poweredOff:
            raise FailedConfigException('Virtual machine should be powered off before adding disk.')
        elif not self.scsiControllers:
            raise LookupError('SCSI controller not found.')

        unitNumber = None
        for scsiController in self.scsiControllers:
            # Make list of occupied unit numbers on given controller
            unitNumbersInUse = [vmDevice.unitNumber for vmDevice in self.vm.config.hardware.device if
            vmDevice.controllerKey == scsiController['key']]

            # Normally up to 15 devices per controller, number 7 reserved for the controller
            for number in range(1, 16):
                if (number not in unitNumbersInUse) and (number != 7):
                    unitNumber = number
                    controllerKey = scsiController['key']
                    break
            if unitNumber is not None:
                break

        # No SCSI controllers or no available numbers on controllers
        if unitNumber is None:
            raise LookupError('No available unit numbers on SCSI controller.')

        vDiskSpec = vim.VirtualDeviceConfigSpec()
        vDiskSpec.device = vim.VirtualDisk()
        vDiskSpec.device.controllerKey = controllerKey
        vDiskSpec.device.unitNumber = unitNumber

        # Disk represents LUN mapped as RDM
        if device is not None:
            try:
                # Pick disk by it's NAA ID
                disk = [disk for disk in self.host.datastoreSystem.QueryAvailableDisksForVmfs() if disk.canonicalName
                == device][0]
            except IndexError:
                raise IndexError("Device '%s' not found." % device)

            backing = vim.VirtualDiskRawDiskMappingVer1BackingInfo()
            backing.compatibilityMode = vim.VirtualDiskCompatibilityMode.physicalMode
            # Ex. '/vmfs/devices/disks/naa.600a0980383030364746314b36547133'
            backing.deviceName = disk.deviceName
            backing.diskMode = vim.VirtualDiskMode.independent_persistent

        # Virtual flat disk file residing on either specified datastore or VM's datastore
        else:
            backing = vim.VirtualDiskFlatVer2BackingInfo()
            backing.diskMode = vim.VirtualDiskMode.persistent

            # Datastore what virtual disk will reside on
            if datastore is not None:
                datastore = self.host._getVimDatastore(id=datastore)
                backing.datastore = datastore
            else:
                backing.datastore = self.vm.datastore[0]
            if size is not None:
                size = int(utils.convertBytes(size=size)['K'])
                vDiskSpec.device.capacityInKB = size

        if filename is not None:
            backing.fileName = filename

        vDiskSpec.device.backing = backing
        vDiskSpec.fileOperation = vim.VirtualDeviceConfigSpecFileOperation.create
        vDiskSpec.operation = vim.VirtualDeviceConfigSpecOperation.add

        vmConfigSpec = vim.VirtualMachineConfigSpec()
        vmConfigSpec.deviceChange = [vDiskSpec]

        taskThread = ESXTask(tasks=[{
            'task': self.vm.ReconfigVM_Task,
            'kwargs': {
                'spec': vmConfigSpec
            }
        }])

        taskThread.start()

        if waitForFinish:
            taskThread.join()

            try:
                diskKey = [vmDevice.key for vmDevice in self.vm.config.hardware.device if vmDevice.controllerKey ==
                controllerKey and vmDevice.unitNumber == unitNumber][0]
            except IndexError:
                raise IndexError('Created disk not found.')

            LOG.info("Virtual disk added to virtual machine '%s'." % self.name)

            if force:
                if precedingPowerState == vim.VirtualMachinePowerState.poweredOn:
                    self.powerOn()
                elif precedingPowerState == vim.VirtualMachinePowerState.poweredOff:
                    self.powerOff()
                elif precedingPowerState == vim.VirtualMachinePowerState.suspended:
                    self.suspend()
            return self.getDisks(diskIDs=[diskKey])[0]

    def removeDisk(self, diskID=None, waitForFinish=True):
        """
            Remove virtual disk from VM
            @param diskID: Actually, disk key (ex. 2001, 2003, 3000) unique across all devices mounted on VM. Use
              getDisks() to obtain info about mounted disks
            @param waitForFinish: If True, wait until operation completed; otherwise proceed
        """

        LOG.l4("Removing virtual disk 'diskID: %s' from virtual machine '%s'..." % (diskID, self.name))

        try:
            vDisk = [device for device in self.vm.config.hardware.device if isinstance(device, vim.VirtualDisk) and
            device.key == diskID][0]
        except IndexError:
            raise KeyError("Virtual disk ID: '%s' not found." % diskID)

        vDiskSpec = vim.VirtualDeviceConfigSpec()
        vDiskSpec.device = vDisk
        vDiskSpec.fileOperation = vim.VirtualDeviceConfigSpecFileOperation.destroy
        vDiskSpec.operation = vim.VirtualDeviceConfigSpecOperation.remove

        vmConfigSpec = vim.VirtualMachineConfigSpec(deviceChange=[vDiskSpec])

        taskThread = ESXTask(tasks=[{
            'task': self.vm.ReconfigVM_Task,
            'kwargs': {
                'spec': vmConfigSpec
            }
        }])

        taskThread.start()

        if waitForFinish:
            taskThread.join()
            LOG.info("Virtual machine '%s': Virtual disk id: '%s' removed." % (self.name, diskID))

    def delete(self, waitForFinish=True):
        """
            Delete VM (remove from inventory and delete VM files)
            @param waitForFinish: If True, wait for task completion; otherwise proceed
        """

        LOG.l4("Deleting virtual machine '%s'..." % self.name)

        taskThread = ESXTask(tasks=[{
            'task': self.vm.Destroy_Task
        }])

        taskThread.start()

        if waitForFinish:
            taskThread.join()
            LOG.info("Virtual machine '%s' deleted." % self.name)


class ESXTask(threading.Thread):
    """
        Execute ESX tasks
        Remark: Tasks are the methods applied to objects of ESXi server (ex. AddHost_Task, CopyDatastoreFile_Task,
         CreateVM_Task etc.) which are executed in non-blocking asynchronous way. On start, task returns object 'info'
         which allows to keep track task execution in real time. That means we need to re-request task state until it's
         completed: either with success or with error.
    """
    def __init__(self, tasks=[{}], timeout=sys.maxint):
        """
            @param tasks: List of one or many {}s describing task, it's arguments and ignored faults (exceptions):
            [
                {
                    'task': <Task reference (ex. vm.CloneVM_Task)>,
                    'kwargs': {
                        <(optional) argument(s) passed to task (ex. 'name': 'New VM')>, ...
                    },
                    'ignoreFault': [
                        <(optional) fault type(s) to be ignored (ex. vim.FileNotFound)>
                    ]
                }
            ]
            @param timeout: How long we allow task to run without completing. Raise exception when timeout reached.
        """
        threading.Thread.__init__(self)
        self.tasks = tasks
        self.id = str(uuid.uuid4())
        self.timeout = Timeout(timeout)
        # Results of evaluation accessible from main thread as 'task.result'
        self.result = []

    def run(self):
        # Multiple tasks scenario example: create folder, copy VM's files, register VM.
        for task in self.tasks:
            ignoreFault = []
            if task.get('ignoreFault', None) is not None:
                # We want to ignore fault when, as example, we attempt to create folder which already exists
                ignoreFault = task['ignoreFault']
            try:
                # Take reference to task and run its instance with provided arguments (if any)
                if 'kwargs' in task:
                    taskExecution = task['task'](**task['kwargs'])
                else:
                    taskExecution = task['task']()
                # Re-request state of task while it's not completed (either 'success' of 'error')
                while taskExecution.info.state not in [vim.TaskInfoState.success, vim.TaskInfoState.error]:
                    self.timeout.exceeded()
                self.result.append({'entity': taskExecution.info.entityName, 'state': taskExecution.info.state,
                'result': taskExecution.info.result, 'error': taskExecution.info.error})
                # Task completed with error
            except Exception as e:
                # Skip faults specified for given task
                if type(e) in ignoreFault:
                    break
